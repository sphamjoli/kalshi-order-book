//! Kalshi REST client. Owns URLs, request signing and response mapping. Errors
//! never carry signed headers. A decoding error may quote part of a response.

use crate::kalshi::auth::{RequestSigner, auth_header_map};
use crate::transport;
use chrono::Utc;
use reqwest::header::InvalidHeaderValue;
use reqwest::redirect::Policy;
use reqwest::{Client, Response, StatusCode};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use std::collections::HashSet;
use std::ops::ControlFlow;
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;
use tracing::{debug, warn};
use url::Url;

const ACCOUNT_LIMITS_PATH: &str = "account/limits";
const MARKETS_PATH: &str = "markets";
/// The largest page `GET /markets` accepts.
const MARKETS_PAGE_LIMIT: usize = 1000;
/// Stops a server that never ends a scan. A real scan was about 181 pages.
const MAX_MARKET_PAGES: usize = 1000;
/// Long enough for a full page while every socket shares the same link.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
/// Far above a real page, which stops a faulty server exhausting memory.
const MAX_BODY_BYTES: usize = 32 << 20;
const RETRY: RetryPolicy = RetryPolicy {
    attempts: 5,
    base: Duration::from_millis(250),
    max: Duration::from_secs(5),
};

/// Reasons a REST call fails.
#[derive(Debug, Error)]
pub enum RestError {
    #[error("URL is invalid: {0}")]
    InvalidBaseUrl(#[from] url::ParseError),
    #[error("signed header is not a valid header value")]
    InvalidHeader(#[from] InvalidHeaderValue),
    /// Retrying cannot help: the key or signature was refused.
    #[error("Kalshi refused the credentials")]
    Unauthorized,
    #[error("Kalshi returned status {0}")]
    Status(u16),
    /// Signatures and market data must not cross a network in the clear.
    #[error("base URL must use https unless the host is loopback")]
    InsecureBaseUrl,
    #[error("a markets page exceeded the requested limit")]
    OversizedPage,
    #[error("a response exceeded {MAX_BODY_BYTES} bytes")]
    OversizedBody,
    #[error("market scan exceeded {MAX_MARKET_PAGES} pages")]
    TooManyPages,
    /// A loop in the cursors would otherwise page forever.
    #[error("Kalshi repeated a pagination cursor")]
    RepeatedCursor,
    #[error("response is not the expected JSON: {0}")]
    Decode(#[from] serde_json::Error),
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
}

/// Signs and sends requests to one Kalshi environment.
pub struct KalshiClient {
    http: Client,
    base_url: Url,
    signer: Arc<dyn RequestSigner>,
    retry: RetryPolicy,
}

/// How one request is retried. Bounded, because a caller of a REST scan can
/// start the whole scan again; it is not the policy for a live socket.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub attempts: u32,
    pub base: Duration,
    pub max: Duration,
}

/// Which markets a scan asks for. Times are Unix seconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketQuery {
    Open,
    /// Open markets created at or after this time.
    CreatedSince(i64),
    /// Closed markets that closed at or after this time.
    ClosedSince(i64),
    /// Settled markets that settled at or after this time. A market can close
    /// and settle between two polls, leaving it on neither other list.
    SettledSince(i64),
}

/// A market that discovery found.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Market {
    pub ticker: String,
    pub status: String,
}

/// One page of `GET /markets`.
#[derive(Debug, Deserialize)]
struct MarketsPage {
    markets: Vec<Market>,
    /// Empty on the last page. Required, so a truncated response cannot look
    /// like the end of a scan.
    cursor: String,
}

/// The account's REST budget.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct AccountLimits {
    pub usage_tier: String,
    pub read: TokenBucket,
    pub write: TokenBucket,
}

/// One token bucket from [`AccountLimits`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct TokenBucket {
    pub refill_rate: u64,
    pub bucket_capacity: u64,
}

impl RestError {
    /// Returns whether the same request could succeed if sent again.
    fn is_retryable(&self) -> bool {
        match self {
            Self::Status(status) => *status == 429 || *status >= 500,
            Self::Http(error) => error.is_timeout() || error.is_connect() || error.is_body(),
            _ => false,
        }
    }
}

impl KalshiClient {
    /// Creates a client for a base URL that ends in the API version, such as
    /// `https://host/trade-api/v2`.
    ///
    /// # Errors
    /// Returns [`RestError::InvalidBaseUrl`] when `base_url` cannot be parsed,
    /// [`RestError::InsecureBaseUrl`] when it is plain HTTP to another
    /// machine, and [`RestError::Http`] when the HTTP client cannot be built.
    pub fn try_new(base_url: &str, signer: Arc<dyn RequestSigner>) -> Result<Self, RestError> {
        let base_url = Url::parse(&format!("{}/", base_url.trim_end_matches('/')))?;
        if !transport::is_secure(&base_url) {
            return Err(RestError::InsecureBaseUrl);
        }
        Ok(Self {
            // A redirect would carry the signed headers to the new host.
            http: Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .redirect(Policy::none())
                .build()?,
            base_url,
            signer,
            retry: RETRY,
        })
    }

    /// Replaces the default retry policy.
    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// Fetches the account's limits. This also proves the signature works,
    /// because the endpoint requires authentication.
    ///
    /// # Errors
    /// Returns [`RestError::Unauthorized`] when the credentials are refused,
    /// and another [`RestError`] when the call or its decoding fails.
    pub async fn account_limits(&self) -> Result<AccountLimits, RestError> {
        self.get(self.base_url.join(ACCOUNT_LIMITS_PATH)?).await
    }

    /// Fetches every matching market, excluding multivariate ones. Returns
    /// the whole set or an error, never a partial list, so a failed scan
    /// cannot look like markets disappearing.
    ///
    /// # Errors
    /// Returns the same errors as [`Self::scan_markets`].
    pub async fn markets(&self, query: MarketQuery) -> Result<Vec<Market>, RestError> {
        let mut markets = Vec::new();
        self.scan_markets(query, |page| {
            markets.extend(page);
            ControlFlow::Continue(())
        })
        .await?;
        Ok(markets)
    }

    /// Passes each page of matching markets to `on_page` as it arrives, until the
    /// scan ends or `on_page` breaks. A full scan takes minutes, so a caller
    /// can act on early pages. It sees partial data if the scan later fails.
    ///
    /// # Errors
    /// Returns an error when pagination loops, runs past its bounds, or a
    /// page still fails after its retries.
    pub async fn scan_markets(
        &self,
        query: MarketQuery,
        mut on_page: impl FnMut(Vec<Market>) -> ControlFlow<()>,
    ) -> Result<(), RestError> {
        let mut seen_cursors = HashSet::new();
        let mut cursor = None;
        for _ in 0..MAX_MARKET_PAGES {
            let started = Instant::now();
            let page: MarketsPage = self
                .get(self.markets_url(query, cursor.as_deref())?)
                .await?;
            if page.markets.len() > MARKETS_PAGE_LIMIT {
                return Err(RestError::OversizedPage);
            }
            debug!(
                markets = page.markets.len(),
                elapsed_ms = started.elapsed().as_millis(),
                "Fetched a market page"
            );
            if on_page(page.markets).is_break() || page.cursor.is_empty() {
                return Ok(());
            }
            if !seen_cursors.insert(page.cursor.clone()) {
                return Err(RestError::RepeatedCursor);
            }
            cursor = Some(page.cursor);
        }
        Err(RestError::TooManyPages)
    }

    fn markets_url(&self, query: MarketQuery, cursor: Option<&str>) -> Result<Url, RestError> {
        let mut url = self.base_url.join(MARKETS_PATH)?;
        let (status, time_filter) = match query {
            MarketQuery::Open => ("open", None),
            MarketQuery::CreatedSince(time) => ("open", Some(("min_created_ts", time))),
            MarketQuery::ClosedSince(time) => ("closed", Some(("min_close_ts", time))),
            MarketQuery::SettledSince(time) => ("settled", Some(("min_settled_ts", time))),
        };
        if let Some((name, time)) = time_filter {
            url.query_pairs_mut().append_pair(name, &time.to_string());
        }
        url.query_pairs_mut()
            .append_pair("status", status)
            .append_pair("mve_filter", "exclude")
            .append_pair("limit", &MARKETS_PAGE_LIMIT.to_string());
        if let Some(cursor) = cursor {
            url.query_pairs_mut().append_pair("cursor", cursor);
        }
        Ok(url)
    }

    /// Retries transient failures with capped backoff. Each attempt signs
    /// again, because a signature carries its own timestamp.
    async fn get<T: DeserializeOwned>(&self, url: Url) -> Result<T, RestError> {
        let mut attempt = 0;
        loop {
            match self.get_once(url.clone()).await {
                Err(error) if error.is_retryable() && attempt + 1 < self.retry.attempts => {
                    warn!(%error, attempt, "Request failed; retrying");
                    tokio::time::sleep(transport::backoff(
                        attempt,
                        self.retry.base,
                        self.retry.max,
                    ))
                    .await;
                    attempt += 1;
                }
                result => return result,
            }
        }
    }

    /// Signs the final URL's path, so the signature always covers the API
    /// version prefix that the server sees.
    async fn get_once<T: DeserializeOwned>(&self, url: Url) -> Result<T, RestError> {
        let signed = self
            .signer
            .sign(Utc::now().timestamp_millis(), "GET", url.path());
        let response = self
            .http
            .get(url)
            .headers(auth_header_map(&signed)?)
            .send()
            .await?;
        match response.status() {
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Err(RestError::Unauthorized),
            status if status.is_success() => {
                Ok(serde_json::from_slice(&read_body(response).await?)?)
            }
            status => Err(RestError::Status(status.as_u16())),
        }
    }
}

async fn read_body(mut response: Response) -> Result<Vec<u8>, RestError> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if body.len() + chunk.len() > MAX_BODY_BYTES {
            return Err(RestError::OversizedBody);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}
