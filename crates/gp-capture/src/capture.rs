//! The capture loop: connect, record, and on any failure write `GAP_START`, back off,
//! reconnect and resubscribe. The first market-data frame after a reconnect writes `GAP_END`
//! (the `market_info` greeting does not).

use std::sync::Arc;

use gp_log::LogError;
use tokio::sync::{Notify, watch};

use crate::backoff::Backoff;
use crate::client::{ConnOpts, End, Outcome, run_connection, stopped};
use crate::endpoint::Endpoint;
use crate::recorder::Recorder;
use crate::subscriber::Subscriber;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Summary {
    /// Connection attempts made with a resolved feed URL (a failed authorize is not one).
    pub connections: u32,
    /// Frames written, `market_info` greetings included.
    pub frames: u64,
    pub gaps: u64,
}

/// Capture from `endpoint` until `stop` becomes true. When `token_changed` fires during a
/// backoff wait, the next attempt starts at once.
pub async fn capture(
    endpoint: &Endpoint,
    sub: &mut Subscriber,
    rec: &mut Recorder,
    backoff: &mut Backoff,
    opts: ConnOpts,
    token_changed: Option<Arc<Notify>>,
    stop: &mut watch::Receiver<bool>,
) -> Result<Summary, LogError> {
    let mut summary = Summary::default();
    let http = reqwest::Client::builder()
        .timeout(opts.stall)
        .build()
        .expect("TLS backend available");
    let mut token = None;
    while !*stop.borrow() {
        if let Some(fp) = endpoint.token_fingerprint() {
            match token.replace(fp) {
                None => rec.token_reload("token loaded")?,
                Some(old) if old != fp => rec.token_reload("token file changed")?,
                Some(_) => {}
            }
        }
        let url = tokio::select! {
            _ = stopped(stop) => break,
            r = endpoint.url(&http) => r,
        };
        let out = match url {
            Ok(url) => {
                summary.connections += 1;
                run_connection(&url, sub, rec, opts, stop).await?
            }
            Err(e) => Outcome {
                end: End::Failed(e),
                frames: 0,
                data_frames: 0,
            },
        };
        summary.frames += out.frames;
        // Only market data proves the feed works: a greeting followed by a drop must not
        // reset the backoff, or a feed that always cuts after market_info is hammered.
        if out.data_frames > 0 {
            backoff.reset();
        }
        if out.end == End::Stopped {
            break;
        }
        rec.gap_start(&out.end.reason())?;
        rec.flush()?;
        let delay = backoff.next_delay();
        let woken = async {
            match &token_changed {
                Some(n) => n.notified().await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            _ = stopped(stop) => break,
            _ = tokio::time::sleep(delay) => {}
            _ = woken => backoff.reset(),
        }
    }
    summary.gaps = rec.stats().gaps;
    rec.flush()?;
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::{Fault, MockConfig, MockFeed};
    use gp_feed::synth::{MarketConfig, universe};
    use gp_feed::{Date, Selection, parse_request};
    use gp_log::{RecordKind, Tail, WriterConfig, read_log};
    use std::time::Duration;

    /// Polls the live log until `pred` holds over every record kind written so far.
    async fn wait_for(
        tail: &mut Tail,
        seen: &mut Vec<RecordKind>,
        want: &str,
        pred: impl Fn(&[RecordKind]) -> bool,
    ) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok((recs, _)) = tail.poll_skipping_torn() {
                seen.extend(recs.into_iter().map(|r| r.kind));
            }
            if pred(seen) {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {want}; log so far: {seen:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn count(kinds: &[RecordKind], want: RecordKind) -> usize {
        kinds.iter().filter(|&&k| k == want).count()
    }

    /// A raw frame was written after the last `want` record.
    fn frames_after_last(kinds: &[RecordKind], want: RecordKind) -> bool {
        kinds
            .iter()
            .rposition(|&k| k == want)
            .is_some_and(|i| kinds[i..].contains(&RecordKind::RawFrame))
    }

    #[tokio::test]
    async fn survives_cuts_and_resubscribes() {
        let date = Date::from_ymd(2026, 10, 19);
        let u = universe(date, 25_000.0, 2, 3.0, 50.0);
        let feed = MockFeed::start(
            MockConfig {
                market: MarketConfig::new(date),
                instruments: u.clone(),
                interval: Duration::from_millis(5),
            },
            "127.0.0.1:0",
        )
        .await
        .unwrap();
        let sel = Selection {
            band_pct: 1.0,
            ..Selection::default()
        };
        let mut sub = Subscriber::new(u, date, sel, "B");
        let dir = tempfile::tempdir().unwrap();
        let (mut rec, _) = Recorder::open(dir.path(), WriterConfig::default(), 1).unwrap();
        let mut backoff = Backoff::new(Duration::from_millis(20), Duration::from_millis(100), 9);
        let opts = ConnOpts {
            stall: Duration::from_millis(250),
            flush: Duration::from_millis(20),
        };
        let (tx, mut rx) = watch::channel(false);
        let url = Endpoint::Direct(feed.url());
        let task = tokio::spawn(async move {
            let s = capture(&url, &mut sub, &mut rec, &mut backoff, opts, None, &mut rx)
                .await
                .unwrap();
            rec.close().unwrap();
            s
        });
        // Drive the scenario by what the recorder has written, not by wall-clock pauses: a slow
        // runner then only makes the test slower, never changes the sequence it asserts.
        use RecordKind::{GapEnd, GapStart, RawFrame};
        let (mut tail, mut seen) = (Tail::new(dir.path()), Vec::new());
        wait_for(&mut tail, &mut seen, "first frames", |k| {
            count(k, RawFrame) >= 5
        })
        .await;
        feed.cut(Fault::Drop);
        wait_for(&mut tail, &mut seen, "gap 1 start", |k| {
            count(k, GapStart) == 1
        })
        .await;
        feed.restore();
        wait_for(&mut tail, &mut seen, "gap 1 end and frames", |k| {
            count(k, GapEnd) == 1 && frames_after_last(k, GapEnd)
        })
        .await;
        feed.cut(Fault::Silence);
        wait_for(&mut tail, &mut seen, "gap 2 start (stall)", |k| {
            count(k, GapStart) == 2
        })
        .await;
        feed.restore();
        wait_for(&mut tail, &mut seen, "gap 2 end and frames", |k| {
            count(k, GapEnd) == 2 && frames_after_last(k, GapEnd)
        })
        .await;
        tx.send(true).unwrap();
        let summary = task.await.unwrap();

        assert_eq!(summary.gaps, 2, "{summary:?}");
        assert!(summary.connections >= 3);
        let recs = read_log(dir.path()).unwrap();
        let kinds: Vec<RecordKind> = recs.iter().map(|r| r.kind).collect();
        let starts: Vec<usize> = (0..kinds.len())
            .filter(|&i| kinds[i] == RecordKind::GapStart)
            .collect();
        let ends: Vec<usize> = (0..kinds.len())
            .filter(|&i| kinds[i] == RecordKind::GapEnd)
            .collect();
        assert_eq!((starts.len(), ends.len()), (2, 2));
        assert!(starts[0] < ends[0] && ends[0] < starts[1] && starts[1] < ends[1]);
        assert!(String::from_utf8_lossy(&recs[starts[1]].payload).contains("stalled"));
        // each reconnect resubscribes the whole band in its first requests
        let first_sub_after_gap = recs[starts[0]..]
            .iter()
            .find(|r| r.kind == RecordKind::Subscribe)
            .unwrap();
        assert!(
            parse_request(&first_sub_after_gap.payload)
                .unwrap()
                .data
                .instrument_keys
                .len()
                > 1
        );
        // frames flowed in each of the three healthy windows
        assert!(kinds[..starts[0]].contains(&RecordKind::RawFrame));
        assert!(kinds[ends[0]..starts[1]].contains(&RecordKind::RawFrame));
        assert!(kinds[ends[1]..].contains(&RecordKind::RawFrame));
    }

    #[tokio::test]
    async fn new_token_wakes_backoff_and_is_recorded() {
        use axum::routing::get;
        let date = Date::from_ymd(2026, 10, 19);
        let u = universe(date, 25_000.0, 1, 2.0, 50.0);
        let feed = MockFeed::start(
            MockConfig {
                market: MarketConfig::new(date),
                instruments: u.clone(),
                interval: Duration::from_millis(5),
            },
            "127.0.0.1:0",
        )
        .await
        .unwrap();
        // Authorize only the token "fresh"; hand out the mock feed's URL.
        let ws = feed.url();
        let app = axum::Router::new().route(
            crate::endpoint::AUTHORIZE_PATH,
            get(move |h: axum::http::HeaderMap| {
                let ws = ws.clone();
                async move {
                    if h.get("authorization").and_then(|v| v.to_str().ok()) == Some("Bearer fresh") {
                        (axum::http::StatusCode::OK, format!(
                            r#"{{"status":"success","data":{{"authorized_redirect_uri":"{ws}"}}}}"#
                        ))
                    } else {
                        (axum::http::StatusCode::UNAUTHORIZED, String::new())
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let dir = tempfile::tempdir().unwrap();
        let token_file = dir.path().join("upstox.token");
        std::fs::write(&token_file, "stale").unwrap();
        let watch = crate::token::TokenWatch::new(&token_file).unwrap();
        let endpoint = Endpoint::Upstox {
            api_base: api,
            token_file: token_file.clone(),
        };
        let logs = tempfile::tempdir().unwrap();
        let (mut rec, _) = Recorder::open(logs.path(), WriterConfig::default(), 0).unwrap();
        let mut sub = Subscriber::new(u, date, Selection::default(), "A");
        // A long backoff: only the token notification can get the capture going quickly.
        let mut backoff = Backoff::new(Duration::from_secs(30), Duration::from_secs(30), 1);
        let opts = ConnOpts {
            stall: Duration::from_secs(2),
            flush: Duration::from_millis(20),
        };
        let (tx, mut rx) = watch::channel(false);
        let changed = watch.changed();
        let task = tokio::spawn(async move {
            let s = capture(
                &endpoint,
                &mut sub,
                &mut rec,
                &mut backoff,
                opts,
                Some(changed),
                &mut rx,
            )
            .await
            .unwrap();
            rec.close().unwrap();
            s
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        std::fs::write(&token_file, "fresh").unwrap();
        tokio::time::sleep(Duration::from_millis(800)).await;
        tx.send(true).unwrap();
        let summary = tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
        assert!(summary.frames > 10, "{summary:?}");

        let recs = read_log(logs.path()).unwrap();
        let kinds: Vec<_> = recs.iter().map(|r| r.kind).collect();
        assert_eq!(kinds[0], RecordKind::TokenReload);
        assert_eq!(recs[0].payload, b"token loaded");
        assert_eq!(kinds[1], RecordKind::GapStart);
        assert!(String::from_utf8_lossy(&recs[1].payload).contains("HTTP 401"));
        assert_eq!(kinds[2], RecordKind::TokenReload);
        assert_eq!(recs[2].payload, b"token file changed");
        assert!(kinds.contains(&RecordKind::GapEnd));
        assert!(
            recs.iter()
                .all(|r| !String::from_utf8_lossy(&r.payload).contains("fresh"))
        );
        drop(watch);
    }
}
