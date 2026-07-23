//! The `manual` source: the key *is* the pre-formatted citation text.
//!
//! This is the escape hatch for citations that have no online source — the
//! consumer passes the already-formatted text as the key and it is stored
//! verbatim under the extension field `_formatted_text`, bypassing CSL
//! rendering. Entries are ephemeral (TTL 0): they live only for the run.

use alloc::boxed::Box;
use alloc::string::String;
use alloc::vec::Vec;
use core::time::Duration;

use crate::csl::CslValue;
use crate::source::{Outcome, Resolution, RetrieveCtx, Source};
use crate::BoxFuture;

/// See module docs.
#[derive(Default)]
pub struct ManualSource;

impl ManualSource {
    pub fn new() -> Self {
        ManualSource
    }
}

impl Source for ManualSource {
    fn prefix(&self) -> &str {
        "manual"
    }

    fn chunk_size(&self) -> usize {
        usize::MAX
    }

    fn min_interval(&self) -> Duration {
        Duration::ZERO
    }

    fn default_ttl(&self) -> Duration {
        Duration::ZERO
    }

    fn retrieve_chunk<'a>(
        &'a self,
        keys: Vec<String>,
        _ctx: &'a RetrieveCtx<'a>,
    ) -> BoxFuture<'a, Vec<Resolution>> {
        Box::pin(async move {
            keys.into_iter()
                .map(|k| {
                    let mut obj = serde_json::Map::new();
                    obj.insert("_formatted_text".into(), CslValue::String(k.clone()));
                    Resolution {
                        key: k,
                        outcome: Outcome::Concrete {
                            csl: CslValue::Object(obj),
                            ttl: Some(Duration::ZERO),
                        },
                    }
                })
                .collect()
        })
    }
}
