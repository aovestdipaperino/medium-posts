use ractor::{Actor, ActorProcessingErr, ActorRef};
use std::collections::BTreeMap;
use crate::types::{DetectionResult, AggregatorStats};

pub enum AggregatorMessage {
    RecordDetection(DetectionResult),
    GetPrevalence { family: String, reply: ractor::RpcReplyPort<u64> },
    GetTotalStats(ractor::RpcReplyPort<AggregatorStats>),
    SyncWith { peer: ActorRef<AggregatorMessage> },
}

pub struct StatisticsAggregator {
    pub region: String,
}

pub struct AggregatorState {
    region: String,
    total_detections: u64,
    malicious_count: u64,
    family_counts: BTreeMap<String, u64>,
}

#[ractor::async_trait]
impl Actor for StatisticsAggregator {
    type Msg = AggregatorMessage;
    type State = AggregatorState;
    type Arguments = String;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        region: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        println!("[Aggregator:{}] Starting statistics aggregator", region);
        Ok(AggregatorState {
            region,
            total_detections: 0,
            malicious_count: 0,
            family_counts: BTreeMap::new(),
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            AggregatorMessage::RecordDetection(result) => {
                state.total_detections += 1;

                if result.is_malicious {
                    state.malicious_count += 1;

                    if let Some(family) = result.family {
                        *state.family_counts.entry(family).or_insert(0) += 1;
                    }
                }

                if state.total_detections % 100 == 0 {
                    println!("[Aggregator:{}] Stats: {}/{} malicious",
                             state.region, state.malicious_count, state.total_detections);
                }
            }
            AggregatorMessage::GetPrevalence { family, reply } => {
                let count = state.family_counts.get(&family).copied().unwrap_or(0);
                reply.send(count)?;
            }
            AggregatorMessage::GetTotalStats(reply) => {
                reply.send(AggregatorStats {
                    total_detections: state.total_detections,
                    malicious_count: state.malicious_count,
                    family_counts: state.family_counts.clone(),
                })?;
            }
            AggregatorMessage::SyncWith { peer: _peer } => {
                // In a real implementation, this would sync state with remote aggregator
                println!("[Aggregator:{}] Syncing state with peer aggregator", state.region);
            }
        }
        Ok(())
    }
}
