use ractor::{Actor, ActorProcessingErr, ActorRef};
use crate::types::{Sample, DetectionResult, WorkerStats};
use crate::aggregator::AggregatorMessage;

pub enum WorkerMessage {
    AnalyzeSample(Sample),
    GetStats(ractor::RpcReplyPort<WorkerStats>),
}

pub struct AnalysisWorker {
    pub worker_id: String,
    pub region: String,
    pub aggregator: ActorRef<AggregatorMessage>,
}

pub struct WorkerState {
    worker_id: String,
    region: String,
    aggregator: ActorRef<AggregatorMessage>,
    samples_analyzed: u64,
    malicious_detected: u64,
    total_analysis_time: u64,
}

#[ractor::async_trait]
impl Actor for AnalysisWorker {
    type Msg = WorkerMessage;
    type State = WorkerState;
    type Arguments = (String, String, ActorRef<AggregatorMessage>);

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        (worker_id, region, aggregator): Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        println!("[Worker:{}:{}] Starting analysis worker", region, worker_id);
        Ok(WorkerState {
            worker_id,
            region,
            aggregator,
            samples_analyzed: 0,
            malicious_detected: 0,
            total_analysis_time: 0,
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            WorkerMessage::AnalyzeSample(sample) => {
                use tokio::time::{Instant, Duration};

                let start = Instant::now();

                // Simulate analysis work
                tokio::time::sleep(Duration::from_millis(50)).await;

                // Simple heuristic: samples with certain byte patterns are malicious
                let is_malicious = sample.data.iter().any(|&b| b == 0xDE || b == 0xAD);
                let family = if is_malicious {
                    Some(format!("Family-{}", sample.data[0] % 5))
                } else {
                    None
                };

                let elapsed = start.elapsed().as_millis() as u64;
                state.total_analysis_time += elapsed;
                state.samples_analyzed += 1;

                if is_malicious {
                    state.malicious_detected += 1;
                }

                let result = DetectionResult {
                    sample_id: sample.id.clone(),
                    is_malicious,
                    family: family.clone(),
                    confidence: 0.85,
                };

                // Send result to aggregator
                state.aggregator.cast(AggregatorMessage::RecordDetection(result))?;

                if is_malicious {
                    println!("[Worker:{}:{}] Sample {} detected as malicious ({})",
                             state.region, state.worker_id, sample.id,
                             family.unwrap_or_else(|| "unknown".to_string()));
                }
            }
            WorkerMessage::GetStats(reply) => {
                reply.send(WorkerStats {
                    samples_analyzed: state.samples_analyzed,
                    malicious_detected: state.malicious_detected,
                    analysis_time_ms: if state.samples_analyzed > 0 {
                        state.total_analysis_time / state.samples_analyzed
                    } else {
                        0
                    },
                })?;
            }
        }
        Ok(())
    }
}
