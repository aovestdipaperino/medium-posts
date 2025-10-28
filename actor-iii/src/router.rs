use ractor::{Actor, ActorProcessingErr, ActorRef};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use crate::types::Sample;
use crate::worker::WorkerMessage;

pub enum RouterMessage {
    RegisterWorker { worker_id: String, worker_ref: ActorRef<WorkerMessage> },
    UnregisterWorker { worker_id: String },
    RouteSample(Sample),
    GetWorkerCount(ractor::RpcReplyPort<usize>),
}

pub struct SampleRouter {
    pub region: String,
}

pub struct RouterState {
    region: String,
    workers: HashMap<String, ActorRef<WorkerMessage>>,
    samples_routed: u64,
}

impl RouterState {
    fn hash_sample(&self, sample: &Sample) -> u64 {
        let mut hasher = DefaultHasher::new();
        sample.data.hash(&mut hasher);
        hasher.finish()
    }

    fn select_worker(&self, sample: &Sample) -> Option<&ActorRef<WorkerMessage>> {
        if self.workers.is_empty() {
            return None;
        }

        let hash = self.hash_sample(sample);
        let worker_keys: Vec<_> = self.workers.keys().collect();
        let index = (hash as usize) % worker_keys.len();
        let worker_id = worker_keys[index];

        self.workers.get(worker_id)
    }
}

#[ractor::async_trait]
impl Actor for SampleRouter {
    type Msg = RouterMessage;
    type State = RouterState;
    type Arguments = String;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        region: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        println!("[Router:{}] Starting sample router", region);
        Ok(RouterState {
            region,
            workers: HashMap::new(),
            samples_routed: 0,
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            RouterMessage::RegisterWorker { worker_id, worker_ref } => {
                println!("[Router:{}] Registering worker {}", state.region, worker_id);
                state.workers.insert(worker_id, worker_ref);
            }
            RouterMessage::UnregisterWorker { worker_id } => {
                println!("[Router:{}] Unregistering worker {}", state.region, worker_id);
                state.workers.remove(&worker_id);
            }
            RouterMessage::RouteSample(sample) => {
                if let Some(worker) = state.select_worker(&sample).cloned() {
                    state.samples_routed += 1;
                    worker.cast(WorkerMessage::AnalyzeSample(sample))?;
                } else {
                    println!("[Router:{}] No workers available for sample {}",
                             state.region, sample.id);
                }
            }
            RouterMessage::GetWorkerCount(reply) => {
                reply.send(state.workers.len())?;
            }
        }
        Ok(())
    }
}
