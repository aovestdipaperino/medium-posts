use ractor::{Actor, ActorProcessingErr, ActorRef};
use crate::types::{Sample, RegionStats, NodeType};
use crate::router::{SampleRouter, RouterMessage};
use crate::worker::{AnalysisWorker, WorkerMessage};
use crate::aggregator::{StatisticsAggregator, AggregatorMessage};
use crate::coordinator::CoordinatorMessage;

pub enum RegionMessage {
    Start,
    SubmitSample(Sample),
    GetRegionStats(ractor::RpcReplyPort<RegionStats>),
}

pub struct RegionalSupervisor {
    pub region: String,
    pub num_workers: usize,
}

pub struct RegionState {
    region: String,
    router: Option<ActorRef<RouterMessage>>,
    workers: Vec<ActorRef<WorkerMessage>>,
    aggregator: Option<ActorRef<AggregatorMessage>>,
    coordinator: ActorRef<CoordinatorMessage>,
}

#[ractor::async_trait]
impl Actor for RegionalSupervisor {
    type Msg = RegionMessage;
    type State = RegionState;
    type Arguments = (String, usize, ActorRef<CoordinatorMessage>);

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        (region, num_workers, coordinator): Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        println!("[Region:{}] Starting regional supervisor with {} workers", region, num_workers);

        // Spawn aggregator
        let (aggregator_ref, _) = Actor::spawn_linked(
            Some(format!("{}-aggregator", region)),
            StatisticsAggregator { region: region.clone() },
            region.clone(),
            myself.get_cell(),
        ).await?;

        coordinator.cast(CoordinatorMessage::RegisterNode {
            node_id: format!("{}-aggregator", region),
            node_type: NodeType::Aggregator,
        })?;

        // Spawn router
        let (router_ref, _) = Actor::spawn_linked(
            Some(format!("{}-router", region)),
            SampleRouter { region: region.clone() },
            region.clone(),
            myself.get_cell(),
        ).await?;

        coordinator.cast(CoordinatorMessage::RegisterNode {
            node_id: format!("{}-router", region),
            node_type: NodeType::Router,
        })?;

        // Spawn workers
        let mut workers = Vec::new();
        for i in 0..num_workers {
            let worker_id = format!("worker-{}", i);
            let (worker_ref, _) = Actor::spawn_linked(
                Some(format!("{}-{}", region, worker_id)),
                AnalysisWorker {
                    worker_id: worker_id.clone(),
                    region: region.clone(),
                    aggregator: aggregator_ref.clone(),
                },
                (worker_id.clone(), region.clone(), aggregator_ref.clone()),
                myself.get_cell(),
            ).await?;

            // Register worker with router
            router_ref.cast(RouterMessage::RegisterWorker {
                worker_id: format!("{}-{}", region, worker_id),
                worker_ref: worker_ref.clone(),
            })?;

            coordinator.cast(CoordinatorMessage::RegisterNode {
                node_id: format!("{}-{}", region, worker_id),
                node_type: NodeType::Worker,
            })?;

            workers.push(worker_ref);
        }

        Ok(RegionState {
            region,
            router: Some(router_ref),
            workers,
            aggregator: Some(aggregator_ref),
            coordinator,
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            RegionMessage::Start => {
                println!("[Region:{}] Region started and ready", state.region);
            }
            RegionMessage::SubmitSample(sample) => {
                if let Some(ref router) = state.router {
                    router.cast(RouterMessage::RouteSample(sample))?;
                }
            }
            RegionMessage::GetRegionStats(reply) => {
                use tokio::time::Duration;

                let workers_active = state.workers.len();

                let (samples_processed, malicious_detected) = if let Some(ref aggregator) = state.aggregator {
                    match aggregator.call(
                        |r| AggregatorMessage::GetTotalStats(r),
                        Some(Duration::from_secs(1))
                    ).await {
                        Ok(call_result) => {
                            if let ractor::rpc::CallResult::Success(stats) = call_result {
                                (stats.total_detections, stats.malicious_count)
                            } else {
                                (0, 0)
                            }
                        },
                        Err(_) => (0, 0),
                    }
                } else {
                    (0, 0)
                };

                reply.send(RegionStats {
                    region: state.region.clone(),
                    workers_active,
                    samples_processed,
                    malicious_detected,
                })?;
            }
        }
        Ok(())
    }
}
