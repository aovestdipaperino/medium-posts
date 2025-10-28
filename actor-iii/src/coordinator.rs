use ractor::{Actor, ActorProcessingErr, ActorRef};
use tokio::time::{interval, Duration};
use std::collections::HashMap;
use crate::types::{NodeType, NodeInfo, ClusterStatus};

pub enum CoordinatorMessage {
    RegisterNode { node_id: String, node_type: NodeType },
    NodeHeartbeat { node_id: String },
    GetClusterStatus(ractor::RpcReplyPort<ClusterStatus>),
    CheckHealth,
}

pub struct ClusterCoordinator;

pub struct CoordinatorState {
    nodes: HashMap<String, NodeInfo>,
    heartbeat_timeout: Duration,
}

#[ractor::async_trait]
impl Actor for ClusterCoordinator {
    type Msg = CoordinatorMessage;
    type State = CoordinatorState;
    type Arguments = ();

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        println!("[Coordinator] Starting cluster coordinator");

        // Start periodic health checks
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                let _ = myself.cast(CoordinatorMessage::CheckHealth);
            }
        });

        Ok(CoordinatorState {
            nodes: HashMap::new(),
            heartbeat_timeout: Duration::from_secs(30),
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            CoordinatorMessage::RegisterNode { node_id, node_type } => {
                println!("[Coordinator] Registering node {} as {:?}", node_id, node_type);
                state.nodes.insert(node_id, NodeInfo {
                    node_type,
                    last_heartbeat: std::time::Instant::now(),
                });
            }
            CoordinatorMessage::NodeHeartbeat { node_id } => {
                if let Some(node) = state.nodes.get_mut(&node_id) {
                    node.last_heartbeat = std::time::Instant::now();
                }
            }
            CoordinatorMessage::GetClusterStatus(reply) => {
                let now = std::time::Instant::now();
                let healthy_nodes = state.nodes.values()
                    .filter(|n| now.duration_since(n.last_heartbeat) < state.heartbeat_timeout)
                    .count();

                let routers = state.nodes.values()
                    .filter(|n| matches!(n.node_type, NodeType::Router))
                    .count();

                let workers = state.nodes.values()
                    .filter(|n| matches!(n.node_type, NodeType::Worker))
                    .count();

                let aggregators = state.nodes.values()
                    .filter(|n| matches!(n.node_type, NodeType::Aggregator))
                    .count();

                reply.send(ClusterStatus {
                    total_nodes: state.nodes.len(),
                    healthy_nodes,
                    routers,
                    workers,
                    aggregators,
                })?;
            }
            CoordinatorMessage::CheckHealth => {
                let now = std::time::Instant::now();
                let dead_nodes: Vec<_> = state.nodes.iter()
                    .filter(|(_, info)| now.duration_since(info.last_heartbeat) >= state.heartbeat_timeout)
                    .map(|(id, _)| id.clone())
                    .collect();

                for node_id in dead_nodes {
                    println!("[Coordinator] Node {} appears dead, removing from cluster", node_id);
                    state.nodes.remove(&node_id);
                }
            }
        }
        Ok(())
    }
}
