use ractor::rpc::CallResult;
use ractor::{Actor, ActorProcessingErr, ActorRef, RpcReplyPort};
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[derive(Debug)]
pub enum CounterMessage {
    Increment { amount: i64 },
    Decrement { amount: i64 },
    GetValue(RpcReplyPort<i64>),
    GetStats(RpcReplyPort<CounterStats>),
}

#[derive(Debug, Clone)]
pub struct CounterStats {
    pub current_value: i64,
    pub total_operations: u64,
    pub operations_per_second: f64,
}

pub struct CounterActor {
    #[allow(dead_code)]
    id: String,
}

pub struct CounterState {
    id: String,
    value: i64,
    total_operations: u64,
    start_time: Instant,
}

#[ractor::async_trait]
impl Actor for CounterActor {
    type Msg = CounterMessage;
    type State = CounterState;
    type Arguments = String;

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        id: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        println!("[{}] Counter starting", id);
        Ok(CounterState {
            id,
            value: 0,
            total_operations: 0,
            start_time: Instant::now(),
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            CounterMessage::Increment { amount } => {
                state.value += amount;
                state.total_operations += 1;
            }
            CounterMessage::Decrement { amount } => {
                state.value -= amount;
                state.total_operations += 1;
            }
            CounterMessage::GetValue(reply) => {
                reply.send(state.value)?;
            }
            CounterMessage::GetStats(reply) => {
                let elapsed = state.start_time.elapsed().as_secs_f64();
                let ops_per_sec = if elapsed > 0.0 {
                    state.total_operations as f64 / elapsed
                } else {
                    0.0
                };

                reply.send(CounterStats {
                    current_value: state.value,
                    total_operations: state.total_operations,
                    operations_per_second: ops_per_sec,
                })?;
            }
        }
        Ok(())
    }

    async fn post_stop(
        &self,
        _myself: ActorRef<Self::Msg>,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        println!(
            "[{}] Counter stopping with final value {} after {} operations",
            state.id, state.value, state.total_operations
        );
        Ok(())
    }
}

pub enum AggregatorMessage {
    RegisterCounter(ActorRef<CounterMessage>),
    CollectStats,
    GetTotalStats(RpcReplyPort<AggregatedStats>),
}

#[derive(Debug, Clone)]
pub struct AggregatedStats {
    pub total_value: i64,
    pub total_operations: u64,
    pub num_counters: usize,
    pub avg_ops_per_second: f64,
}

pub struct AggregatorActor;

pub struct AggregatorState {
    counters: Vec<ActorRef<CounterMessage>>,
    last_stats: Option<AggregatedStats>,
}

#[ractor::async_trait]
impl Actor for AggregatorActor {
    type Msg = AggregatorMessage;
    type State = AggregatorState;
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        println!("[Aggregator] Starting");
        Ok(AggregatorState {
            counters: Vec::new(),
            last_stats: None,
        })
    }

    async fn handle(
        &self,
        myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            AggregatorMessage::RegisterCounter(counter_ref) => {
                println!("[Aggregator] Registered counter");
                state.counters.push(counter_ref);
            }
            AggregatorMessage::CollectStats => {
                let mut total_value = 0i64;
                let mut total_operations = 0u64;
                let mut total_ops_per_sec = 0.0;

                for counter in &state.counters {
                    match counter.call(
                        |reply| CounterMessage::GetStats(reply),
                        Some(Duration::from_secs(1))
                    ).await {
                        Ok(CallResult::Success(stats)) => {
                            total_value += stats.current_value;
                            total_operations += stats.total_operations;
                            total_ops_per_sec += stats.operations_per_second;
                        }
                        Ok(CallResult::Timeout) => {
                            eprintln!("[Aggregator] Timeout getting stats from counter");
                        }
                        Ok(CallResult::SenderError) => {
                            eprintln!("[Aggregator] Sender error getting stats from counter");
                        }
                        Err(e) => {
                            eprintln!("[Aggregator] Failed to get stats: {:?}", e);
                        }
                    }
                }

                let aggregated = AggregatedStats {
                    total_value,
                    total_operations,
                    num_counters: state.counters.len(),
                    avg_ops_per_second: if state.counters.len() > 0 {
                        total_ops_per_sec / state.counters.len() as f64
                    } else {
                        0.0
                    },
                };

                println!("[Aggregator] Total value: {}, Total ops: {}, Avg ops/sec: {:.2}",
                    aggregated.total_value,
                    aggregated.total_operations,
                    aggregated.avg_ops_per_second
                );

                state.last_stats = Some(aggregated);

                // Schedule next collection
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(2)).await;
                    let _ = myself.cast(AggregatorMessage::CollectStats);
                });
            }
            AggregatorMessage::GetTotalStats(reply) => {
                if let Some(stats) = &state.last_stats {
                    reply.send(stats.clone())?;
                } else {
                    reply.send(AggregatedStats {
                        total_value: 0,
                        total_operations: 0,
                        num_counters: 0,
                        avg_ops_per_second: 0.0,
                    })?;
                }
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting distributed counter system\n");

    // Spawn the aggregator
    let (aggregator_ref, _aggregator_handle) = Actor::spawn(
        Some("aggregator".to_string()),
        AggregatorActor,
        (),
    ).await?;

    // Spawn multiple counter actors
    let mut counter_refs = Vec::new();
    for i in 0..4 {
        let counter_id = format!("counter-{}", i);
        let (counter_ref, _counter_handle) = Actor::spawn(
            Some(counter_id.clone()),
            CounterActor { id: counter_id.clone() },
            counter_id,
        ).await?;

        // Register with aggregator
        aggregator_ref.cast(AggregatorMessage::RegisterCounter(counter_ref.clone()))?;
        counter_refs.push(counter_ref);
    }

    // Start periodic stats collection
    aggregator_ref.cast(AggregatorMessage::CollectStats)?;

    // Simulate concurrent operations from multiple clients
    let mut handles = Vec::new();
    for (idx, counter_ref) in counter_refs.iter().enumerate() {
        let counter = counter_ref.clone();
        let handle = tokio::spawn(async move {
            for i in 0..50 {
                if i % 3 == 0 {
                    let _ = counter.cast(CounterMessage::Decrement {
                        amount: (idx + 1) as i64
                    });
                } else {
                    let _ = counter.cast(CounterMessage::Increment {
                        amount: (idx + 1) as i64
                    });
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        });
        handles.push(handle);
    }

    // Wait for operations to complete
    for handle in handles {
        handle.await?;
    }

    // Give time for final stats collection
    sleep(Duration::from_secs(3)).await;

    // Get final aggregated stats
    let final_stats = match aggregator_ref.call(
        |reply| AggregatorMessage::GetTotalStats(reply),
        Some(Duration::from_secs(1))
    ).await? {
        CallResult::Success(stats) => stats,
        CallResult::Timeout => {
            eprintln!("Timeout getting final stats");
            return Err("Timeout".into());
        }
        CallResult::SenderError => {
            eprintln!("Sender error getting final stats");
            return Err("Sender error".into());
        }
    };

    println!("\n=== Final System Statistics ===");
    println!("Total value across all counters: {}", final_stats.total_value);
    println!("Total operations: {}", final_stats.total_operations);
    println!("Number of counters: {}", final_stats.num_counters);
    println!("Average ops/second per counter: {:.2}", final_stats.avg_ops_per_second);

    // Shutdown
    for counter in counter_refs {
        counter.stop(None);
    }
    aggregator_ref.stop(None);

    sleep(Duration::from_millis(100)).await;

    Ok(())
}
