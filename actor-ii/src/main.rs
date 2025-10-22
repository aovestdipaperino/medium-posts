use ractor::{Actor, ActorProcessingErr, ActorRef, SupervisionEvent};
use std::time::Duration;
use tokio::time::sleep;

// Market Data Feed Types and Implementation
#[derive(Debug, Clone)]
pub struct MarketDataTick {
    pub symbol: String,
    pub price: f64,
    pub volume: u64,
    pub timestamp: u64,
}

pub enum MarketDataMessage {
    Start,
    Stop,
    Subscribe(ActorRef<OrderBookMessage>),
}

pub struct MarketDataFeed {
    symbol: String,
}

pub struct MarketDataState {
    symbol: String,
    subscribers: Vec<ActorRef<OrderBookMessage>>,
    connection_attempts: u32,
    is_running: bool,
    skip_first_failure: bool,
}

#[ractor::async_trait]
impl Actor for MarketDataFeed {
    type Msg = MarketDataMessage;
    type State = MarketDataState;
    type Arguments = (String, bool);

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        (symbol, skip_first_failure): Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        println!("[MarketData] Starting feed for {}", symbol);
        Ok(MarketDataState {
            symbol,
            subscribers: Vec::new(),
            connection_attempts: 0,
            is_running: false,
            skip_first_failure,
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            MarketDataMessage::Start => {
                state.is_running = true;
                state.connection_attempts += 1;

                // Simulate connection failure on first attempt (unless we're a restarted actor)
                if state.connection_attempts == 1 && !state.skip_first_failure {
                    return Err(ActorProcessingErr::from(
                        "Failed to connect to exchange"
                    ));
                }

                println!("[MarketData] Connected to exchange for {}", state.symbol);

                // Start streaming data
                let symbol = state.symbol.clone();
                let subscribers = state.subscribers.clone();
                tokio::spawn(async move {
                    for i in 0..10 {
                        let tick = MarketDataTick {
                            symbol: symbol.clone(),
                            price: 100.0 + (i as f64 * 0.5),
                            volume: 1000 + (i * 100),
                            timestamp: i,
                        };

                        for subscriber in &subscribers {
                            let _ = subscriber.cast(OrderBookMessage::UpdatePrice(tick.clone()));
                        }

                        sleep(Duration::from_millis(500)).await;
                    }
                });
            }
            MarketDataMessage::Stop => {
                state.is_running = false;
                println!("[MarketData] Stopping feed for {}", state.symbol);
            }
            MarketDataMessage::Subscribe(subscriber) => {
                println!("[MarketData] New subscriber for {}", state.symbol);
                state.subscribers.push(subscriber);
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
            "[MarketData] Stopped after {} connection attempts",
            state.connection_attempts
        );
        Ok(())
    }
}

// Order Book Types and Implementation
pub enum OrderBookMessage {
    UpdatePrice(MarketDataTick),
    GetBestBid(ractor::RpcReplyPort<Option<f64>>),
    GetBestAsk(ractor::RpcReplyPort<Option<f64>>),
    PlaceOrder(Order),
    UpdateRiskManager(ActorRef<RiskMessage>),
}

#[derive(Debug, Clone)]
pub struct Order {
    pub order_id: String,
    pub symbol: String,
    pub side: OrderSide,
    pub quantity: u64,
    pub price: f64,
}

#[derive(Debug, Clone)]
pub enum OrderSide {
    Buy,
    Sell,
}

pub struct OrderBook {
    symbol: String,
}

pub struct OrderBookState {
    symbol: String,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    last_price: Option<f64>,
    update_count: u64,
    risk_manager: Option<ActorRef<RiskMessage>>,
}

#[ractor::async_trait]
impl Actor for OrderBook {
    type Msg = OrderBookMessage;
    type State = OrderBookState;
    type Arguments = (String, Option<ActorRef<RiskMessage>>);

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        (symbol, risk_manager): Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        println!("[OrderBook] Initializing for {}", symbol);
        Ok(OrderBookState {
            symbol,
            best_bid: None,
            best_ask: None,
            last_price: None,
            update_count: 0,
            risk_manager,
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            OrderBookMessage::UpdatePrice(tick) => {
                state.update_count += 1;
                state.last_price = Some(tick.price);

                // Simulate spread
                state.best_bid = Some(tick.price - 0.1);
                state.best_ask = Some(tick.price + 0.1);

                if state.update_count % 5 == 0 {
                    println!(
                        "[OrderBook] {} updates: last={:.2}, bid={:.2}, ask={:.2}",
                        state.update_count,
                        tick.price,
                        state.best_bid.unwrap(),
                        state.best_ask.unwrap()
                    );
                }
            }
            OrderBookMessage::GetBestBid(reply) => {
                reply.send(state.best_bid)?;
            }
            OrderBookMessage::GetBestAsk(reply) => {
                reply.send(state.best_ask)?;
            }
            OrderBookMessage::PlaceOrder(order) => {
                if let Some(ref risk_manager) = state.risk_manager {
                    println!("[OrderBook] Forwarding order {} to risk check", order.order_id);
                    risk_manager.cast(RiskMessage::CheckOrder(order))?;
                } else {
                    println!("[OrderBook] No risk manager configured, rejecting order");
                }
            }
            OrderBookMessage::UpdateRiskManager(new_risk_manager) => {
                println!("[OrderBook] Updating risk manager reference");
                state.risk_manager = Some(new_risk_manager);
            }
        }
        Ok(())
    }
}

// Risk Manager Types and Implementation
pub enum RiskMessage {
    CheckOrder(Order),
    GetExposure(ractor::RpcReplyPort<f64>),
    Reset,
}

pub struct RiskManager;

pub struct RiskState {
    max_position_size: u64,
    current_exposure: f64,
    orders_checked: u64,
    orders_rejected: u64,
    executor: Option<ActorRef<ExecutorMessage>>,
}

#[ractor::async_trait]
impl Actor for RiskManager {
    type Msg = RiskMessage;
    type State = RiskState;
    type Arguments = (u64, Option<ActorRef<ExecutorMessage>>);

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        (max_position_size, executor): Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        println!("[RiskManager] Starting with max position {}", max_position_size);
        Ok(RiskState {
            max_position_size,
            current_exposure: 0.0,
            orders_checked: 0,
            orders_rejected: 0,
            executor,
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            RiskMessage::CheckOrder(order) => {
                state.orders_checked += 1;

                // Check position limits
                if order.quantity > state.max_position_size {
                    state.orders_rejected += 1;
                    println!(
                        "[RiskManager] REJECTED order {}: quantity {} exceeds limit {}",
                        order.order_id, order.quantity, state.max_position_size
                    );
                    return Ok(());
                }

                // Simulate risk check failure for specific order
                if order.order_id == "ORDER-FAIL" {
                    return Err(ActorProcessingErr::from(
                        "Risk calculation error: invalid market state"
                    ));
                }

                let order_value = order.quantity as f64 * order.price;
                state.current_exposure += order_value;

                println!(
                    "[RiskManager] APPROVED order {}: exposure now {:.2}",
                    order.order_id, state.current_exposure
                );

                if let Some(ref executor) = state.executor {
                    executor.cast(ExecutorMessage::ExecuteOrder(order))?;
                }
            }
            RiskMessage::GetExposure(reply) => {
                reply.send(state.current_exposure)?;
            }
            RiskMessage::Reset => {
                println!("[RiskManager] Resetting state");
                state.current_exposure = 0.0;
                state.orders_checked = 0;
                state.orders_rejected = 0;
            }
        }
        Ok(())
    }
}

// Order Executor Types and Implementation
pub enum ExecutorMessage {
    ExecuteOrder(Order),
    GetExecutedCount(ractor::RpcReplyPort<u64>),
}

pub struct OrderExecutor;

pub struct ExecutorState {
    executed_count: u64,
}

#[ractor::async_trait]
impl Actor for OrderExecutor {
    type Msg = ExecutorMessage;
    type State = ExecutorState;
    type Arguments = ();

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        _args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        println!("[Executor] Starting order executor");
        Ok(ExecutorState {
            executed_count: 0,
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            ExecutorMessage::ExecuteOrder(order) => {
                // Simulate sending to exchange
                println!(
                    "[Executor] Executing order {}: {} {} @ {:.2}",
                    order.order_id,
                    match order.side {
                        OrderSide::Buy => "BUY",
                        OrderSide::Sell => "SELL",
                    },
                    order.quantity,
                    order.price
                );

                sleep(Duration::from_millis(100)).await;
                state.executed_count += 1;

                println!("[Executor] Order {} filled", order.order_id);
            }
            ExecutorMessage::GetExecutedCount(reply) => {
                reply.send(state.executed_count)?;
            }
        }
        Ok(())
    }
}

// Trading System Supervisor Types and Implementation
pub enum TradingSystemMessage {
    Start,
    SubmitOrder(Order),
    GetSystemStats(ractor::RpcReplyPort<SystemStats>),
    Shutdown,
}

#[derive(Debug, Clone)]
pub struct SystemStats {
    pub market_data_restarts: u32,
    pub risk_manager_restarts: u32,
    pub orders_executed: u64,
}

pub struct TradingSystemSupervisor {
    symbol: String,
}

pub struct SupervisorState {
    symbol: String,
    market_data: Option<ActorRef<MarketDataMessage>>,
    order_book: Option<ActorRef<OrderBookMessage>>,
    risk_manager: Option<ActorRef<RiskMessage>>,
    executor: Option<ActorRef<ExecutorMessage>>,
    market_data_restarts: u32,
    risk_manager_restarts: u32,
}

#[ractor::async_trait]
impl Actor for TradingSystemSupervisor {
    type Msg = TradingSystemMessage;
    type State = SupervisorState;
    type Arguments = String;

    async fn pre_start(
        &self,
        myself: ActorRef<Self::Msg>,
        symbol: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        println!("[Supervisor] Initializing trading system for {}", symbol);

        // Spawn executor (no supervision needed, stateless)
        let (executor_ref, _) = Actor::spawn(
            Some(format!("{}-executor", symbol)),
            OrderExecutor,
            (),
        ).await?;

        // Spawn risk manager with supervision
        let (risk_ref, _) = Actor::spawn_linked(
            Some(format!("{}-risk", symbol)),
            RiskManager,
            (10000, Some(executor_ref.clone())),
            myself.clone().into(),
        ).await?;

        // Spawn order book
        let (book_ref, _) = Actor::spawn(
            Some(format!("{}-book", symbol)),
            OrderBook { symbol: symbol.clone() },
            (symbol.clone(), Some(risk_ref.clone())),
        ).await?;

        // Spawn market data with supervision (allow first failure)
        let (market_ref, _) = Actor::spawn_linked(
            Some(format!("{}-market", symbol)),
            MarketDataFeed { symbol: symbol.clone() },
            (symbol.clone(), false),
            myself.clone().into(),
        ).await?;

        // Subscribe order book to market data
        market_ref.cast(MarketDataMessage::Subscribe(book_ref.clone()))?;

        Ok(SupervisorState {
            symbol,
            market_data: Some(market_ref),
            order_book: Some(book_ref),
            risk_manager: Some(risk_ref),
            executor: Some(executor_ref),
            market_data_restarts: 0,
            risk_manager_restarts: 0,
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            TradingSystemMessage::Start => {
                if let Some(ref market_data) = state.market_data {
                    println!("[Supervisor] Starting market data feed");
                    market_data.cast(MarketDataMessage::Start)?;
                }
            }
            TradingSystemMessage::SubmitOrder(order) => {
                if let Some(ref order_book) = state.order_book {
                    order_book.cast(OrderBookMessage::PlaceOrder(order))?;
                }
            }
            TradingSystemMessage::GetSystemStats(reply) => {
                let executed = if let Some(ref executor) = state.executor {
                    match executor.call(
                        |r| ExecutorMessage::GetExecutedCount(r),
                        Some(Duration::from_secs(1))
                    ).await {
                        Ok(ractor::rpc::CallResult::Success(count)) => count,
                        _ => 0,
                    }
                } else {
                    0
                };

                reply.send(SystemStats {
                    market_data_restarts: state.market_data_restarts,
                    risk_manager_restarts: state.risk_manager_restarts,
                    orders_executed: executed,
                })?;
            }
            TradingSystemMessage::Shutdown => {
                println!("[Supervisor] Shutting down trading system");
            }
        }
        Ok(())
    }

    async fn handle_supervisor_evt(
        &self,
        myself: ActorRef<Self::Msg>,
        event: SupervisionEvent,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match event {
            SupervisionEvent::ActorFailed(cell, error) => {
                println!("[Supervisor] Actor failed: {}", error);

                // Check which actor failed and restart it immediately
                if let Some(ref market_data) = state.market_data {
                    if market_data.get_cell() == cell {
                        println!("[Supervisor] Market data feed failed, restarting...");
                        state.market_data_restarts += 1;

                        // On restart, skip the simulated failure
                        let (market_ref, _) = Actor::spawn_linked(
                            Some(format!("{}-market", state.symbol)),
                            MarketDataFeed { symbol: state.symbol.clone() },
                            (state.symbol.clone(), true),
                            myself.clone().into(),
                        ).await?;

                        if let Some(ref order_book) = state.order_book {
                            market_ref.cast(MarketDataMessage::Subscribe(order_book.clone()))?;
                        }

                        // Restart the feed
                        market_ref.cast(MarketDataMessage::Start)?;
                        state.market_data = Some(market_ref);
                        println!("[Supervisor] Market data feed restarted");
                        return Ok(());
                    }
                }

                if let Some(ref risk_manager) = state.risk_manager {
                    if risk_manager.get_cell() == cell {
                        println!("[Supervisor] Risk manager failed, restarting...");
                        state.risk_manager_restarts += 1;

                        let executor = state.executor.clone();
                        let (risk_ref, _) = Actor::spawn_linked(
                            Some(format!("{}-risk", state.symbol)),
                            RiskManager,
                            (10000, executor),
                            myself.clone().into(),
                        ).await?;

                        state.risk_manager = Some(risk_ref.clone());

                        // Update order book with new risk manager reference
                        if let Some(ref order_book) = state.order_book {
                            order_book.cast(OrderBookMessage::UpdateRiskManager(risk_ref.clone()))?;
                            println!("[Supervisor] Risk manager restarted with clean state");
                        }
                        return Ok(());
                    }
                }
            }
            SupervisionEvent::ActorTerminated(cell, _, _) => {
                // Actor terminated normally, no action needed
                println!("[Supervisor] Actor terminated: {:?}", cell);
            }
            _ => {}
        }
        Ok(())
    }
}

// Main function
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Starting Trading System ===\n");

    let (supervisor_ref, _supervisor_handle) = Actor::spawn(
        Some("trading-supervisor".to_string()),
        TradingSystemSupervisor { symbol: "AAPL".to_string() },
        "AAPL".to_string(),
    ).await?;

    // Start the market data feed (will fail once, then restart)
    supervisor_ref.cast(TradingSystemMessage::Start)?;

    // Wait for market data to stabilize
    sleep(Duration::from_secs(2)).await;

    println!("\n=== Submitting Orders ===\n");

    // Submit some valid orders
    for i in 0..5 {
        let order = Order {
            order_id: format!("ORDER-{}", i),
            symbol: "AAPL".to_string(),
            side: if i % 2 == 0 { OrderSide::Buy } else { OrderSide::Sell },
            quantity: 100,
            price: 100.0 + (i as f64),
        };
        supervisor_ref.cast(TradingSystemMessage::SubmitOrder(order))?;
        sleep(Duration::from_millis(200)).await;
    }

    // Submit an order that will cause risk manager to crash
    println!("\n=== Triggering Risk Manager Failure ===\n");
    let failing_order = Order {
        order_id: "ORDER-FAIL".to_string(),
        symbol: "AAPL".to_string(),
        side: OrderSide::Buy,
        quantity: 500,
        price: 105.0,
    };
    supervisor_ref.cast(TradingSystemMessage::SubmitOrder(failing_order))?;

    // Wait for restart (longer delay to ensure restart completes)
    sleep(Duration::from_secs(3)).await;

    // Submit more orders after recovery
    println!("\n=== Submitting Orders After Recovery ===\n");
    for i in 5..8 {
        let order = Order {
            order_id: format!("ORDER-{}", i),
            symbol: "AAPL".to_string(),
            side: OrderSide::Buy,
            quantity: 100,
            price: 100.0 + (i as f64),
        };
        supervisor_ref.cast(TradingSystemMessage::SubmitOrder(order))?;
        sleep(Duration::from_millis(200)).await;
    }

    sleep(Duration::from_secs(1)).await;

    // Get final statistics
    match supervisor_ref.call(
        |reply| TradingSystemMessage::GetSystemStats(reply),
        Some(Duration::from_secs(1))
    ).await {
        Ok(ractor::rpc::CallResult::Success(stats)) => {
            println!("\n=== System Statistics ===");
            println!("Market data restarts: {}", stats.market_data_restarts);
            println!("Risk manager restarts: {}", stats.risk_manager_restarts);
            println!("Orders executed: {}", stats.orders_executed);
        }
        _ => {
            println!("\nFailed to get system statistics");
        }
    }

    supervisor_ref.cast(TradingSystemMessage::Shutdown)?;
    sleep(Duration::from_millis(500)).await;

    Ok(())
}
