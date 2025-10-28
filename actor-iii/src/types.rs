use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct Sample {
    pub id: String,
    pub data: Vec<u8>,
    pub source_region: String,
}

#[derive(Debug, Clone)]
pub struct DetectionResult {
    pub sample_id: String,
    pub is_malicious: bool,
    pub family: Option<String>,
    pub confidence: f64,
}

#[derive(Debug, Clone)]
pub struct WorkerStats {
    pub samples_analyzed: u64,
    pub malicious_detected: u64,
    pub analysis_time_ms: u64,
}

#[derive(Debug, Clone)]
pub struct AggregatorStats {
    pub total_detections: u64,
    pub malicious_count: u64,
    pub family_counts: BTreeMap<String, u64>,
}

#[derive(Debug, Clone)]
pub enum NodeType {
    Router,
    Worker,
    Aggregator,
}

#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub node_type: NodeType,
    pub last_heartbeat: std::time::Instant,
}

#[derive(Debug, Clone)]
pub struct ClusterStatus {
    pub total_nodes: usize,
    pub healthy_nodes: usize,
    pub routers: usize,
    pub workers: usize,
    pub aggregators: usize,
}

#[derive(Debug, Clone)]
pub struct RegionStats {
    pub region: String,
    pub workers_active: usize,
    pub samples_processed: u64,
    pub malicious_detected: u64,
}
