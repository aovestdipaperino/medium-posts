use distributed_malware_detection::types::Sample;
use distributed_malware_detection::coordinator::{ClusterCoordinator, CoordinatorMessage};
use distributed_malware_detection::supervisor::{RegionalSupervisor, RegionMessage};
use ractor::{Actor, rpc::CallResult};
use tokio::time::{sleep, Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Starting Distributed Malware Detection System ===\n");

    // Spawn cluster coordinator
    let (coordinator_ref, _) = Actor::spawn(
        Some("coordinator".to_string()),
        ClusterCoordinator,
        (),
    ).await?;

    // Spawn multiple regions
    let (region1_ref, _) = Actor::spawn(
        Some("region-us-east".to_string()),
        RegionalSupervisor {
            region: "us-east".to_string(),
            num_workers: 3,
        },
        ("us-east".to_string(), 3, coordinator_ref.clone()),
    ).await?;

    let (region2_ref, _) = Actor::spawn(
        Some("region-eu-west".to_string()),
        RegionalSupervisor {
            region: "eu-west".to_string(),
            num_workers: 2,
        },
        ("eu-west".to_string(), 2, coordinator_ref.clone()),
    ).await?;

    region1_ref.cast(RegionMessage::Start)?;
    region2_ref.cast(RegionMessage::Start)?;

    sleep(Duration::from_millis(500)).await;

    // Check cluster status
    let cluster_status = match coordinator_ref.call(
        |reply| CoordinatorMessage::GetClusterStatus(reply),
        Some(Duration::from_secs(1))
    ).await? {
        CallResult::Success(status) => status,
        _ => panic!("Failed to get cluster status"),
    };

    println!("\n=== Cluster Status ===");
    println!("Total nodes: {}", cluster_status.total_nodes);
    println!("Healthy nodes: {}", cluster_status.healthy_nodes);
    println!("Routers: {}, Workers: {}, Aggregators: {}\n",
             cluster_status.routers, cluster_status.workers, cluster_status.aggregators);

    // Submit samples to different regions
    println!("=== Submitting Samples ===\n");

    for i in 0..50 {
        let sample = Sample {
            id: format!("SAMPLE-{:04}", i),
            data: vec![i as u8, 0xDE, 0xAD, (i * 2) as u8],
            source_region: if i % 2 == 0 { "us-east".to_string() } else { "eu-west".to_string() },
        };

        if i % 2 == 0 {
            region1_ref.cast(RegionMessage::SubmitSample(sample))?;
        } else {
            region2_ref.cast(RegionMessage::SubmitSample(sample))?;
        }

        sleep(Duration::from_millis(50)).await;
    }

    // Wait for processing
    sleep(Duration::from_secs(4)).await;

    // Get regional statistics
    println!("\n=== Regional Statistics ===");

    let region1_stats = match region1_ref.call(
        |reply| RegionMessage::GetRegionStats(reply),
        Some(Duration::from_secs(1))
    ).await? {
        CallResult::Success(stats) => stats,
        _ => panic!("Failed to get region1 stats"),
    };

    println!("[{}] Workers: {}, Processed: {}, Malicious: {}",
             region1_stats.region,
             region1_stats.workers_active,
             region1_stats.samples_processed,
             region1_stats.malicious_detected);

    let region2_stats = match region2_ref.call(
        |reply| RegionMessage::GetRegionStats(reply),
        Some(Duration::from_secs(1))
    ).await? {
        CallResult::Success(stats) => stats,
        _ => panic!("Failed to get region2 stats"),
    };

    println!("[{}] Workers: {}, Processed: {}, Malicious: {}",
             region2_stats.region,
             region2_stats.workers_active,
             region2_stats.samples_processed,
             region2_stats.malicious_detected);

    println!("\n=== System Summary ===");
    println!("Total processed: {}",
             region1_stats.samples_processed + region2_stats.samples_processed);
    println!("Total malicious: {}",
             region1_stats.malicious_detected + region2_stats.malicious_detected);

    Ok(())
}
