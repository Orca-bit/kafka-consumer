use futures::TryStreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::Consumer;
use rdkafka::message::Message;
use tracing::info;

struct CustomConsumerConfig {
    bootstrap_server: String,
    topic: Vec<&'static str>,
    group_id: String,
    auto_offset_reset: String,
}

async fn subscribe_job(config: CustomConsumerConfig) {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.bootstrap_server)
        .set("group.id", &config.group_id)
        .set("auto.offset.reset", &config.auto_offset_reset)
        .set("enable.auto.commit", "true")
        .create()
        .unwrap();

    consumer.subscribe(&config.topic).unwrap();

    let processor = consumer.stream().try_for_each(|borrowed_msg| async move {
        info!(
            "Consumed message with key '{:?}' and payload '{:?}'",
            borrowed_msg.key().unwrap(),
            borrowed_msg.payload().unwrap()
        );
        Ok(())
    });
    let _ = processor.await;
}

#[tokio::main]
async fn main() {
    // 日志
    tracing_subscriber::fmt().init();

    let config = CustomConsumerConfig {
        bootstrap_server: "".to_string(),
        topic: vec![""],
        group_id: "my_group".to_string(),
        auto_offset_reset: "earliest".to_string(),
    };
    tokio::spawn(subscribe_job(config));
}
