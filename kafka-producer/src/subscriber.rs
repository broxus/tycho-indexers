use std::time::Duration;

use anyhow::Result;
use everscale_types::boc::BocRepr;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesOrdered;
use futures_util::{FutureExt, StreamExt};
use rdkafka::producer::FutureRecord;
use tycho_block_util::block::BlockStuff;
use tycho_core::block_strider::{StateSubscriber, StateSubscriberContext};

use crate::config::KafkaConsumerConfig;

pub struct KafkaProducer {
    producer: rdkafka::producer::FutureProducer,
    config: KafkaConsumerConfig,
}

impl KafkaProducer {
    pub fn new(config: KafkaConsumerConfig) -> Result<Self> {
        let mut client_config = rdkafka::config::ClientConfig::new();
        client_config.set("bootstrap.servers", &config.brokers);

        if let Some(message_timeout_ms) = config.message_timeout_ms {
            client_config.set("message.timeout.ms", message_timeout_ms.to_string());
        }
        if let Some(message_max_size) = config.message_max_size {
            client_config.set("message.max.bytes", message_max_size.to_string());
        }

        #[cfg(feature = "ssl")]
        match &config.security_config {
            #[cfg(feature = "ssl")]
            Some(crate::config::SecurityConfig::Ssl(config)) => {
                client_config
                    .set("security.protocol", &config.security_protocol)
                    .set("ssl.ca.location", &config.ssl_ca_location)
                    .set("ssl.key.location", &config.ssl_key_location)
                    .set("ssl.certificate.location", &config.ssl_certificate_location);

                if let Some(enable_verification) = config.enable_ssl_certificate_verification {
                    client_config.set(
                        "enable.ssl.certificate.verification",
                        if enable_verification { "true" } else { "false" },
                    );
                }
            }
            None => {}
        }

        let producer = client_config.create()?;

        Ok(Self { producer, config })
    }
    async fn handle_block(&self, block_stuff: &BlockStuff) -> Result<()> {
        let block_id = *block_stuff.id();

        let extra = block_stuff.load_extra()?;
        let account_blocks = extra.account_blocks.load()?;

        let transactions: anyhow::Result<Vec<_>> = tokio::task::spawn_blocking(move || {
            let mut compressor = ton_block_compressor::ZstdWrapper::with_level(3);
            let mut transactions = Vec::new();

            for account_block in account_blocks.iter() {
                let (addr, _, block) = account_block?;
                for transaction in block.transactions.iter() {
                    let (_, _, transaction) = transaction?;
                    let hash = transaction.inner().repr_hash().0.to_vec();
                    let transaction = transaction.load()?;
                    let timestamp = transaction.now;
                    let data = BocRepr::encode(&transaction)?;
                    let data = compressor.compress(&data)?.to_vec();

                    let partition = if block_id.is_masterchain() {
                        0
                    } else {
                        // first 3 bits of the account id
                        1 + (addr[0] >> 5)
                    };

                    transactions.push(TransactionToKafka {
                        hash,
                        data,
                        partition,
                        timestamp,
                    });
                }
            }
            Ok(transactions)
        })
        .await?;
        let transactions = transactions?;

        let mut futures: FuturesOrdered<_> = transactions
            .into_iter()
            .map(move |tx| self.send_with_retry(tx))
            .collect();

        while futures.next().await.is_some() {}

        Ok(())
    }

    async fn send_with_retry(&self, record: TransactionToKafka) {
        let mut attempt = 0;
        loop {
            let record = FutureRecord::to(&self.config.topic)
                .key(&record.hash)
                .payload(&record.data)
                .timestamp((record.timestamp as i64) * 1_000) // milliseconds
                .partition(record.partition as i32);
            let queue_timeout = Duration::from_millis(self.config.session_timeout_ms as u64);
            match self.producer.send(record, queue_timeout).await {
                Ok(_) => return,
                Err((error, _)) => {
                    attempt += 1;
                    tracing::warn!("Failed to send message (attempt {attempt}): {error:?}",);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }
}

struct TransactionToKafka {
    hash: Vec<u8>,
    data: Vec<u8>,
    partition: u8,
    timestamp: u32,
}

impl StateSubscriber for KafkaProducer {
    type HandleStateFut<'a> = BoxFuture<'a, Result<()>>;

    fn handle_state<'a>(&'a self, cx: &'a StateSubscriberContext) -> Self::HandleStateFut<'a> {
        async move {
            self.handle_block(&cx.block).await?;
            Ok(())
        }
        .boxed()
    }
}
