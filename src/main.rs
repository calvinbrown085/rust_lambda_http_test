use bytes::Bytes;
use lambda_runtime::error::HandlerError;
use lambda_runtime::*;
use rusoto_core::credential::AwsCredentials;
use rusoto_core::{HttpClient, Region};
use rusoto_kinesis::{Kinesis, KinesisClient, PutRecordsInput, PutRecordsRequestEntry};
use rusoto_credential::ProvideAwsCredentials;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Clone)]
struct KinesisEvent {
    #[serde(rename = "Records")]
    records: Vec<Record>,
}
#[derive(Deserialize, Clone)]
struct Record {
    kinesis: KinesisRecord,
}
#[derive(Deserialize, Clone)]
struct KinesisRecord {
    #[serde(rename = "partitionKey")]
    partition_key: String,
    data: String,
}

fn main() {
    lambda!(my_handler)
}

fn my_handler(e: KinesisEvent, ctx: Context) -> Result<PutRecordsInput, HandlerError> {
    let kinesis_client = KinesisClient::new(
        Region::UsEast1,
    );

    let data_vec: Vec<&String> = e
        .records
        .iter()
        .map(|record| &record.kinesis.data)
        .collect();
    let put_record_vec: Vec<PutRecordsRequestEntry> = e
        .records
        .iter()
        .map(|record| PutRecordsRequestEntry {
            data: Bytes::from(record.kinesis.data.clone()),
            explicit_hash_key: None,
            partition_key: record.kinesis.partition_key.clone(),
        })
        .collect();
    let put_records_input: PutRecordsInput = PutRecordsInput {
        records: put_record_vec,
        stream_name: String::from("basic-tweet-stream"),
    };
    kinesis_client.put_records(put_records_input.clone());

    Ok(put_records_input)
}
