use lambda_runtime::*;
use serde::{Serialize, Deserialize};
use lambda_runtime::error::HandlerError;

#[derive(Deserialize, Clone)]
struct KinesisEvent {
    #[serde(rename = "Records")]
    records: Vec<Record>
}
#[derive(Deserialize, Clone)]
struct Record {
    kinesis: KinesisRecord
}
#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct KinesisRecord {
    partition_key: String,
    data: String,
}
#[derive(Serialize, Deserialize, Clone)]
struct CustomOutput {
    message: String
}

fn main() {lambda!(my_handler)}

fn my_handler(e: KinesisEvent, ctx: Context) -> Result<CustomOutput, HandlerError> {
    let data_vec: Vec<&String> = e.records.iter().map(|record| &record.kinesis.data).collect();
    Ok(CustomOutput{
        message: format!("Kinesis Events: {:#?}", &data_vec),
    })
}