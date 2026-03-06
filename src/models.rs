use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
pub struct BinanceTrade {
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "q")]
    pub volume: String,
    #[serde(rename = "T")]
    pub time: i64,
    #[serde(rename = "m")]
    pub is_buyer_maker: bool
}