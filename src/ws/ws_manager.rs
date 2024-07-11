use crate::{
    prelude::*,
    ws::message_types::{AllMids, Candle, L2Book, OrderUpdates, Trades, User},
    Error, Notification, UserFills, UserFundings, UserNonFundingLedgerUpdates,
};
use futures_util::{stream::SplitSink, SinkExt, StreamExt};
use log::{error, warn};
use serde::{Deserialize, Serialize};
use std::{borrow::BorrowMut, collections::HashMap, ops::DerefMut, sync::Arc, time::Duration};
use tokio::{
    net::TcpStream,
    spawn,
    sync::{mpsc::UnboundedSender, Mutex},
    time,
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{self, protocol},
    MaybeTlsStream, WebSocketStream,
};
use ethers::types::H160;

#[derive(Debug)]
struct SubscriptionData {
    sending_channel: UnboundedSender<Message>,
    subscription_id: u32,
    identifier : String,
}
pub(crate) struct WsManager {
    writer: Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, protocol::Message>>>,
    subscriptions: Arc<Mutex<HashMap<String, Vec<SubscriptionData>>>>,
    subscription_id: u32,
    subscription_identifiers: HashMap<u32, String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
#[serde(rename_all = "camelCase")]
pub enum Subscription {
    AllMids,
    Trades { coin: String },
    L2Book { coin: String },
    UserEvents { user: H160 },
    UserFills { user: H160 },
    Candle { coin: String, interval: String },
    OrderUpdates { user: H160 },
    UserFundings { user: H160 },
    UserNonFundingLedgerUpdates { user: H160 },
    Notification { user: H160 },
}

#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "channel")]
#[serde(rename_all = "camelCase")]
pub enum Message {
    AllMids(AllMids),
    Trades(Trades),
    L2Book(L2Book),
    User(User),
    UserFills(UserFills),
    Candle(Candle),
    SubscriptionResponse,
    OrderUpdates(OrderUpdates),
    UserFundings(UserFundings),
    UserNonFundingLedgerUpdates(UserNonFundingLedgerUpdates),
    Notification(Notification),
    Pong,
    Disconnect
}

#[derive(Serialize)]
pub(crate) struct SubscriptionSendData<'a> {
    method: &'static str,
    subscription: &'a serde_json::Value,
}

#[derive(Serialize)]
pub(crate) struct Ping {
    method: &'static str,
}

impl WsManager {
    const SEND_PING_INTERVAL: u64 = 50;

    pub(crate) async fn new(url: String) -> Result<WsManager> {
        
        let (writer, mut reader) = Self::connect(url.as_str()).await?.split();
        let writer = Arc::new(Mutex::new(writer));

        let subscriptions_map: HashMap<String, Vec<SubscriptionData>> = HashMap::new();
        let subscriptions = Arc::new(Mutex::new(subscriptions_map));
        let subscriptions_copy = Arc::clone(&subscriptions);

        {
            let writer = writer.clone();
            let reader_fut = async move {
                loop {
                    let data = reader.next().await;
                    if data.is_none() {
                        warn!("WS Manager disconnect");
                        for (_, v) in subscriptions_copy.lock().await.iter() {
                            for data in v.iter() {
                                if let Err(err) = data.sending_channel.send(Message::Disconnect) {
                                    warn!("Error sending disconnection notification {err}");
                                }
                            }
                        }
                        match Self::connect(url.as_str()).await {
                            Ok(ws) => {
                                let (writer_inner, reader_inner) = ws.split();
                                reader = reader_inner;
                                let mut writer_guard = writer.lock().await;
                                *writer_guard = writer_inner;
                                for (identifier, v) in subscriptions_copy.lock().await.iter() {
                                    if identifier.eq("userEvents") || identifier.eq("orderUpdates") { //TODO should these special keys be removed and instead use the simpler direct identifier mapping?
                                        for data in v.iter() { 
                                            if let Err(error) = Self::send_subscribe(writer_guard.deref_mut(), &data.identifier).await {
                                                warn!("Could not resubscribe correctly {identifier}: {error}");
                                            }
                                        }
                                    }
                                    else if let Err(error) = Self::send_subscribe(writer_guard.deref_mut(), identifier).await {
                                        warn!("Could not resubscribe correctly {identifier}: {error}");
                                    }
                                }
                            },
                            Err(err) => error!("Could not connect to websocket {err}"),
                        }
                    }
                    else if let Err(err) = WsManager::parse_and_send_data(data, &subscriptions_copy).await {
                        error!("Error processing data received by WS manager reader: {err}");
                    }
                }
            };
            spawn(reader_fut);
        }

        {
            let writer = Arc::clone(&writer);
            let ping_fut = async move {
                loop {
                    match serde_json::to_string(&Ping { method: "ping" }) {
                        Ok(payload) => {
                            let mut writer = writer.lock().await;
                            if let Err(err) = writer.send(protocol::Message::Text(payload)).await {
                                error!("Error pinging server: {err}")
                            }
                        }
                        Err(err) => error!("Error serializing ping message: {err}"),
                    }
                    time::sleep(Duration::from_secs(Self::SEND_PING_INTERVAL)).await;
                }
            };
            spawn(ping_fut);
        }

        Ok(WsManager {
            writer,
            subscriptions,
            subscription_id: 0,
            subscription_identifiers: HashMap::new(),
        })
    }

    async fn connect(url : &str) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>> {
        Ok(connect_async(url )
            .await
            .map_err(|e| Error::Websocket(e.to_string()))?.0)
    }

    fn get_identifier(message: &Message) -> Result<String> {
        match message {
            Message::AllMids(_) => serde_json::to_string(&Subscription::AllMids)
                .map_err(|e| Error::JsonParse(e.to_string())),
            Message::User(_) => Ok("userEvents".to_string()),
            Message::UserFills(_) => Ok("userFills".to_string()),
            Message::Trades(trades) => {
                if trades.data.is_empty() {
                    Ok(String::default())
                } else {
                    serde_json::to_string(&Subscription::Trades {
                        coin: trades.data[0].coin.clone(),
                    })
                    .map_err(|e| Error::JsonParse(e.to_string()))
                }
            }
            Message::L2Book(l2_book) => serde_json::to_string(&Subscription::L2Book {
                coin: l2_book.data.coin.clone(),
            })
            .map_err(|e| Error::JsonParse(e.to_string())),
            Message::Candle(candle) => serde_json::to_string(&Subscription::Candle {
                coin: candle.data.coin.clone(),
                interval: candle.data.interval.clone(),
            })
            .map_err(|e| Error::JsonParse(e.to_string())),
            Message::OrderUpdates(_) => Ok("orderUpdates".to_string()),
            Message::UserFundings(_) => Ok("userFundings".to_string()),
            Message::UserNonFundingLedgerUpdates(user_non_funding_ledger_updates) => {
                serde_json::to_string(&Subscription::UserNonFundingLedgerUpdates {
                    user: user_non_funding_ledger_updates.data.user,
                })
                .map_err(|e| Error::JsonParse(e.to_string()))
            }
            Message::Notification(_) => Ok("notification".to_string()),
            Message::SubscriptionResponse | Message::Pong => Ok(String::default()),
            Message::Disconnect => Ok(String::default()),
        }
    }

    async fn parse_and_send_data(
        data: Option<std::result::Result<protocol::Message, tungstenite::Error>>,
        subscriptions: &Arc<Mutex<HashMap<String, Vec<SubscriptionData>>>>,
    ) -> Result<()> {
        let data = data
            .ok_or(Error::ReaderDataNotFound)?
            .map_err(|e| Error::GenericReader(e.to_string()))?
            .into_text()
            .map_err(|e| Error::ReaderTextConversion(e.to_string()))?;
        if !data.starts_with('{') {
            return Ok(());
        }
        let message =
            serde_json::from_str::<Message>(&data).map_err(|e| Error::JsonParse(e.to_string()))?;
        let identifier = WsManager::get_identifier(&message)?;
        if identifier.is_empty() {
            return Ok(());
        }

        let mut subscriptions = subscriptions.lock().await;
        let mut res = Ok(());
        if let Some(subscription_datas) = subscriptions.get_mut(&identifier) {
            for subscription_data in subscription_datas {
                if let Err(e) = subscription_data
                    .sending_channel
                    .send(message.clone())
                    .map_err(|e| Error::WsSend(e.to_string()))
                {
                    res = Err(e);
                }
            }
        }
        res
    }

    async fn send_subscribe(writer : &mut SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, protocol::Message>, identifier : &str) -> Result<()> {
        let payload = serde_json::to_string(&SubscriptionSendData {
            method: "subscribe",
            subscription: &serde_json::from_str::<serde_json::Value>(identifier)
                .map_err(|e| Error::JsonParse(e.to_string()))?,
        })
        .map_err(|e| Error::JsonParse(e.to_string()))?;

        writer
            .send(protocol::Message::Text(payload))
            .await
            .map_err(|e| Error::Websocket(e.to_string()))?;
        Ok(())
    }

    pub(crate) async fn add_subscription(
        &mut self,
        identifier: String,
        sending_channel: UnboundedSender<Message>,
    ) -> Result<u32> {
        let mut subscriptions = self.subscriptions.lock().await;

        let identifier_entry = if let Subscription::UserEvents { user: _ } =
            serde_json::from_str::<Subscription>(&identifier)
                .map_err(|e| Error::JsonParse(e.to_string()))?
        {
            "userEvents".to_string()
        } else if let Subscription::OrderUpdates { user: _ } =
            serde_json::from_str::<Subscription>(&identifier)
                .map_err(|e| Error::JsonParse(e.to_string()))?
        {
            "orderUpdates".to_string()
        } else {
            identifier.clone()
        };
        let subscriptions = subscriptions
            .entry(identifier_entry.clone())
            .or_insert(Vec::new());

        if !subscriptions.is_empty() && identifier_entry.eq("userEvents") {
            return Err(Error::UserEvents);
        }

        if subscriptions.is_empty() {
            Self::send_subscribe(self.writer.lock().await.borrow_mut(), identifier.as_str()).await?;
        }

        let subscription_id = self.subscription_id;
        self.subscription_identifiers
            .insert(subscription_id, identifier.clone());
        subscriptions.push(SubscriptionData {
            sending_channel,
            subscription_id,
            identifier,
        });

        self.subscription_id += 1;
        Ok(subscription_id)
    }

    pub(crate) async fn remove_subscription(&mut self, subscription_id: u32) -> Result<()> {
        let identifier = self
            .subscription_identifiers
            .get(&subscription_id)
            .ok_or(Error::SubscriptionNotFound)?
            .clone();

        let identifier_entry = if let Subscription::UserEvents { user: _ } =
            serde_json::from_str::<Subscription>(&identifier)
                .map_err(|e| Error::JsonParse(e.to_string()))?
        {
            "userEvents".to_string()
        } else if let Subscription::OrderUpdates { user: _ } =
            serde_json::from_str::<Subscription>(&identifier)
                .map_err(|e| Error::JsonParse(e.to_string()))?
        {
            "orderUpdates".to_string()
        } else {
            identifier.clone()
        };

        self.subscription_identifiers.remove(&subscription_id);

        let mut subscriptions = self.subscriptions.lock().await;

        let subscriptions = subscriptions
            .get_mut(&identifier_entry)
            .ok_or(Error::SubscriptionNotFound)?;
        let index = subscriptions
            .iter()
            .position(|subscription_data| subscription_data.subscription_id == subscription_id)
            .ok_or(Error::SubscriptionNotFound)?;
        subscriptions.remove(index);

        if subscriptions.is_empty() {
            let payload = serde_json::to_string(&SubscriptionSendData {
                method: "unsubscribe",
                subscription: &serde_json::from_str::<serde_json::Value>(&identifier)
                    .map_err(|e| Error::JsonParse(e.to_string()))?,
            })
            .map_err(|e| Error::JsonParse(e.to_string()))?;

            let mut writer = self.writer.lock().await;
            writer
                .send(protocol::Message::Text(payload))
                .await
                .map_err(|e| Error::Websocket(e.to_string()))?;
        }
        Ok(())
    }
}
