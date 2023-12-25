use std::{sync::{Arc, Mutex}, env, time::Duration};

use serenity::{prelude::{GatewayIntents, EventHandler, Context}, http::{HttpBuilder, Http}, client::ClientBuilder, model::prelude::{Message, ChannelId}, async_trait};

use rand::prelude::*;
use std::sync::atomic::Ordering;


#[tokio::test]
async fn works_normally() {
    let test_message = format!("Test {}", random::<u32>());
    generic(WhenSendMessage::BeforeStarted, false, &test_message, vec![]).await;

    let test_message = format!("Test {}", random::<u32>());
    generic(WhenSendMessage::BeforeStarted2, false, &test_message, vec![]).await;

    let test_message = format!("Test {}", random::<u32>());
    generic(WhenSendMessage::StartedStillRunning, false, &test_message.clone(), vec![test_message]).await;

    let test_message = format!("Test {}", random::<u32>());
    generic(WhenSendMessage::StartedThenStopped, false, &test_message, vec![]).await;
}

#[tokio::test]
async fn resume_works() {
    let test_message = format!("Test {}", random::<u32>());
    generic(WhenSendMessage::StartedThenStopped, true, &test_message.clone(), vec![test_message]).await;

    let test_message = format!("Test {}", random::<u32>());
    generic(WhenSendMessage::StartedThenStoppedThenRestartedAfter, true, &test_message.clone(), vec![test_message]).await;
}

enum WhenSendMessage {
    BeforeStarted,
    BeforeStarted2,
    StartedStillRunning,
    StartedThenStopped,
    StartedThenStoppedThenRestartedAfter,
}

async fn generic(when_to_send_message: WhenSendMessage, resume: bool, test_message: &str, expected: Vec<String>) {

    let token = env::var("DISCORD_TOKEN").expect("DISCORD_TOKEN not set in the environment");
    let channel = Into::<ChannelId>::into(
        env::var("DISCORD_CHANNEL")
        .expect("DISCORD_CHANNEL not set in the environment")
        .parse::<u64>()
        .expect("DISCORD_CHANNEL not a valid snowflake")
    );
    
    
    //let test_message2 = format!("Test 5678");

    let intents = GatewayIntents::non_privileged() | GatewayIntents::MESSAGE_CONTENT | GatewayIntents::GUILD_MEMBERS;

    let new_http = || {
        let reqwest_client = reqwest::Client::builder()
            .build()
            .unwrap();

        HttpBuilder::new(&token)
            .client(reqwest_client)
            .build()
    };

    let http = new_http();

    println!("Connect done");

    if matches!(when_to_send_message, WhenSendMessage::BeforeStarted) {
        send_message(&http, channel, &test_message).await;
    }

    let messages = Arc::new(Mutex::new(Vec::new()));

    let mut discord_client_builder = ClientBuilder::new_with_http(new_http(), intents)
            .event_handler(MyHandler { messages: messages.clone() });

    let session_id_recv = discord_client_builder.set_session_id(None);
    let seq_num = discord_client_builder.get_seq_num();

    let mut discord_client = discord_client_builder
        .await
        .expect("Error creating client");
    println!("Have discord client");

    if matches!(when_to_send_message, WhenSendMessage::BeforeStarted2) {
        send_message(&http, channel, &test_message).await;
    }

    discord_client.start().await.expect("Failed to start discord listener");
    println!("Start done");
    let http = Arc::clone(&discord_client.cache_and_http.http);
    
    if matches!(when_to_send_message, WhenSendMessage::StartedStillRunning) {
        send_message(&http, channel, &test_message).await;
    }

    // Stop the client.
    discord_client.shard_manager.lock().await.shutdown_all().await;

    //let state = ???;
    let mut session_id = None;
    loop {
        match session_id_recv.try_recv() {
            Ok(x) => session_id = x,
            Err(_) => break,
        }
    };

    if matches!(when_to_send_message, WhenSendMessage::StartedThenStopped) {
        send_message(&http, channel, &test_message).await;
    }

    if resume {
        let mut discord_client_builder = ClientBuilder::new_with_http(new_http(), intents)
            .event_handler(MyHandler { messages: messages.clone() });

        discord_client_builder.set_session_id(session_id);

        discord_client_builder
            .seq_num(seq_num.load(Ordering::Acquire))
            .await
            .expect("Error resuming client");

        println!("Have discord resumed client");
        

        if matches!(when_to_send_message, WhenSendMessage::StartedThenStoppedThenRestartedAfter) {
            send_message(&http, channel, &test_message).await;
        }
        else {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    assert_eq!(messages.lock().unwrap().iter().map(|x| x.content.clone()).collect::<Vec<_>>(), expected);
}

struct MyHandler {
    messages: Arc<Mutex<Vec<Message>>>
}

async fn send_message(http: &Http, channel: ChannelId, message_text: &str) {
    tokio::time::sleep(Duration::from_secs(1)).await;
    channel.send_message(http, |m| { m.content(message_text.clone()) }).await
        .expect("Failed to send message 1");
    tokio::time::sleep(Duration::from_secs(1)).await;
}

#[async_trait]
impl EventHandler for MyHandler {
    async fn message(&self, _ctx: Context, new_message: Message) {
        self.messages.lock().expect("Poisoned lock").push(new_message.clone());
    }

}