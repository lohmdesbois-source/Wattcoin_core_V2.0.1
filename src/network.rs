use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use futures::{SinkExt, StreamExt};
use bytes::Bytes;
use std::sync::{Arc, Mutex};
use serde::{Serialize, Deserialize};
use rand::Rng;

use crate::block::Block;
use crate::blockchain::Blockchain;
use crate::transaction::Transaction;
use crate::api::{Order, SharedPool};

#[derive(Serialize, Deserialize, Debug)]
pub enum P2PMessage {
    Handshake { genesis_hash: String, current_height: u64, sender_port: String },
    SyncResponse { blocks: Vec<Block> },
    NewBlock { block: Block, sender_port: String }, 
    WhisperTransaction { tx: Transaction },    
    BroadcastTransaction { tx: Transaction },  
    BroadcastOrder { order: Order },
}

pub async fn start_p2p_server(port: &str, blockchain: Arc<Mutex<Blockchain>>, mempool: Arc<Mutex<Vec<Transaction>>>, dex_pool: SharedPool) {
    let address = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&address).await.unwrap();
    println!("📡 Serveur P2P à l'écoute sur TCP/{} (Mode Encadré + IP Dynamique)...", port);
    
    let my_port = port.to_string(); 

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        
        // 🌍 DÉCOUVERTE DYNAMIQUE DE L'IP DU VOISIN !
        let peer_ip = socket.peer_addr().unwrap().ip().to_string();
        
        let blockchain_clone = Arc::clone(&blockchain);
        let mempool_clone = Arc::clone(&mempool); 
        let dex_pool_clone = Arc::clone(&dex_pool);
        let my_port_clone = my_port.clone();

        tokio::spawn(async move {
            let mut framed_socket = Framed::new(socket, LengthDelimitedCodec::new());

            while let Some(result) = framed_socket.next().await {
                match result {
                    Ok(bytes) => {
                        let message_str = String::from_utf8_lossy(&bytes);
                        
                        if let Ok(message) = serde_json::from_str::<P2PMessage>(&message_str) {
                            let mut chain = blockchain_clone.lock().unwrap();
                            
                            match message {
                                P2PMessage::Handshake { genesis_hash, current_height, sender_port } => {
                                    // On recompose l'adresse complète : IP_DÉCOUVERTE:PORT_DÉCLARÉ
                                    let return_addr = format!("{}:{}", peer_ip, sender_port);
                                    
                                    let my_genesis = &chain.chain[0].header.hash;
                                    if genesis_hash != *my_genesis {
                                        println!("🚨 [P2P] INTRUSION REJETÉE depuis {}.", peer_ip);
                                    } else {
                                        let my_height = chain.chain.len() as u64;
                                        if current_height < my_height {
                                            println!("🔄 [P2P] Le nœud {} est en RETARD. Envoi de l'historique...", return_addr);
                                            let all_blocks = chain.chain.clone();
                                            tokio::spawn(async move {
                                                send_sync_response(&return_addr, all_blocks).await;
                                            });
                                        } else {
                                            println!("🤝 [P2P] Poignée de main ok avec {}.", return_addr);
                                        }
                                    }
                                },
                                
                                P2PMessage::SyncResponse { blocks } => {
                                    println!("📦 [P2P] Réception d'une synchronisation massive ({} blocs) depuis {} !", blocks.len(), peer_ip);
                                    let my_work = Blockchain::calculate_total_work(&chain.chain);
                                    let new_work = Blockchain::calculate_total_work(&blocks);
                                    
                                    if new_work > my_work {
                                        println!("⚖️ Le Juge a pesé : La nouvelle chaîne est plus LOURDE !");
                                        if chain.resolve_fork(blocks) {
                                            println!("✅ Synchronisation réussie ! Nous sommes à jour.");
                                        }
                                    }
                                },

                                P2PMessage::NewBlock { block, sender_port } => {
                                    let return_addr = format!("{}:{}", peer_ip, sender_port);
                                    let my_height = chain.chain.len() as u64;
                                    
                                    if block.header.index > my_height {
                                        println!("⏩ [P2P] Bloc du futur reçu ({}). Demande de mise à jour à {} !", block.header.index, return_addr);
                                        let my_genesis = chain.chain[0].header.hash.clone();
                                        let port_for_sync = my_port_clone.clone();
                                        tokio::spawn(async move {
                                            send_handshake(&return_addr, &port_for_sync, my_genesis, my_height).await;
                                        });
                                    } else if block.header.index == my_height {
                                        println!("\n🌍 [P2P] Nouveau BLOC {} reçu en direct !", block.header.index);
                                        let block_to_clean = block.clone(); 

                                        if let Err(e) = chain.validate_and_add_external_block(block) {
                                            println!("   🚨 BLOC REJETÉ : {}", e);
                                        } else {
                                            let mut mp = mempool_clone.lock().unwrap();
                                            mp.retain(|tx| {
                                                !block_to_clean.transactions.iter().any(|mined_tx| mined_tx.kyber_capsule == tx.kyber_capsule)
                                            });
                                        }
                                    }
                                },

                                P2PMessage::WhisperTransaction { tx } => {
                                    let mut rng = rand::thread_rng();
                                    if rng.gen_range(1..=10) <= 2 {
                                        let mut pool = mempool_clone.lock().unwrap();
                                        pool.push(tx.clone());
                                    }
                                },

                                P2PMessage::BroadcastTransaction { tx } => {
                                    if tx.is_valid() {
                                        let mut pool = mempool_clone.lock().unwrap();
                                        if !pool.iter().any(|t| t.dilithium_signature == tx.dilithium_signature) {
                                            pool.push(tx.clone());
                                        }
                                    }
                                },
                                
                                P2PMessage::BroadcastOrder { order } => {
                                    let mut pool = dex_pool_clone.lock().unwrap();
                                    if !pool.iter().any(|o| o.id == order.id) {
                                        pool.push(order.clone());
                                    }
                                },
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        });
    }
}

// 🌐 LES FONCTIONS D'ENVOI PRENNENT MAINTENANT DES ADRESSES COMPLÈTES (IP:PORT)
pub async fn broadcast_block(target_addr: &str, my_port: &str, block: Block) {
    if let Ok(socket) = TcpStream::connect(target_addr).await {
        let mut framed = Framed::new(socket, LengthDelimitedCodec::new());
        let envelope = P2PMessage::NewBlock { block, sender_port: my_port.to_string() };
        let _ = framed.send(Bytes::from(serde_json::to_string(&envelope).unwrap())).await;
    }
}

pub async fn send_handshake(target_addr: &str, my_port: &str, genesis_hash: String, current_height: u64) {
    if let Ok(socket) = TcpStream::connect(target_addr).await {
        let mut framed = Framed::new(socket, LengthDelimitedCodec::new());
        let envelope = P2PMessage::Handshake { genesis_hash, current_height, sender_port: my_port.to_string() };
        let _ = framed.send(Bytes::from(serde_json::to_string(&envelope).unwrap())).await;
    }
}

pub async fn send_sync_response(target_addr: &str, blocks: Vec<Block>) {
    if let Ok(socket) = TcpStream::connect(target_addr).await {
        let mut framed = Framed::new(socket, LengthDelimitedCodec::new());
        let envelope = P2PMessage::SyncResponse { blocks };
        let _ = framed.send(Bytes::from(serde_json::to_string(&envelope).unwrap())).await;
    }
}

pub async fn broadcast_transaction(target_addr: &str, tx: Transaction) {
    if let Ok(socket) = TcpStream::connect(target_addr).await {
        let mut framed = Framed::new(socket, LengthDelimitedCodec::new());
        let envelope = P2PMessage::BroadcastTransaction { tx };
        let _ = framed.send(Bytes::from(serde_json::to_string(&envelope).unwrap())).await;
    }
}

pub async fn broadcast_order(target_addr: &str, order: Order) {
    if let Ok(socket) = TcpStream::connect(target_addr).await {
        let mut framed = Framed::new(socket, LengthDelimitedCodec::new());
        let envelope = P2PMessage::BroadcastOrder { order };
        let _ = framed.send(Bytes::from(serde_json::to_string(&envelope).unwrap())).await;
    }
}