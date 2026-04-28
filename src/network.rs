use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use futures::{SinkExt, StreamExt}; // Nécessaire pour .send() et .next()
use bytes::Bytes;
use std::sync::{Arc, Mutex};
use serde::{Serialize, Deserialize};
use rand::Rng; // Pour le Dandelion++

use crate::block::Block;
use crate::blockchain::Blockchain;
use crate::transaction::Transaction;
use crate::api::{Order, SharedPool};

// 🎛️ LE SWITCH RÉSEAU CENTRALISÉ
//const TARGET_IP: &str = "127.0.0.1";
const TARGET_IP: &str = "80.78.26.243";


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
    println!("📡 Serveur P2P à l'écoute sur TCP/{} (Mode Encadré/Framed)...", port);
    
    let my_port = port.to_string(); 

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let blockchain_clone = Arc::clone(&blockchain);
        let mempool_clone = Arc::clone(&mempool); 
        let dex_pool_clone = Arc::clone(&dex_pool);
        let my_port_clone = my_port.clone();

        tokio::spawn(async move {
            // 🛡️ L'ARMURE DU P2P
            let mut framed_socket = Framed::new(socket, LengthDelimitedCodec::new());

            // 🔄 On écoute le flux de paquets parfaits
            while let Some(result) = framed_socket.next().await {
                match result {
                    Ok(bytes) => {
                        let message_str = String::from_utf8_lossy(&bytes);
                        
                        if let Ok(message) = serde_json::from_str::<P2PMessage>(&message_str) {
                            let mut chain = blockchain_clone.lock().unwrap();
                            
                            // 🧠 LA LOGIQUE P2P RÉINTÉGRÉE
                            match message {
                                // 🤝 GESTION DU HANDSHAKE
                                P2PMessage::Handshake { genesis_hash, current_height, sender_port } => {
                                    let my_genesis = &chain.chain[0].header.hash;
                                    if genesis_hash != *my_genesis {
                                        println!("🚨 [P2P] INTRUSION REJETÉE.");
                                    } else {
                                        let my_height = chain.chain.len() as u64;
                                        if current_height < my_height {
                                            println!("🔄 [P2P] Le nœud {} est en RETARD. Envoi de l'historique...", sender_port);
                                            let all_blocks = chain.chain.clone();
                                            tokio::spawn(async move {
                                                send_sync_response(&sender_port, all_blocks).await;
                                            });
                                        } else {
                                            println!("🤝 [P2P] Poignée de main ok avec {}.", sender_port);
                                        }
                                    }
                                },
                                
                                // 📥 RÉCEPTION DE SYNCHRONISATION
                                P2PMessage::SyncResponse { blocks } => {
                                    println!("📦 [P2P] Réception d'une synchronisation massive ({} blocs) !", blocks.len());
                                    let my_work = Blockchain::calculate_total_work(&chain.chain);
                                    let new_work = Blockchain::calculate_total_work(&blocks);
                                    
                                    if new_work > my_work {
                                        println!("⚖️ Le Juge a pesé : La nouvelle chaîne est plus LOURDE !");
                                        if chain.resolve_fork(blocks) {
                                            println!("✅ Synchronisation réussie ! Nous sommes à jour.");
                                        } else {
                                            println!("❌ Chaîne massive rejetée par le Juge.");
                                        }
                                    } else {
                                        println!("🛡️ Chaîne massive ignorée : Notre chaîne est plus lourde ou égale !");
                                    }
                                },

                                // 🧱 RÉCEPTION D'UN BLOC EN DIRECT
                                P2PMessage::NewBlock { block, sender_port } => {
                                    let my_height = chain.chain.len() as u64;
                                    if block.header.index > my_height {
                                        println!("⏩ [P2P] Bloc du futur reçu ({}). Demande de mise à jour !", block.header.index);
                                        let my_genesis = chain.chain[0].header.hash.clone();
                                        let port_for_sync = my_port_clone.clone();
                                        tokio::spawn(async move {
                                            send_handshake(&sender_port, &port_for_sync, my_genesis, my_height).await;
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
                                            println!("🧹 [MEMPOOL] Nettoyé suite au bloc d'un pair. TX restantes : {}", mp.len());
                                        }
                                    }
                                },

                                // 🤫 RÉCEPTION D'UN CHUCHOTEMENT DANDELION
                                P2PMessage::WhisperTransaction { tx } => {
                                    let mut rng = rand::thread_rng();
                                    let dice_roll = rng.gen_range(1..=10); 

                                    if dice_roll <= 2 {
                                        println!("🌼 [DANDELION] Explosion du pissenlit ! Diffusion publique.");
                                        let mut pool = mempool_clone.lock().unwrap();
                                        pool.push(tx.clone());
                                    } else {
                                        println!("🤫 [DANDELION] Relais furtif de la TX...");
                                    }
                                },

                                // 📢 RÉCEPTION D'UN CRI PUBLIC DANDELION
                                P2PMessage::BroadcastTransaction { tx } => {
                                    if tx.is_valid() {
                                        let mut pool = mempool_clone.lock().unwrap();
                                        if !pool.iter().any(|t| t.dilithium_signature == tx.dilithium_signature) {
                                            println!("📥 [MEMPOOL] Nouvelle transaction publique ajoutée.");
                                            pool.push(tx.clone());
                                        }
                                    }
                                },
                                
                                // 🌊 RÉCEPTION D'UN ORDRE DEX DU RÉSEAU P2P
                                P2PMessage::BroadcastOrder { order } => {
                                    let mut pool = dex_pool_clone.lock().unwrap();
                                    if !pool.iter().any(|o| o.id == order.id) {
                                        println!("🌊 [P2P DEX] Ordre reçu du réseau : {} {} WATT", order.order_type, order.amount_flames);
                                        pool.push(order.clone());
                                    }
                                },
                            }
                        } else {
                            println!("⚠️ [P2P] Message reçu indéchiffrable (JSON invalide).");
                        }
                    }
                    Err(e) => {
                        println!("🔌 [P2P] Nœud déconnecté ou erreur réseau : {}", e);
                        break;
                    }
                }
            }
        });
    }
}

// --- FONCTIONS RÉSEAU D'ENVOI (MISES À JOUR AVEC FRAMING) ---

pub async fn broadcast_block(target_port: &str, my_port: &str, block: Block) {
    let address = format!("{}:{}", TARGET_IP, target_port);
    if let Ok(socket) = TcpStream::connect(&address).await {
        let mut framed = Framed::new(socket, LengthDelimitedCodec::new());
        let envelope = P2PMessage::NewBlock { block, sender_port: my_port.to_string() };
        let json = serde_json::to_string(&envelope).unwrap();
        let _ = framed.send(Bytes::from(json)).await;
    }
}

pub async fn send_handshake(target_port: &str, my_port: &str, genesis_hash: String, current_height: u64) {
    let address = format!("{}:{}", TARGET_IP, target_port);
    if let Ok(socket) = TcpStream::connect(&address).await {
        let mut framed = Framed::new(socket, LengthDelimitedCodec::new());
        let envelope = P2PMessage::Handshake { genesis_hash, current_height, sender_port: my_port.to_string() };
        let json = serde_json::to_string(&envelope).unwrap();
        let _ = framed.send(Bytes::from(json)).await;
    }
}

pub async fn send_sync_response(target_port: &str, blocks: Vec<Block>) {
    let address = format!("{}:{}", TARGET_IP, target_port);
    if let Ok(socket) = TcpStream::connect(&address).await {
        let mut framed = Framed::new(socket, LengthDelimitedCodec::new());
        let envelope = P2PMessage::SyncResponse { blocks };
        let json = serde_json::to_string(&envelope).unwrap();
        let _ = framed.send(Bytes::from(json)).await;
    }
}

pub async fn broadcast_transaction(target_port: &str, tx: Transaction) {
    let address = format!("{}:{}", TARGET_IP, target_port);
    if let Ok(socket) = TcpStream::connect(&address).await {
        let mut framed = Framed::new(socket, LengthDelimitedCodec::new());
        let envelope = P2PMessage::BroadcastTransaction { tx };
        let json = serde_json::to_string(&envelope).unwrap();
        let _ = framed.send(Bytes::from(json)).await;
    }
}

pub async fn broadcast_order(target_port: &str, order: Order) {
    let address = format!("{}:{}", TARGET_IP, target_port);
    if let Ok(socket) = TcpStream::connect(&address).await {
        let mut framed = Framed::new(socket, LengthDelimitedCodec::new());
        let envelope = P2PMessage::BroadcastOrder { order };
        let json = serde_json::to_string(&envelope).unwrap();
        let _ = framed.send(Bytes::from(json)).await;
    }
}