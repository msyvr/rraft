use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::thread;
use rand::{self, Rng};
use tokio::sync::mpsc::{self, Sender, Receiver};
use tokio::time::sleep;

// Message types for Raft communication
#[derive(Debug, Clone)]
enum Message {
    // Leader election messages
    RequestVote {
        term: u64,
        candidate_id: String,
        last_log_index: u64,
        last_log_term: u64,
    },
    RequestVoteResponse {
        term: u64,
        vote_granted: bool,
    },
    
    // Log replication messages
    AppendEntries {
        term: u64,
        leader_id: String,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    AppendEntriesResponse {
        term: u64,
        success: bool,
        match_index: u64,
    },
    
    // Client request message
    ClientRequest {
        command: String,
        client_id: String,
    },
    ClientResponse {
        success: bool,
        result: String,
    },
}

// Log entry structure
#[derive(Debug, Clone)]
struct LogEntry {
    term: u64,
    command: String,
    index: u64,
}

// Server states
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum ServerState {
    Follower,
    Candidate,
    Leader,
}

// Raft server structure
struct RaftServer {
    // Server identity
    id: String,
    state: ServerState,
    
    // Persistent state
    current_term: u64,
    voted_for: Option<String>,
    log: Vec<LogEntry>,
    
    // Volatile state
    commit_index: u64,
    last_applied: u64,
    
    // Leader state
    next_index: HashMap<String, u64>,
    match_index: HashMap<String, u64>,
    
    // Cluster configuration
    peers: HashSet<String>,
    
    // Communication
    sender: HashMap<String, Sender<Message>>,
    receiver: Receiver<Message>,
    
    // Timers
    last_heartbeat: Instant,
    election_timeout: Duration,
    
    // State machine
    state_machine: HashMap<String, String>,
}

impl RaftServer {
    // Create a new Raft server
    fn new(id: String, peers: HashSet<String>) -> (Self, Sender<Message>) {
        let (tx, rx) = mpsc::channel(100);
        
        // Generate random election timeout between 150-300ms
        let mut rng = rand::thread_rng();
        let timeout_ms = rng.gen_range(150..300);
        
        (RaftServer {
            id,
            state: ServerState::Follower,
            current_term: 0,
            voted_for: None,
            log: vec![LogEntry { term: 0, command: String::new(), index: 0 }], // Sentinel entry
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            peers,
            sender: HashMap::new(),
            receiver: rx,
            last_heartbeat: Instant::now(),
            election_timeout: Duration::from_millis(timeout_ms),
            state_machine: HashMap::new(),
        }, tx)
    }
    
    // Main server loop
    async fn run(&mut self) {
        loop {
            // Check for timeouts based on server state
            match self.state {
                ServerState::Follower | ServerState::Candidate => {
                    if Instant::now().duration_since(self.last_heartbeat) > self.election_timeout {
                        self.start_election().await;
                    }
                },
                ServerState::Leader => {
                    // Send heartbeats periodically (every 50ms)
                    if Instant::now().duration_since(self.last_heartbeat) > Duration::from_millis(50) {
                        self.send_heartbeats().await;
                        self.last_heartbeat = Instant::now();
                    }
                }
            }
            
            // Process incoming messages
            if let Ok(message) = self.receiver.try_recv() {
                self.handle_message(message).await;
            }
            
            // Apply committed entries to state machine
            self.apply_committed_entries().await;
            
            // Small sleep to avoid CPU spinning
            sleep(Duration::from_millis(10)).await;
        }
    }
    
    // Start an election
    async fn start_election(&mut self) {
        self.state = ServerState::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id.clone());
        self.last_heartbeat = Instant::now();
        
        let mut votes_received = 1; // Vote for self
        
        // Send RequestVote to all peers
        for peer in &self.peers {
            let last_log_index = self.log.len() as u64 - 1;
            let last_log_term = self.log[last_log_index as usize].term;
            
            let vote_request = Message::RequestVote {
                term: self.current_term,
                candidate_id: self.id.clone(),
                last_log_index,
                last_log_term,
            };
            
            if let Some(sender) = self.sender.get(peer) {
                // Ignore send errors - peer might be down
                let _ = sender.send(vote_request).await;
            }
        }
        
        // Wait for votes with a timeout
        let election_end = Instant::now() + self.election_timeout;
        
        while Instant::now() < election_end && self.state == ServerState::Candidate {
            if let Ok(message) = self.receiver.try_recv() {
                match message {
                    Message::RequestVoteResponse { term, vote_granted } => {
                        // Step down if we discover a higher term
                        if term > self.current_term {
                            self.current_term = term;
                            self.state = ServerState::Follower;
                            self.voted_for = None;
                            break;
                        }
                        
                        // Count votes
                        if vote_granted && term == self.current_term {
                            votes_received += 1;
                            
                            // Check if we have majority
                            if votes_received > (self.peers.len() + 1) / 2 {
                                self.become_leader().await;
                                break;
                            }
                        }
                    },
                    // Handle other messages
                    _ => {
                        self.handle_message(message).await;
                    }
                }
            }
            
            sleep(Duration::from_millis(10)).await;
        }
    }
    
    // Transition to leader state
    async fn become_leader(&mut self) {
        if self.state != ServerState::Candidate {
            return;
        }
        
        self.state = ServerState::Leader;
        
        // Initialize leader state
        let last_log_index = self.log.len() as u64 - 1;
        
        for peer in &self.peers {
            self.next_index.insert(peer.clone(), last_log_index + 1);
            self.match_index.insert(peer.clone(), 0);
        }
        
        // Send initial empty AppendEntries RPCs (heartbeats)
        self.send_heartbeats().await;
    }
    
    // Send heartbeats to all peers
    async fn send_heartbeats(&mut self) {
        for peer in &self.peers {
            self.send_append_entries(peer).await;
        }
        self.last_heartbeat = Instant::now();
    }
    
    // Send AppendEntries RPC to a specific peer
    async fn send_append_entries(&mut self, peer: &str) {
        if let Some(next_idx) = self.next_index.get(peer).cloned() {
            let prev_log_index = next_idx - 1;
            let prev_log_term = if prev_log_index > 0 && prev_log_index < self.log.len() as u64 {
                self.log[prev_log_index as usize].term
            } else {
                0
            };
            
            // Get entries to send (may be empty for heartbeat)
            let entries = if next_idx < self.log.len() as u64 {
                self.log[next_idx as usize..].to_vec()
            } else {
                Vec::new()
            };
            
            let append_entries = Message::AppendEntries {
                term: self.current_term,
                leader_id: self.id.clone(),
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            };
            
            if let Some(sender) = self.sender.get(peer) {
                let _ = sender.send(append_entries).await;
            }
        }
    }
    
    // Handle incoming messages
    async fn handle_message(&mut self, message: Message) {
        match message {
            Message::RequestVote { term, candidate_id, last_log_index, last_log_term } => {
                let mut vote_granted = false;
                
                // Step down if we discover a higher term
                if term > self.current_term {
                    self.current_term = term;
                    self.state = ServerState::Follower;
                    self.voted_for = None;
                }
                
                // Check if our log is at least as up-to-date as the candidate's
                let our_last_idx = self.log.len() as u64 - 1;
                let our_last_term = self.log[our_last_idx as usize].term;
                let log_is_ok = last_log_term > our_last_term || 
                               (last_log_term == our_last_term && last_log_index >= our_last_idx);
                
                // Grant vote if: same or higher term, haven't voted for anyone else, and log is ok
                if term >= self.current_term && 
                   (self.voted_for.is_none() || self.voted_for.as_ref() == Some(&candidate_id)) &&
                   log_is_ok {
                    self.voted_for = Some(candidate_id.clone());
                    vote_granted = true;
                    self.last_heartbeat = Instant::now(); // Reset election timeout
                }
                
                // Send response
                let response = Message::RequestVoteResponse {
                    term: self.current_term,
                    vote_granted,
                };
                
                if let Some(sender) = self.sender.get(&candidate_id) {
                    let _ = sender.send(response).await;
                }
            },
            
            Message::AppendEntries { term, leader_id, prev_log_index, prev_log_term, entries, leader_commit } => {
                let mut success = false;
                let mut match_index = 0;
                
                // Step down if we discover a higher term
                if term > self.current_term {
                    self.current_term = term;
                    self.state = ServerState::Follower;
                    self.voted_for = None;
                }
                
                // Process the append entries request
                if term >= self.current_term {
                    // Accept leader
                    self.state = ServerState::Follower;
                    self.last_heartbeat = Instant::now();
                    
                    // Check if log contains entry at prev_log_index with term prev_log_term
                    let log_ok = prev_log_index == 0 || 
                                (prev_log_index < self.log.len() as u64 && 
                                 self.log[prev_log_index as usize].term == prev_log_term);
                    
                    if log_ok {
                        success = true;
                        
                        // Find conflicting entries
                        let mut new_entries = entries.clone();
                        let mut idx = prev_log_index + 1;
                        let mut i = 0;
                        
                        while i < new_entries.len() && idx < self.log.len() as u64 {
                            if self.log[idx as usize].term != new_entries[i].term {
                                // Delete conflicting entries and everything after
                                self.log.truncate(idx as usize);
                                break;
                            }
                            i += 1;
                            idx += 1;
                        }
                        
                        // Append any remaining new entries
                        if i < new_entries.len() {
                            self.log.extend_from_slice(&new_entries[i..]);
                        }
                        
                        match_index = prev_log_index + new_entries.len() as u64;
                        
                        // Update commit index if leader has higher commit index
                        if leader_commit > self.commit_index {
                            self.commit_index = std::cmp::min(leader_commit, self.log.len() as u64 - 1);
                        }
                    }
                }
                
                // Send response
                let response = Message::AppendEntriesResponse {
                    term: self.current_term,
                    success,
                    match_index,
                };
                
                if let Some(sender) = self.sender.get(&leader_id) {
                    let _ = sender.send(response).await;
                }
            },
            
            Message::AppendEntriesResponse { term, success, match_index } => {
                // Step down if we discover a higher term
                if term > self.current_term {
                    self.current_term = term;
                    self.state = ServerState::Follower;
                    self.voted_for = None;
                    return;
                }
                
                // Only process responses if we're still the leader
                if self.state != ServerState::Leader || term != self.current_term {
                    return;
                }
                
                // Update follower's progress
                if success {
                    // Find which peer this is from (inefficient, but for simplicity)
                    let peer_id = self.find_peer_by_match_index(match_index).unwrap_or_default();
                    
                    if !peer_id.is_empty() {
                        if let Some(next_idx) = self.next_index.get_mut(&peer_id) {
                            *next_idx = match_index + 1;
                        }
                        
                        if let Some(match_idx) = self.match_index.get_mut(&peer_id) {
                            *match_idx = match_index;
                        }
                        
                        // Try to advance commit index
                        self.update_commit_index();
                    }
                } else {
                    // If AppendEntries failed, decrement nextIndex and retry
                    let peer_id = self.find_peer_by_match_index(match_index).unwrap_or_default();
                    
                    if !peer_id.is_empty() {
                        if let Some(next_idx) = self.next_index.get_mut(&peer_id) {
                            if *next_idx > 1 {
                                *next_idx -= 1;
                            }
                        }
                        
                        // Retry immediately
                        self.send_append_entries(&peer_id).await;
                    }
                }
            },
            
            Message::ClientRequest { command, client_id } => {
                if self.state != ServerState::Leader {
                    // Redirect to leader if known
                    // In a real implementation, we would return the leader's ID
                    if let Some(sender) = self.sender.get(&client_id) {
                        let _ = sender.send(Message::ClientResponse {
                            success: false,
                            result: "Not the leader".to_string(),
                        }).await;
                    }
                } else {
                    // Append entry to log
                    let entry = LogEntry {
                        term: self.current_term,
                        command: command.clone(),
                        index: self.log.len() as u64,
                    };
                    
                    self.log.push(entry);
                    
                    // Try to replicate immediately
                    self.send_heartbeats().await;
                    
                    // In a real implementation, we would wait for commit and then respond
                    // For now, just acknowledge receipt
                    if let Some(sender) = self.sender.get(&client_id) {
                        let _ = sender.send(Message::ClientResponse {
                            success: true,
                            result: "Command received".to_string(),
                        }).await;
                    }
                }
            },
            
            _ => {} // Ignore other messages
        }
    }
    
    // Apply committed log entries to the state machine
    async fn apply_committed_entries(&mut self) {
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            let entry = &self.log[self.last_applied as usize];
            
            // Apply command to state machine
            // This is a simplistic key-value store implementation
            if !entry.command.is_empty() {
                let parts: Vec<&str> = entry.command.splitn(3, " ").collect();
                
                match parts[0] {
                    "SET" if parts.len() >= 3 => {
                        self.state_machine.insert(parts[1].to_string(), parts[2].to_string());
                    },
                    "DEL" if parts.len() >= 2 => {
                        self.state_machine.remove(parts[1]);
                    },
                    _ => {} // Ignore invalid commands
                }
            }
        }
    }
    
    // Update commit index for leader (leader-only)
    fn update_commit_index(&mut self) {
        if self.state != ServerState::Leader {
            return;
        }
        
        // For each index N, count replicas including self
        for n in (self.commit_index + 1)..self.log.len() as u64 {
            let mut count = 1; // Leader itself
            
            for (_, &match_idx) in &self.match_index {
                if match_idx >= n {
                    count += 1;
                }
            }
            
            // If majority and entry from current term, commit
            if count > (self.peers.len() + 1) / 2 && self.log[n as usize].term == self.current_term {
                self.commit_index = n;
            } else {
                // No more entries can be committed
                break;
            }
        }
    }
    
    // Helper method to find a peer by match_index
    fn find_peer_by_match_index(&self, match_index: u64) -> Option<String> {
        for (peer, &idx) in &self.match_index {
            if idx == match_index {
                return Some(peer.clone());
            }
        }
        None
    }
}

// Example of creating a cluster
#[tokio::main]
async fn main() {
    // Create a cluster of 3 nodes
    let node_ids = vec!["node1".to_string(), "node2".to_string(), "node3".to_string()];
    let mut handles = Vec::new();
    let mut senders = HashMap::new();
    
    // Create each node
    for id in &node_ids {
        let mut peers = HashSet::new();
        for peer_id in &node_ids {
            if peer_id != id {
                peers.insert(peer_id.clone());
            }
        }
        
        let (mut server, tx) = RaftServer::new(id.clone(), peers);
        senders.insert(id.clone(), tx);
        
        let id_clone = id.clone();
        handles.push(tokio::spawn(async move {
            println!("Starting node {}", id_clone);
            server.run().await;
        }));
    }
    
    // Connect the nodes
    for id in &node_ids {
        let node_senders = senders.clone();
        server.sender = node_senders;
    }
    
    // Wait for all nodes
    for handle in handles {
        handle.await.unwrap();
    }
}
