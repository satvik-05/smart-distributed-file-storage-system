# Distributed File Storage System

A robust, decentralized Python-based file storage system utilizing a **central metadata server** and **peer-to-peer storage clients**. Large files are actively split into 1MB chunks, aggressively replicated, and later downloaded from various disparate nodes in parallel.

---

## 🏗️ System Architecture

- **The Server**: Function as the "brain". Actively tracking client liveliness (via periodic heartbeats) while securely associating file chunks to available online Peer-To-Peer nodes. 
    * To ensure robust compatibility across Web Hosting platforms like Render, the metadata server communicates with clients globally utilizing pure **WebSockets** seamlessly routed over HTTP endpoints.
- **The Clients**: Terminal interface users registering to the main Central Server via WebSockets. Responsible for breaking big files into fragments and using active TCP connection sockets to exchange chunk data dynamically peer-to-peer!

---

## 🛠️ Setup Guide

### 1. Requirements

Verify you have **Python 3** properly installed, then run:

```bash
pip install -r requirements.txt
```

### 2. Booting the Global Server Locally
Initialize the metadata hub first.
```bash
python server/main.py
```
> **Default Configuration**: The WS/HTTP Server binds and routes universally on `10000` via the `PORT` environment variable.

### 3. Deploying Network Clients 
Initiating storage clients. Use different terminal segments. Avoid overlapping storage directories to mimic varied environments.

**Terminal 1:**
```bash
python client/cli.py --storage storage/node1
```
**Terminal 2:**
```bash
python client/cli.py --storage storage/node2
```
**Terminal 3:**
```bash
python client/cli.py --storage storage/node3
```

### 4. Client Usage Commands
Inside a running client terminal instance you must execute setup sequences:

1. `register` -> Link this terminal interface with the Central Server node.
2. `upload <file_path.pdf>` -> Begin data shredding across Peer-To-Peer connections.
3. `download <filename.pdf>` -> Construct file locally from all distributed network targets! *Note: Can automatically resume interrupted downloads.*
4. `list_files` -> Gather public indexing of accessible network files.
5. `status` -> Run diagnostic verifications (Check Port, Allocated Network Disk space, Node Cache). 

---

## 🚀 Render Deployment Documentation

This repository naturally accommodates immediate deployment capabilities to the **Render** web hosting dashboard.

1. Connect this repository to a new `Web Service` module.
2. **Build Command**: `pip install -r requirements.txt`
3. **Start Command**: `python server/main.py`
4. Use standard Render routing! You just need your generated URL domain:
   - Just run `python client/cli.py`. The node automatically defaults to `smart-distributed-file-storage-system.onrender.com`, securely fetching its unique persistent ID and mapping an OS-allocated native Port behind the scenes!

## Advanced Systems Covered
- **Fault-Tolerant Chunk Replication**: Redundancy implementation ensures missing Nodes aren't problematic.
- **Stale Component Deactivation**: Integrated heartbeat pings rapidly scrub severed Node listings routing chunk fetching smoothly securely to the next backup Node.
- **Concurrent Polling**: Native python Threading optimizes peer chunk streaming actively during bulk downloading procedures.
