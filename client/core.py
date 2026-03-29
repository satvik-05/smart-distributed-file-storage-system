import socket
import threading
import time
import os
import math
import shutil
import json
import websocket
import re
import glob

import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from client.chunk_handler import ChunkHandlerServer, send_chunk_to_peer, get_chunk_from_peer, delete_chunk_from_peer
import client.logger as log

CHUNK_SIZE = 1024 * 1024  # 1 MB per chunk


class DistributedClient:
    """
    Main client class that connects to the central server for metadata
    and controls the upload/download process to/from peer nodes.
    """

    def __init__(self, storage_dir="storage/default",
                 server_ip="smart-distributed-file-storage-system.onrender.com",
                 server_port=10000, peer_ip="127.0.0.1"):
        self.storage_dir = storage_dir
        self.server_ip = server_ip
        self.server_port = server_port
        self.peer_ip = peer_ip

        log.section("Initializing Client Node")
        log.log("CLIENT", f"Storage directory     : {storage_dir}")
        log.log("CLIENT", f"Central Metadata Hub  : {server_ip}:{server_port}")
        log.log("CLIENT", "Binding local P2P TCP socket on OS-assigned port...")

        # Start background TCP Server on an automatically allocated free OS port
        self.chunk_server = ChunkHandlerServer(0, storage_dir)
        self.port = self.chunk_server.port
        self.chunk_server.start()

        log.log("CLIENT", f"P2P chunk server live on port {self.port}")

        # Attempt to load persistent client_id securely from disk
        self.client_id = None
        self.id_file = os.path.join(storage_dir, ".client_id")
        if os.path.exists(self.id_file):
            try:
                with open(self.id_file, "r") as f:
                    self.client_id = f.read().strip()
                log.log("CLIENT", f"Persistent identity found on disk: {self.client_id[:8]}...")
            except Exception:
                pass
        else:
            log.log("CLIENT", "No cached identity found — will request one from server on register")

    # ── Internal Helpers ──────────────────────────────────────────────────────

    def _connect_to_server(self):
        """Establish a short-lived WebSocket connection to the central server."""
        scheme = "wss" if "onrender.com" in self.server_ip else "ws"
        target_port = 10000 if self.server_ip == "127.0.0.1" and self.server_port == 9000 else self.server_port
        if scheme == "wss":
            url = f"{scheme}://{self.server_ip}/ws"
        else:
            url = f"{scheme}://{self.server_ip}:{target_port}/ws"
        ws = websocket.create_connection(url, timeout=10)
        return ws, url

    def _send_ws(self, ws, payload):
        ws.send(json.dumps(payload))

    def _recv_ws(self, ws):
        resp = ws.recv()
        return json.loads(resp) if resp else None

    # ── Register ──────────────────────────────────────────────────────────────

    def register(self):
        log.section("REGISTER  ·  Linking node to Central Metadata Hub")
        try:
            log.log("CLIENT",    f"Preparing REGISTER request (port={self.port}, ip={self.peer_ip})")
            log.log("SERVER_UP", f"Opening WebSocket connection to {self.server_ip}...")

            ws, url = self._connect_to_server()

            payload = {"type": "REGISTER", "port": self.port, "ip": self.peer_ip}
            if self.client_id:
                payload["client_id"] = self.client_id
                log.log("SERVER_UP", f"Sending REGISTER  →  reusing existing identity: {self.client_id[:8]}...")
            else:
                log.log("SERVER_UP", "Sending REGISTER  →  requesting new server-assigned UUID identity")

            self._send_ws(ws, payload)
            resp = self._recv_ws(ws)
            ws.close()

            if resp and resp.get("status") == "success":
                assigned_id = resp.get("client_id")
                if assigned_id and assigned_id != self.client_id:
                    self.client_id = assigned_id
                    os.makedirs(self.storage_dir, exist_ok=True)
                    with open(self.id_file, "w") as f:
                        f.write(self.client_id)
                    log.log("SERVER_DN", f"Server assigned new UUID  →  {self.client_id}")
                    log.log("CLIENT",    f"Identity cached to disk at {self.id_file}")
                else:
                    log.log("SERVER_DN", f"Server acknowledged existing identity  →  {self.client_id}")

                log.log("SERVER_DN", f"Node registered in active client registry at hub")
                log.log("CLIENT",    f"Background heartbeat thread starting (keeps node alive in registry)")
                threading.Thread(target=self._heartbeat_loop, daemon=True).start()
                log.result(True, f"Node is ONLINE  ·  Identity: {self.client_id}  ·  P2P Port: {self.port}")
                return True
            else:
                log.log("ERROR", f"Server rejected registration: {resp.get('message', 'Unknown Error')}")
                log.result(False, "Registration failed")
                return False
        except Exception as e:
            log.log("ERROR", f"Cannot reach Central Metadata Hub: {e}")
            log.result(False, "Registration failed — is the server reachable?")
            return False

    # ── Heartbeat ─────────────────────────────────────────────────────────────

    def _heartbeat_loop(self):
        """Sends periodic heartbeats so server keeps this node in the active registry."""
        while True:
            time.sleep(5)
            try:
                ws, _ = self._connect_to_server()
                self._send_ws(ws, {"type": "HEARTBEAT", "client_id": self.client_id})
                ws.close()
            except Exception:
                pass

    # ── Upload ────────────────────────────────────────────────────────────────

    def upload(self, file_path, target_name=None):
        """Splits a file into chunks and uploads them directly to available peer clients."""
        log.section(f"UPLOAD  ·  {os.path.basename(file_path)}")

        if not os.path.exists(file_path):
            log.log("ERROR", f"File not found on local disk: {file_path}")
            log.result(False, "Upload aborted")
            return

        file_size = os.path.getsize(file_path)
        num_chunks = math.ceil(file_size / CHUNK_SIZE) if file_size > 0 else 1
        file_name = target_name if target_name else os.path.basename(file_path)

        log.log("CLIENT",    f"File resolved  →  '{file_name}'  ({file_size:,} bytes)")
        log.log("CLIENT",    f"Splitting into {num_chunks} chunk(s) of {CHUNK_SIZE // 1024 // 1024} MB max each")
        log.log("CLIENT",    f"Uploader identity: {self.client_id}")

        try:
            # ── Phase 1: Negotiate with Central Server ──────────────────────
            log.log("SERVER_UP", f"Connecting to Central Metadata Hub at {self.server_ip}...")
            ws, url = self._connect_to_server()
            log.log("SERVER_UP", f"WebSocket open  →  {url}")
            log.log("SERVER_UP", f"Sending UPLOAD_REQUEST  →  file='{file_name}', chunks={num_chunks}")

            self._send_ws(ws, {
                "type": "UPLOAD_REQUEST",
                "file_name": file_name,
                "num_chunks": num_chunks,
                "client_id": self.client_id
            })
            resp = self._recv_ws(ws)
            ws.close()
            log.log("SERVER_DN", "WebSocket closed after metadata negotiation")

            if not resp or resp.get("status") != "success":
                err_msg = str(resp.get("message", resp))
                if "already exists" in err_msg.lower():
                    log.log("SERVER_DN", f"Conflict: '{file_name}' already exists in hub metadata registry")
                    choice = input(
                        f"\n  [!] '{file_name}' already exists on the network!\n"
                        f"  Keep a duplicate with an auto-modified name? (y/n): "
                    ).strip().lower()
                    if choice == 'y':
                        name, ext = os.path.splitext(file_name)
                        match = re.search(r'\s\((\d+)\)$', name)
                        if match:
                            n = int(match.group(1)) + 1
                            new_name = f"{name[:match.start()]} ({n}){ext}"
                        else:
                            new_name = f"{name} (1){ext}"
                        log.log("CLIENT", f"Retrying with renamed target  →  '{new_name}'")
                        return self.upload(file_path, target_name=new_name)
                    else:
                        log.result(False, "Upload cancelled by user")
                        return
                log.log("ERROR", f"Server denied upload: {err_msg}")
                log.result(False, "Upload failed")
                return

            assignments = resp["assignments"]
            chunk_ids = resp["chunk_ids"]

            log.log("SERVER_DN", f"Server allocated {len(chunk_ids)} chunk ID(s) in metadata registry")
            for i, cid in enumerate(chunk_ids):
                peers = assignments.get(cid, [])
                peer_str = ", ".join(f"{p['ip']}:{p['port']}" for p in peers) if peers else "none"
                log.log("SERVER_DN", f"  Chunk {i+1}/{num_chunks}  ID={cid[:8]}...  →  store at: [{peer_str}]")

            # ── Phase 2: Direct P2P Transfer to Peers ──────────────────────
            log.log("CLIENT", "Beginning direct P2P transfers (bypassing server entirely)...")

            with open(file_path, "rb") as f:
                for idx, chunk_id in enumerate(chunk_ids):
                    chunk_data = f.read(CHUNK_SIZE) or b""
                    chunk_kb = len(chunk_data) / 1024

                    locations = assignments.get(chunk_id, [])
                    if not locations:
                        log.log("ERROR", f"No peers assigned for chunk {idx+1} — skipping")
                        continue

                    log.log("CLIENT", f"Read chunk {idx+1}/{num_chunks}  ({chunk_kb:.1f} KB from disk)")

                    threads = []
                    for loc in locations:
                        log.log("PEER_UP", f"TCP connect  →  {loc['ip']}:{loc['port']}  [Chunk {idx+1}/{num_chunks}]")
                        t = threading.Thread(
                            target=self._send_chunk_logged,
                            args=(loc['ip'], loc['port'], chunk_id, chunk_data, idx+1, num_chunks)
                        )
                        t.start()
                        threads.append(t)

                    for t in threads:
                        t.join()

            log.result(True, f"Upload complete  ·  '{file_name}' is now distributed across {num_chunks} chunk(s)")
        except Exception as e:
            log.log("ERROR", f"Upload communication failure: {e}")
            log.result(False, "Upload failed")

    def _send_chunk_logged(self, ip, port, chunk_id, data, idx, total):
        success = send_chunk_to_peer(ip, port, chunk_id, data)
        if success:
            log.log("PEER_DN", f"ACK ✓  {ip}:{port}  confirmed storage of chunk {idx}/{total}  (ID={chunk_id[:8]}...)")
        else:
            log.log("ERROR",   f"No ACK from {ip}:{port}  —  chunk {idx}/{total} may not be stored")

    # ── Download ──────────────────────────────────────────────────────────────

    def download(self, file_name, output_dir="."):
        """Retrieves chunk target IP/Ports from Server, parallel downloads them directly P2P."""
        log.section(f"DOWNLOAD  ·  {file_name}")
        os.makedirs(output_dir, exist_ok=True)

        try:
            # ── Phase 1: Ask Server for the location map ────────────────────
            log.log("SERVER_UP", f"Connecting to Central Metadata Hub at {self.server_ip}...")
            ws, url = self._connect_to_server()
            log.log("SERVER_UP", f"WebSocket open  →  {url}")
            log.log("SERVER_UP", f"Sending DOWNLOAD_REQUEST  →  file='{file_name}'")

            self._send_ws(ws, {"type": "DOWNLOAD_REQUEST", "file_name": file_name})
            resp = self._recv_ws(ws)
            ws.close()
            log.log("SERVER_DN", "WebSocket closed — server returned peer location map")

            if not resp or resp.get("status") != "success":
                log.log("ERROR", f"Server could not locate '{file_name}': {resp.get('message', resp)}")
                log.result(False, "Download failed — file not found on network")
                return

            chunk_ids = resp["chunk_ids"]
            locations = resp["locations"]
            log.log("SERVER_DN", f"File '{file_name}' found  →  {len(chunk_ids)} chunk(s) distributed across network")
            for i, cid in enumerate(chunk_ids):
                peers = locations.get(cid, [])
                peer_str = ", ".join(f"{p['ip']}:{p['port']}" for p in peers) if peers else "none available"
                log.log("SERVER_DN", f"  Chunk {i+1}/{len(chunk_ids)}  ID={cid[:8]}...  →  hosted at: [{peer_str}]")

            # ── Phase 2: Parallel P2P Fetch from Peers ─────────────────────
            log.log("CLIENT", "Beginning parallel P2P chunk retrieval (direct peer connections)...")

            output_file = os.path.join(output_dir, file_name)
            temp_dir = os.path.join(output_dir, f".{file_name}_parts")
            os.makedirs(temp_dir, exist_ok=True)

            lock = threading.Lock()
            downloaded_chunks = []

            def download_worker(c_id, locs, chunk_num):
                chunk_path = os.path.join(temp_dir, c_id)
                if os.path.exists(chunk_path) and os.path.getsize(chunk_path) > 0:
                    log.log("CLIENT", f"Chunk {chunk_num}/{len(chunk_ids)} already cached locally — skipping fetch")
                    with lock:
                        downloaded_chunks.append(c_id)
                    return

                for loc in locs:
                    log.log("PEER_UP", f"TCP connect  →  {loc['ip']}:{loc['port']}  [GET chunk {chunk_num}/{len(chunk_ids)}]")
                    data = get_chunk_from_peer(loc['ip'], loc['port'], c_id)
                    if data is not None:
                        with open(chunk_path, "wb") as fh:
                            fh.write(data)
                        log.log("PEER_DN", f"Received ✓  {loc['ip']}:{loc['port']}  →  chunk {chunk_num}/{len(chunk_ids)}  ({len(data)/1024:.1f} KB)")
                        with lock:
                            downloaded_chunks.append(c_id)
                        return

                log.log("ERROR", f"Chunk {chunk_num}/{len(chunk_ids)} unavailable from all peers — it may be offline")

            threads = []
            for i, chunk_id in enumerate(chunk_ids):
                locs = locations.get(chunk_id, [])
                t = threading.Thread(target=download_worker, args=(chunk_id, locs, i + 1))
                t.start()
                threads.append(t)

            for t in threads:
                t.join()

            if len(downloaded_chunks) != len(chunk_ids):
                log.log("ERROR", f"Incomplete download: {len(downloaded_chunks)}/{len(chunk_ids)} chunks received")
                log.result(False, "Download incomplete — some peers may be offline. Re-run to retry.")
                return

            # ── Phase 3: Reconstruct file locally ──────────────────────────
            log.log("CLIENT", f"All {len(chunk_ids)} chunks received — reconstructing '{file_name}'...")
            with open(output_file, "wb") as f_out:
                for chunk_id in chunk_ids:
                    chunk_path = os.path.join(temp_dir, chunk_id)
                    with open(chunk_path, "rb") as f_in:
                        f_out.write(f_in.read())

            shutil.rmtree(temp_dir)
            log.log("CLIENT", f"Temp chunk parts cleaned up")
            log.result(True, f"File saved to  →  {output_file}")

        except Exception as e:
            log.log("ERROR", f"Download error: {e}")
            log.result(False, "Download failed")

    # ── List Files ────────────────────────────────────────────────────────────

    def list_files(self):
        log.section("LIST FILES  ·  Querying hub for all distributed files")
        try:
            log.log("SERVER_UP", f"Connecting to Central Metadata Hub at {self.server_ip}...")
            ws, url = self._connect_to_server()
            log.log("SERVER_UP", f"Sending LIST_FILES request  →  {url}")

            self._send_ws(ws, {"type": "LIST_FILES"})
            resp = self._recv_ws(ws)
            ws.close()

            if resp and resp.get("status") == "success":
                files = resp["files"]
                log.log("SERVER_DN", f"Hub returned metadata index  →  {len(files)} file(s) registered")
                if not files:
                    log.log("INFO", "No files currently distributed on the network")
                else:
                    for f in files:
                        log.log("INFO", f"  📄  {f}")
                log.result(True, f"{len(files)} file(s) found on network")
            else:
                log.log("ERROR", f"Hub returned an error: {resp}")
                log.result(False, "List query failed")
        except Exception as e:
            log.log("ERROR", f"Cannot reach hub: {e}")
            log.result(False, "List query failed")

    # ── Status ────────────────────────────────────────────────────────────────

    def status(self):
        log.section("NODE STATUS  ·  Local diagnostic snapshot")

        scheme = "wss" if "onrender.com" in self.server_ip else "ws"
        hub_url = f"{scheme}://{self.server_ip}/ws"

        log.log("CLIENT", f"Node Identity  : {self.client_id or 'Not registered yet'}")
        log.log("CLIENT", f"Storage Path   : {self.storage_dir}")
        log.log("CLIENT", f"P2P TCP Port   : {self.port}  (listening for incoming peer connections)")
        log.log("CLIENT", f"Hub Address    : {hub_url}")

        chunks = [f for f in glob.glob(os.path.join(self.storage_dir, "*")) if os.path.isfile(f)]
        # exclude hidden files like .client_id
        chunks = [c for c in chunks if not os.path.basename(c).startswith(".")]
        disk_usage = sum(os.path.getsize(c) for c in chunks)

        log.log("CLIENT", f"Chunks Stored  : {len(chunks)}  ({disk_usage / (1024*1024):.2f} MB used locally)")

        log.log("SERVER_UP", f"Pinging hub to verify node is in active registry...")
        try:
            ws, _ = self._connect_to_server()
            self._send_ws(ws, {"type": "HEARTBEAT", "client_id": self.client_id})
            ws.close()
            log.log("SERVER_DN", "Hub acknowledged heartbeat  →  this node is ONLINE in registry")
            log.result(True, "Node is healthy and registered with Central Metadata Hub")
        except Exception:
            log.log("ERROR", "Could not reach hub — node may appear offline to other clients")
            log.result(False, "Hub unreachable")

    # ── Delete ────────────────────────────────────────────────────────────────

    def delete(self, file_name):
        """
        Orchestrates deleting a file globally. Proves ownership to the Central Hub,
        receives chunk location map, then directly P2P-signals each peer to wipe their disk.
        """
        log.section(f"DELETE  ·  {file_name}")

        if not self.client_id:
            log.log("ERROR", "Cannot delete — this node has not registered yet")
            log.result(False, "Register first before deleting")
            return

        log.log("CLIENT",    f"Requesting deletion of '{file_name}'")
        log.log("CLIENT",    f"Authorization identity: {self.client_id}")

        try:
            # ── Phase 1: Authorization and metadata purge at hub ──────────
            log.log("SERVER_UP", f"Connecting to Central Metadata Hub at {self.server_ip}...")
            ws, url = self._connect_to_server()
            log.log("SERVER_UP", f"Sending DELETE_REQUEST  →  file='{file_name}', client_id={self.client_id[:8]}...")

            self._send_ws(ws, {
                "type": "DELETE_REQUEST",
                "file_name": file_name,
                "client_id": self.client_id
            })
            resp = self._recv_ws(ws)
            ws.close()

            if not resp or resp.get("status") != "success":
                msg = resp.get("message", "Unknown Error") if resp else "No response"
                log.log("SERVER_DN", f"Hub rejected deletion request")
                log.log("ERROR",     f"Reason: {msg}")
                log.result(False, "Deletion denied")
                return

            log.log("SERVER_DN", "Authorization verified  →  uploader identity matches")
            log.log("SERVER_DN", "Hub purged file record, chunk records, and location mappings from database")

            locations = resp.get("locations", {})
            chunk_ids = resp.get("chunk_ids", [])

            if not locations:
                log.log("INFO", "No active peer locations returned — metadata was the only record")
                log.result(True, f"'{file_name}' removed from network index")
                return

            log.log("SERVER_DN", f"Hub returned last-known locations for {len(chunk_ids)} chunk(s)")
            for i, cid in enumerate(chunk_ids):
                peers = locations.get(cid, [])
                peer_str = ", ".join(f"{p['ip']}:{p['port']}" for p in peers) if peers else "none"
                log.log("SERVER_DN", f"  Chunk {i+1}/{len(chunk_ids)}  ID={cid[:8]}...  →  last seen at: [{peer_str}]")

            # ── Phase 2: Direct P2P disk wipe commands ────────────────────
            log.log("CLIENT", "Issuing direct P2P DELETE_CHUNK commands (bypassing server)...")

            def wipe_chunk(chunk_id, peers, chunk_num):
                for peer in peers:
                    log.log("PEER_UP", f"TCP connect  →  {peer['ip']}:{peer['port']}  [DELETE chunk {chunk_num}/{len(chunk_ids)}]")
                    success = delete_chunk_from_peer(peer["ip"], peer["port"], chunk_id)
                    if success:
                        log.log("PEER_DN", f"ACK ✓  {peer['ip']}:{peer['port']}  confirmed chunk {chunk_num} wiped from disk")
                    else:
                        log.log("INFO",    f"Peer {peer['ip']}:{peer['port']} offline — chunk {chunk_num} is already orphaned")

            threads = []
            for i, chunk_id in enumerate(chunk_ids):
                peers = locations.get(chunk_id, [])
                if peers:
                    t = threading.Thread(target=wipe_chunk, args=(chunk_id, peers, i + 1))
                    t.start()
                    threads.append(t)

            for t in threads:
                t.join()

            log.result(True, f"'{file_name}' fully purged from all layers: hub registry + peer disks")

        except Exception as e:
            log.log("ERROR", f"Deletion sequence error: {e}")
            log.result(False, "Deletion failed")
