import threading
import time
import os
import sys
import json
import base64
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import uvicorn

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from server.client_manager import ClientManager
from server.metadata_manager import MetadataManager

client_manager = ClientManager()
metadata_manager = MetadataManager()

app = FastAPI()
RELAY_STORAGE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "storage", "relay")
os.makedirs(RELAY_STORAGE_DIR, exist_ok=True)


def _relay_chunk_path(chunk_id: str) -> str:
    safe_chunk_id = os.path.basename(chunk_id)
    return os.path.join(RELAY_STORAGE_DIR, safe_chunk_id)

@app.get("/health")
def health_check():
    return {
        "status": "ok", 
        "clients_connected": len(client_manager.get_all_clients()),
        "files_indexed": len(metadata_manager.list_files())
    }


@app.post("/relay/store")
def relay_store(payload: dict):
    chunk_id = payload.get("chunk_id")
    data = payload.get("data")
    if not chunk_id or data is None:
        return {"status": "error", "message": "Missing chunk_id or data"}

    try:
        raw = base64.b64decode(data)
        with open(_relay_chunk_path(chunk_id), "wb") as f:
            f.write(raw)
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.get("/relay/get/{chunk_id}")
def relay_get(chunk_id: str):
    path = _relay_chunk_path(chunk_id)
    if not os.path.exists(path):
        return {"status": "error", "message": "Chunk not found"}

    try:
        with open(path, "rb") as f:
            encoded = base64.b64encode(f.read()).decode("utf-8")
        return {"status": "success", "data": encoded}
    except Exception as e:
        return {"status": "error", "message": str(e)}


@app.delete("/relay/delete/{chunk_id}")
def relay_delete(chunk_id: str):
    path = _relay_chunk_path(chunk_id)
    try:
        if os.path.exists(path):
            os.remove(path)
        return {"status": "success"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    Handles incoming WebSocket connections from clients.
    Replaces the previous background raw TCP thread to allow Render compatibility.
    """
    await websocket.accept()
    client_host = websocket.client.host
    print(f"[Server] WebSocket client connected from {client_host}")
    
    try:
        while True:
            # Await raw JSON dictionary directly from FastAPI WebSocket
            msg = await websocket.receive_json()
            msg_type = msg.get("type")
            
            if msg_type == "REGISTER":
                import uuid
                client_id = msg.get("client_id")
                if not client_id:
                    client_id = str(uuid.uuid4())
                    
                client_ip = msg.get("ip") or client_host
                client_manager.register_client(client_id, client_ip, msg["port"])
                await websocket.send_json({"status": "success", "client_id": client_id})
                
            elif msg_type == "HEARTBEAT":
                client_manager.update_heartbeat(msg["client_id"])
                
            elif msg_type == "UPLOAD_REQUEST":
                available_clients = client_manager.get_all_clients()
                success, data = metadata_manager.prepare_upload(msg["file_name"], msg["num_chunks"], available_clients, msg.get("client_id"))
                
                if success:
                    assignments = {}
                    for chunk_id, assigned_cids in data["assignments"].items():
                        assignments[chunk_id] = [client_manager.get_client_info(cid) for cid in assigned_cids if client_manager.get_client_info(cid) is not None]
                    
                    await websocket.send_json({
                        "status": "success", 
                        "chunk_ids": data["chunk_ids"], 
                        "assignments": assignments
                    })
                else:
                    await websocket.send_json({"status": "error", "message": data})
                    
            elif msg_type == "DELETE_REQUEST":
                success, message, locations = metadata_manager.delete_file(msg["file_name"], msg.get("client_id"))
                if success and locations:
                    resolved_locations = {}
                    for chunk_id, cids in locations["locations"].items():
                        resolved_locations[chunk_id] = [client_manager.get_client_info(cid) for cid in cids if client_manager.get_client_info(cid) is not None]
                    
                    await websocket.send_json({
                        "status": "success",
                        "message": message,
                        "chunk_ids": locations["chunk_ids"],
                        "locations": resolved_locations
                    })
                else:
                    await websocket.send_json({"status": "error", "message": message})
                    
            elif msg_type == "DOWNLOAD_REQUEST":
                locations_info = metadata_manager.get_file_locations(msg["file_name"])
                if locations_info:
                    resolved_locations = {}
                    for chunk_id, cids in locations_info["locations"].items():
                        resolved_locations[chunk_id] = [client_manager.get_client_info(cid) for cid in cids if client_manager.get_client_info(cid) is not None]
                    
                    await websocket.send_json({
                        "status": "success", 
                        "chunk_ids": locations_info["chunk_ids"], 
                        "locations": resolved_locations
                    })
                else:
                    await websocket.send_json({"status": "error", "message": "File not found."})
                    
            elif msg_type == "LIST_FILES":
                files = metadata_manager.list_files()
                await websocket.send_json({"status": "success", "files": files})

    except WebSocketDisconnect:
        print(f"[Server] WebSocket client {client_host} disconnected.")
    except Exception as e:
        print(f"[Server] WebSocket Error handling connection from {client_host}: {e}")

def monitor_dead_clients():
    """Periodically checks for clients that missed heartbeats."""
    while True:
        dead = client_manager.remove_dead_clients()
        for d in dead:
            metadata_manager.handle_dead_client(d)
        time.sleep(5)

def run_server():
    # Render maps external HTTP/WebSocket ports 80/443 directly to this single PORT
    http_port = int(os.environ.get("PORT", 10000))
    
    # Start heartbeat monitor in background
    threading.Thread(target=monitor_dead_clients, daemon=True).start()
    
    print(f"[Server] Integrated WebSocket & HTTP Server starting on 0.0.0.0:{http_port}")
    uvicorn.run(app, host="0.0.0.0", port=http_port)

if __name__ == "__main__":
    run_server()
