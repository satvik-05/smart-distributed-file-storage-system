import socket
import threading
import os
import base64
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from shared.protocol import receive_message, send_message

class ChunkHandlerServer:
    """
    A lightweight TCP server running on each client to handle
    receiving new chunks and serving chunks back to other clients.
    """
    def __init__(self, port, storage_dir):
        self.port = port
        self.storage_dir = storage_dir
        os.makedirs(storage_dir, exist_ok=True)
        
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind(('0.0.0.0', port))
        
        # If port is 0, OS allocates a random free port. Get the actually assigned port!
        self.port = self.server_socket.getsockname()[1]
        
        self.server_socket.listen(10)

    def start(self):
        threading.Thread(target=self._run_server, daemon=True).start()
        print(f"[Chunk Server] Listening on port {self.port}, storing to {self.storage_dir}")

    def _run_server(self):
        while True:
            try:
                conn, addr = self.server_socket.accept()
                threading.Thread(target=self._handle_conn, args=(conn, addr), daemon=True).start()
            except Exception as e:
                print(f"[Chunk Server] Accept error: {e}")

    def _handle_conn(self, conn, addr):
        try:
            msg = receive_message(conn)
            if not msg:
                return
            
            msg_type = msg.get("type")
            
            if msg_type == "STORE_CHUNK":
                chunk_id = msg["chunk_id"]
                data = base64.b64decode(msg["data"])
                
                # Save chunk payload to disk
                file_path = os.path.join(self.storage_dir, chunk_id)
                with open(file_path, "wb") as f:
                    f.write(data)
                    
                send_message(conn, {"status": "success"})
                
            elif msg_type == "GET_CHUNK":
                chunk_id = msg["chunk_id"]
                file_path = os.path.join(self.storage_dir, chunk_id)
                
                if os.path.exists(file_path):
                    with open(file_path, "rb") as f:
                        data = f.read()
                    encoded_data = base64.b64encode(data).decode('utf-8')
                    send_message(conn, {"status": "success", "data": encoded_data})
                else:
                    send_message(conn, {"status": "error", "message": "Chunk not found"})
                    
            elif msg_type == "DELETE_CHUNK":
                chunk_id = msg["chunk_id"]
                file_path = os.path.join(self.storage_dir, chunk_id)
                if os.path.exists(file_path):
                    try:
                        os.remove(file_path)
                    except OSError:
                        pass
                send_message(conn, {"status": "success", "message": "Chunk securely wiped."})
                    
        except Exception as e:
            print(f"[Chunk Server] Error handling Peer connection: {e}")
        finally:
            conn.close()
            

def send_chunk_to_peer(ip, port, chunk_id, data):
    """
    Helper function to send a file chunk over the network directly to a Peer.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        sock.connect((ip, port))
        
        encoded_data = base64.b64encode(data).decode('utf-8')
        send_message(sock, {
            "type": "STORE_CHUNK",
            "chunk_id": chunk_id,
            "data": encoded_data
        })
        
        resp = receive_message(sock)
        sock.close()
        return resp and resp.get("status") == "success"
    except Exception as e:
        print(f"Error sending chunk {chunk_id} to {ip}:{port} - {e}")
        return False

def get_chunk_from_peer(ip, port, chunk_id):
    """
    Helper function to pull a file chunk directly from a Peer.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        sock.connect((ip, port))
        
        send_message(sock, {
            "type": "GET_CHUNK",
            "chunk_id": chunk_id
        })
        
        resp = receive_message(sock)
        sock.close()
        
        if resp and resp.get("status") == "success":
            return base64.b64decode(resp["data"])
    except Exception as e:
        pass # Handle gracefully in the download threads
    return None

def delete_chunk_from_peer(ip, port, chunk_id):
    """
    Helper function to command a remote peer to permanently delete a physical chunk from its storage directory.
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        sock.connect((ip, port))
        
        send_message(sock, {
            "type": "DELETE_CHUNK",
            "chunk_id": chunk_id
        })
        
        resp = receive_message(sock)
        sock.close()
        
        if resp and resp.get("status") == "success":
            return True
        return False
    except Exception as e:
        return False
