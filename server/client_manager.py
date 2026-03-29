import time
import threading

class ClientManager:
    """
    Manages active clients connected to the server.
    Tracks their IP, port, and last heartbeat time to purge dead clients.
    """
    def __init__(self):
        # Dictionary mapping client_id to their info
        self.clients = {}  
        self.lock = threading.Lock()

    def register_client(self, client_id, ip, port):
        with self.lock:
            self.clients[client_id] = {
                'ip': ip,
                'port': port,
                'last_seen': time.time()
            }
            print(f"[ClientManager] Client registered: {client_id} at {ip}:{port}")

    def update_heartbeat(self, client_id):
        with self.lock:
            if client_id in self.clients:
                self.clients[client_id]['last_seen'] = time.time()

    def remove_dead_clients(self, timeout=15):
        """
        Removes clients that haven't sent a heartbeat within the timeout period.
        """
        with self.lock:
            current_time = time.time()
            dead_clients = []
            for client_id, info in self.clients.items():
                if current_time - info['last_seen'] > timeout:
                    dead_clients.append(client_id)
            
            for client_id in dead_clients:
                print(f"[ClientManager] Removing dead client (timeout): {client_id}")
                del self.clients[client_id]
            
            return dead_clients

    def get_all_clients(self):
        """
        Returns a list of all currently active client IDs.
        """
        with self.lock:
            return list(self.clients.keys())

    def get_client_info(self, client_id):
        """
        Returns connection information for a specific client.
        """
        with self.lock:
            return self.clients.get(client_id)
