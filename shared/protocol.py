import json
import struct

def send_message(sock, msg_dict):
    """
    Serializes a dictionary to JSON and sends it over a TCP socket
    prefixed with its length to handle stream boundaries.
    """
    try:
        msg_bytes = json.dumps(msg_dict).encode('utf-8')
        # Pack the length of the message into 4 bytes (unsigned int, big-endian)
        msg_len = struct.pack('!I', len(msg_bytes))
        sock.sendall(msg_len + msg_bytes)
    except Exception as e:
        print(f"Error sending message: {e}")

def receive_message(sock):
    """
    Receives a length-prefixed JSON message from a TCP socket
    and deserializes it back to a dictionary.
    """
    try:
        # First 4 bytes indicate the length of the message
        raw_msg_len = recv_all(sock, 4)
        if not raw_msg_len:
            return None
        msg_len = struct.unpack('!I', raw_msg_len)[0]
        
        # Read the exact length of the message payload
        msg_bytes = recv_all(sock, msg_len)
        if not msg_bytes:
            return None
            
        return json.loads(msg_bytes.decode('utf-8'))
    except Exception as e:
        # Expected when client disconnects or connection resets
        pass
    return None

def recv_all(sock, n):
    """
    Helper function to receive exactly n bytes or return None if EOF is hit.
    """
    data = bytearray()
    while len(data) < n:
        try:
            packet = sock.recv(n - len(data))
            if not packet:
                return None
            data.extend(packet)
        except Exception:
            return None
    return data
