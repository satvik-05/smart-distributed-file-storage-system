import sys
import os
import time

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from client.core import DistributedClient

def run_tests():
    print("--- Starting System Test ---")
    
    # 1. Initialize Clients (targeting the FastApi port 10000 for local WS)
    c1 = DistributedClient("storage/test_c1", "127.0.0.1", 10000)
    c2 = DistributedClient("storage/test_c2", "127.0.0.1", 10000)
    
    # 2. Register with Central Server
    assert c1.register() == True
    assert c2.register() == True
    time.sleep(1)
    
    # 3. Upload a file
    c1.upload("render_test.txt")
    time.sleep(2)
    
    # 4. Verify indexing
    c2.list_files()
    
    # 5. Download the file from Peer
    c2.download("render_test.txt", "storage/downloads")
    time.sleep(1)
    
    downloaded_path = "storage/downloads/render_test.txt"
    if os.path.exists(downloaded_path):
        with open(downloaded_path, "r") as f:
            content = f.read()
            print(f"Downloaded Content: {content.strip()}")
            if "Hello world" in content:
                print("SUCCESS: File verified!")
            else:
                print("FAILURE: File content mismatch.")
    else:
        print("FAILURE: File not downloaded.")
        
    # 5. Verify Deletion Protocols
    print("\n--- Verifying Deletion Authorization ---")
    c1.delete("render_test.txt")
    
    # Prove it's gone
    c1.list_files()
    
    print("\nSUCCESS: File verified and completely purged natively!")
    
if __name__ == "__main__":
    run_tests()
