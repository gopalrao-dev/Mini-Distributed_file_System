# datanode/server.py

import socket
import sys
import os
from common.config import MASTER_HOST, MASTER_PORT, DATANODE_PORTS

def register_with_master(node_id):
    s = socket.socket()
    s.connect((MASTER_HOST, MASTER_PORT))
    s.send(f"DATANODE {node_id}".encode())
    msg = s.recv(1024).decode()
    print(f"[DATANODE {node_id}] Master says: {msg}")
    s.close()

def start_datanode(node_id):
    register_with_master(node_id)

    s = socket.socket()
    index = ord(node_id) - ord('A')
    port = DATANODE_PORTS[index]
    s.bind(('localhost', port))
    s.listen()
    print(f"[DATANODE {node_id}] Listening on port {port}")
    os.makedirs(f"storage_{node_id}", exist_ok=True)

    while True:
        conn, addr = s.accept()
        msg = conn.recv(1024).decode()
        
        if msg.startswith("STORE"):
            _, chunk_name, content = msg.split(" ", 2)
            with open(f"storage_{node_id}/{chunk_name}", "w") as f:
                f.write(content)
            print(f"[DATANODE {node_id}] Stored {chunk_name}")
            conn.send(b"STORED")
        
        elif msg.startswith("RETRIEVE"):
            # parse the chunk name
            _, chunk_name = msg.split()
            storage_path = f"storage_{node_id}/{chunk_name}"
            try:
                with open(storage_path, "r") as f:
                    data = f.read()
                conn.send(data.encode())
                print(f"[DATANODE {node_id}] Served {chunk_name}")
            except FileNotFoundError:
                conn.send(b"ERROR: chunk not found")
            
        # ─── DELETE CHUNK ──────────────────────────────────
        elif msg.startswith("DELETE"):
            _, chunk_name = msg.split()
            path = f"storage_{node_id}/{chunk_name}"
            try:
                os.remove(path)
                print(f"[DATANODE {node_id}] Deleted {chunk_name}")
                conn.send(b"DELETED")
            except FileNotFoundError:
                conn.send(b"ERROR: chunk not found")

        else:
            conn.send(b"Unknown command")

        conn.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python -m datanode.server <NodeID>")
    else:
        start_datanode(sys.argv[1])