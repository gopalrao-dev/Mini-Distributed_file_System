# master/server.py

import socket
import json
from common.config import MASTER_HOST, MASTER_PORT

METADATA_PATH = "master_metadata.json"

# In‑memory registry and metadata
data_nodes = {}
file_metadata = {}
chunk_locations = {}

def load_metadata():
    global file_metadata, chunk_locations
    try:
        with open(METADATA_PATH, "r") as f:
            data = json.load(f)
            file_metadata = data.get("file_metadata", {})
            chunk_locations = data.get("chunk_locations", {})
            print(f"[MASTER] Loaded metadata for {len(file_metadata)} files")
    except FileNotFoundError:
        # No existing metadata file
        file_metadata = {}
        chunk_locations = {}

def save_metadata():
    with open(METADATA_PATH, "w") as f:
        json.dump({
            "file_metadata": file_metadata,
            "chunk_locations": chunk_locations
        }, f, indent=2)
    print(f"[MASTER] Saved metadata ({len(file_metadata)} files)")

def start_master():
    load_metadata()
    s = socket.socket()
    s.bind((MASTER_HOST, MASTER_PORT))
    s.listen()
    print(f"[MASTER] Listening on {MASTER_HOST}:{MASTER_PORT}")

    while True:
        conn, addr = s.accept()
        msg = conn.recv(4096).decode().strip()

        if msg.startswith("DATANODE"):
            # Register a new DataNode
            node_id = msg.split()[1]
            data_nodes[node_id] = addr[0]
            print(f"[MASTER] Registered DataNode {node_id} @ {addr[0]}")
            print_current_datanodes()
            conn.send(b"Registered with master")

        elif msg.startswith("UPLOAD"):
            # Handle upload request
            filename = msg.split()[1]
            chunks = [f"{filename}_chunk1", f"{filename}_chunk2"]
            node_ids = list(data_nodes.keys())

            if len(node_ids) < 2:
                conn.send(b"[MASTER] Not enough DataNodes to replicate.")
            else:
                assignments = {}
                for i, chunk in enumerate(chunks):
                    n1 = node_ids[i % len(node_ids)]
                    n2 = node_ids[(i + 1) % len(node_ids)]
                    assignments[chunk] = [n1, n2]

                # Persist metadata
                file_metadata[filename] = chunks
                chunk_locations.update(assignments)
                save_metadata()

                # Reply with the assignment plan
                reply = "[MASTER] File chunk assignment:\n"
                for chunk, nodes in assignments.items():
                    reply += f"  {chunk} → {nodes}\n"
                conn.send(reply.encode())

        elif msg.startswith("GET"):
            # Handle download request
            filename = msg.split()[1]
            if filename not in file_metadata:
                conn.send(f"[MASTER] File {filename} not found".encode())
            else:
                reply = "[MASTER] Chunk locations:\n"
                for chunk in file_metadata[filename]:
                    nodes = chunk_locations.get(chunk, [])
                    reply += f"  {chunk} → {nodes}\n"
                conn.send(reply.encode())

        elif msg == "LS":
            # List all files
            files = list(file_metadata.keys())
            reply = "[MASTER] Files:\n"
            for fn in files:
                reply += f"  - {fn}\n"
            conn.send(reply.encode())

        elif msg.startswith("RM"):
            # Remove a file and its metadata
            filename = msg.split()[1]
            if filename not in file_metadata:
                conn.send(f"[MASTER] File {filename} not found".encode())
            else:
                chunks = file_metadata.pop(filename)
                assignments = {c: chunk_locations.pop(c) for c in chunks}
                save_metadata()

                reply = "[MASTER] Removed file and metadata:\n"
                for chunk, nodes in assignments.items():
                    reply += f"  {chunk} → {nodes}\n"
                conn.send(reply.encode())

        else:
            conn.send(b"[MASTER] Unknown command")

        conn.close()

def print_current_datanodes():
    print("\n[MASTER] Current DataNodes:")
    for node_id, ip in data_nodes.items():
        print(f"  - {node_id} @ {ip}")
    print()

if __name__ == "__main__":
    start_master()