# client/client.py

import socket
from common.config import MASTER_HOST, MASTER_PORT, DATANODE_PORTS

def ask_master_for_chunk_plan(filename):
    s = socket.socket()
    s.connect((MASTER_HOST, MASTER_PORT))
    s.send(f"UPLOAD {filename}".encode())
    msg = s.recv(4096).decode()
    s.close()

    print("[CLIENT] Master replied:\n" + msg)

    assignments = {}
    for line in msg.splitlines():
        if "→" in line:
            chunk, targets = line.strip().split("→")
            chunk = chunk.strip()
            target_nodes = eval(targets.strip())
            assignments[chunk] = target_nodes

    return assignments

def send_chunk_to_datanode(node_id, chunk_name, content):
    index = ord(node_id) - ord('A')
    if index >= len(DATANODE_PORTS):
        print(f"[CLIENT] No port found for node {node_id}")
        return

    port = DATANODE_PORTS[index]
    s = socket.socket()
    s.connect(('localhost', port))
    message = f"STORE {chunk_name} {content}"
    s.send(message.encode())
    reply = s.recv(1024).decode()
    print(f"[CLIENT] Sent {chunk_name} to {node_id}: {reply}")
    s.close()

def simulate_chunking(file_content):
    # Very simple: just two halves for now
    midpoint = len(file_content) // 2
    return {
        "chunk1": file_content[:midpoint],
        "chunk2": file_content[midpoint:]
    }

def upload_file(filename, content):
    chunk_map = simulate_chunking(content)
    assignments = ask_master_for_chunk_plan(filename)

    for chunk_label, chunk_data in chunk_map.items():
        chunk_name = f"{filename}_{chunk_label}"
        target_nodes = assignments.get(chunk_name, [])

        for node in target_nodes:
            send_chunk_to_datanode(node, chunk_name, chunk_data)

if __name__ == "__main__":
    cmd = input("Enter command (put/get/ls/rm): ").strip().lower()
    if cmd == "put":
        fname = input("Filename to upload: ").strip()
        content = input("File content: ")
        upload_file(fname, content)
    elif cmd == "get":
        fname = input("Filename to download: ").strip()
        # Ask master where chunks are
        s = socket.socket()
        s.connect((MASTER_HOST, MASTER_PORT))
        s.send(f"GET {fname}".encode())
        reply = s.recv(4096).decode()
        s.close()
        print("[CLIENT] Master replied:\n" + reply)
        
        # Parse chunk locations
        chunk_map = {}
        for line in reply.splitlines():
            if "→" in line:
                chunk, nodes = line.strip().split("→")
                chunk = chunk.strip()
                chunk_map[chunk] = eval(nodes.strip())

        # Retrieve each chunk and assemble
        assembled = ""
        for chunk in sorted(chunk_map):  # chunk1, then chunk2
            nodes = chunk_map[chunk]
            data = None
            for node in nodes:
                index = ord(node) - ord('A')
                if index < len(DATANODE_PORTS):
                    port = DATANODE_PORTS[index]
                    try:
                        ds = socket.socket()
                        ds.connect(('127.0.0.1', port))
                        ds.send(f"RETRIEVE {chunk}".encode())
                        data = ds.recv(65536).decode()
                        ds.close()
                        if not data.startswith("ERROR"):
                            print(f"[CLIENT] Retrieved {chunk} from {node}")
                            break
                    except ConnectionRefusedError:
                        continue
            if data is None or data.startswith("ERROR"):
                print(f"[CLIENT] Failed to retrieve {chunk}")
                assembled += ""  # or handle error
            else:
                assembled += data

        # Write out the file
        out_name = f"downloaded_{fname}"
        with open(out_name, "w") as f:
            f.write(assembled)
        print(f"[CLIENT] Download complete → {out_name}")
    
    elif cmd == "ls":
        s = socket.socket()
        s.connect((MASTER_HOST, MASTER_PORT))
        s.send(b"LS")
        reply = s.recv(4096).decode()
        s.close()
        print("[CLIENT] Master replied:\n" + reply)

    elif cmd == "rm":
        fname = input("Filename to remove: ").strip()
        # ask master to delete metadata
        s = socket.socket()
        s.connect((MASTER_HOST, MASTER_PORT))
        s.send(f"RM {fname}".encode())
        reply = s.recv(4096).decode()
        s.close()
        print("[CLIENT] Master replied:\n" + reply)

        # parse which chunks to delete
        to_delete = {}
        for line in reply.splitlines():
            if "→" in line:
                chunk, nodes = line.strip().split("→")
                to_delete[chunk.strip()] = eval(nodes.strip())

        # instruct each DataNode to delete its replicas
        for chunk, nodes in to_delete.items():
            for node in nodes:
                idx = ord(node) - ord('A')
                if idx < len(DATANODE_PORTS):
                    port = DATANODE_PORTS[idx]
                    try:
                        ds = socket.socket()
                        ds.connect(('127.0.0.1', port))
                        ds.send(f"DELETE {chunk}".encode())
                        ack = ds.recv(1024).decode()
                        print(f"[CLIENT] DataNode {node} replied: {ack}")
                        ds.close()
                    except ConnectionRefusedError:
                        print(f"[CLIENT] Node {node} unreachable; skipped")

    else:
        print("Unknown command. Use 'put', 'get', 'ls', or 'rm'.")