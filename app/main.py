import argparse
import base64
from app.server import Server
from app.constants import *

def main():
    parser = argparse.ArgumentParser(description="Redis clone")

    parser.add_argument("--port", type=int, default=6379, help="Port the server listens on")
    parser.add_argument("--replicaof", nargs=2)
    parser.add_argument("--dir", help="The directory where RDB files are stored")
    parser.add_argument("--dbfilename", help="The name of the RDB file")

    args = parser.parse_args()

    leader_host, leader_port, rdb_dir, rdb_filename = None, None, None, None

    if args.replicaof:
        leader_host, leader_port = args.replicaof

    if args.dir:
        rdb_dir = args.dir.rstrip()
    
    if args.dbfilename:
        rdb_filename = args.dbfilename.rstrip()

    config = {
        ROLE:  FOLLOWER_ROLE if args.replicaof else LEADER_ROLE,
        LEADER_HOST: leader_host,
        LEADER_PORT: int(leader_port) if leader_port else leader_port,
        REPLID: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
        REPLOFFSET: 0,
        RDB_DIR: rdb_dir,
        RDB_FILENAME: rdb_filename,
    }

    server = Server(config)

    try:
        server.start(args.port)
    except KeyboardInterrupt:
        print("\nCaught KeyboardInterrupt. Shutting down.")

if __name__ == "__main__":
    main()
