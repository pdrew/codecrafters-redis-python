import argparse
from app.server import Server
from app.constants import SERVER_ROLE, LEADER_HOST, LEADER_PORT

def main():
    parser = argparse.ArgumentParser(description="Redis clone")

    parser.add_argument("--port", type=int, default=6379, help="Port the server listens on")
    parser.add_argument("--replicaof", nargs=2)
    
    args = parser.parse_args()

    leader_host, leader_port = None, None

    if args.replicaof:
        leader_host, leader_port = args.replicaof

    config = {
        SERVER_ROLE: "slave" if args.replicaof else "master",
        LEADER_HOST: leader_host,
        LEADER_PORT: leader_port
    }

    server = Server(config)

    try:
        server.start(args.port)
    except KeyboardInterrupt:
        print("\nCaught KeyboardInterrupt. Shutting down.")

if __name__ == "__main__":
    main()
