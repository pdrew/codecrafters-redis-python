import argparse
from app.server import Server

def main():
    parser = argparse.ArgumentParser(description="Redis clone")

    parser.add_argument("--port", type=int, default=6379, help="Port the server listens on")

    args = parser.parse_args()

    server = Server()

    try:
        server.start(args.port)
    except KeyboardInterrupt:
        print("\nCaught KeyboardInterrupt. Shutting down.")

if __name__ == "__main__":
    main()
