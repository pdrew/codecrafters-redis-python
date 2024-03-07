# Uncomment this to pass the first stage
from app.server import Server

def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    port = 6379

    server = Server()

    try:
        server.start(port)
    except KeyboardInterrupt:
        print("\nCaught KeyboardInterrupt. Shutting down.")

if __name__ == "__main__":
    main()
