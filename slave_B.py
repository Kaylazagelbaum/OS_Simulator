import threading
import socket

LOCK = threading.Lock()

SERVER_IP = 'localhost'
SERVER_PORT = 8080

OPTIMIZED_TYPE = "b"
OPTIMIZED_SLEEP = 2
NON_OPTIMIZED_SLEEP = 10


class SlaveBSocketThread(threading.Thread):
    def __init__(self):
        super().__init__()
        self.slave_b_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def run(self):
        try:
            self.slave_b_socket.connect((SERVER_IP, SERVER_PORT))
            print("Connected to server")
        except ConnectionRefusedError:
            print("Connection refused")
            self.slave_b_socket.close()

def main():
    slave_b_socket_thread =SlaveBSocketThread()
    slave_b_socket_thread.start()

if __name__ == '__main__':
    main()