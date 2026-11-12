import socket
import threading

SERVER_IP = 'localhost'
SERVER_PORT = 8080

class ServerSocketThread(threading.Thread):
    def __init__(self):
        super().__init__()
        self.server_socket = None

    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            self.server_socket = server_socket
            self.server_socket.bind((SERVER_IP, SERVER_PORT))
            self.server_socket.listen()
            print("Server Listening")

            # run a handler thread to connect and receive data from the client
            connection, client_address = self.server_socket.accept()
            job_receiver_handler = JobReceiverThread(connection, client_address)
            job_receiver_handler.start()


class JobReceiverThread(threading.Thread):
    def __init__(self, connection, client_address):
        super().__init__()
        self.connection = connection
        self.client_address = client_address

    def run(self):
        with self.connection:
            print("Client connected")
            while True:
                data = self.connection.recv(1024)
                if data:
                    print("Server received data: ", data)
                else:
                    break



def main():
    server_socket_thread = ServerSocketThread()
    server_socket_thread.start()

if __name__ == '__main__':
    main()

