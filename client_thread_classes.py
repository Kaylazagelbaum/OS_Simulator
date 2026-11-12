import json
import queue
import socket
import random
import threading

JOB_QUEUE = queue.Queue()
LOCK =threading.Lock()

# helper function to validate input
def ask_choice_str(prompt, allowed):
    allowed_list = [a.strip().lower() for a in allowed]

    # validates that the word is one of the choices, reprompts if not
    while True:
        try:
            choice = str(input(prompt).strip().lower())
            if choice in allowed_list:
                return choice
            else:
                print("please enter a valid option")
        except ValueError:
            print("please enter a valid option")

# CONNECT THE CLIENT TO THE SERVER

# define the server's address
SERVER_IP = 'localhost'
SERVER_PORT = 8080  # random, above 1024

class ClientSocketThread(threading.Thread):
    def __init__(self):
        super().__init__()
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def get_client_socket(self):
        return self.client_socket

    def run(self):
        # create the socket
        try:
            self.client_socket.connect((SERVER_IP, SERVER_PORT))
            print("Connected to server")
        except ConnectionRefusedError:
            print("Connection refused")
            self.client_socket.close()

class UserInputThread(threading.Thread):
    def __init__(self):
        super().__init__()

    def run(self):
        while True:
            job_type = ask_choice_str("Select a job type (a or b): ", ["a", "b"])

            # assign a random job id
            # the id will be saved as a string not an integer
            num_id = [str(random.randint(0, 9)) for _ in range(5)]
            job_id = "".join(num_id)
            JOB_QUEUE.put((job_type, job_id))
            print("Job added to queue")

            # ask if the user wants to submit another job
            choice = ask_choice_str("Do you want to submit another job? (y or n): ", ["y", "n"])
            if choice == "y":
                continue
            else:
                break

        JOB_QUEUE.put(None)

class JobSenderThread(threading.Thread):
    def __init__(self, client_socket_instance):
        super().__init__()
        self.local_client_socket = client_socket_instance.get_client_socket()

    def run(self):
        while True:
            job = JOB_QUEUE.get()
            if job is None:
                print("Job queue empty")
                break
            try:
                self.local_client_socket.sendall((json.dumps(job)).encode("utf-8"))
                print("Sent job data to server")
                # Uncomment the following line to view what the package looks like, it should be ["job_type", "5_digit_job_id"]
                # print("JSON =" + json.dumps(job))
            except (BrokenPipeError, OSError):
                print("Server connection lost")
                break
        self.local_client_socket.close()

def main():
    client_socket = ClientSocketThread()
    user_input_thread = UserInputThread()
    job_sender_thread = JobSenderThread(client_socket)

    client_socket.start()
    client_socket.join()

    user_input_thread.start()
    job_sender_thread.start()

    user_input_thread.join()
    job_sender_thread.join()

if __name__ == "__main__":
    main()






