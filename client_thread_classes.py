import json
import socket
import random
import threading


def ask_choice_str(prompt, allowed):
    """ This helper functions prompts a user to answer given a few choices and validates that the user's answer is one of those choices."""

    allowed_list = [a.strip().lower() for a in allowed]

    while True:
        try:
            choice = str(input(prompt).strip().lower())
            if choice in allowed_list:
                return choice
            else:
                print("please enter a valid option")

        except ValueError:
            print("please enter a valid option")


SERVER_IP = 'localhost'
SERVER_PORT = 7073 # random, above 1024


class ClientSocketThread(threading.Thread):
    """ This thread creates a socket to connect the client to the master server and prints statements to keep the user informed. Once the socket
        is established, the client informs the master which number it is."""
    def __init__(self, user_num):
        super().__init__()
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.user_num = user_num

    def run(self):
        # create the socket
        try:
            self.client_socket.connect((SERVER_IP, SERVER_PORT))
            self.client_socket.send(f"client_{self.user_num}".encode("utf-8"))
            print(f"client_{self.user_num} connected to server")
        except ConnectionRefusedError:
            print("Connection refused")
            self.client_socket.close()

    def get_client_socket(self):
        return self.client_socket

class UserInputThread(threading.Thread):
    """ Creates a user input thread for the specified client socket. """

    def __init__(self, client_socket_instance):
        super().__init__()
        self.local_client_socket = client_socket_instance.get_client_socket()

    def run(self):
        """ Runs a continuous loop to prompt the user for job submissions until the user chooses to stop. For each job, the user specifies if it is
            type a or b, and the job id is assigned randomly."""

        while True:
            job_response = ask_choice_str("Select a job type (a or b): ", ["a", "b"])
            if job_response == "a":
                job_type = "job_a"
            elif job_response == "b":
                job_type = "job_b"

            # assign a random job id
            # the id will be saved as a string not an integer
            num_id = [str(random.randint(0, 9)) for _ in range(5)]
            job_id = "".join(num_id)
            job = (job_type, job_id)

            if job is None:
                print("No job available")
                break

            try:
                self.local_client_socket.sendall((json.dumps(job)).encode("utf-8"))
                print("Sent job data to server")
                # Uncomment the following line to view what the package looks like, it should be ["job_type", "5_digit_job_id"]
                # print("JSON =" + json.dumps(job))

            except (BrokenPipeError, OSError):
                print("Server connection lost")
                break
            # ask if the user wants to submit another job
            choice = ask_choice_str("Do you want to submit another job? (y or n): ", ["y", "n"])
            if choice == "y":
                continue
            else:
                break


def main():
    client_socket1 = ClientSocketThread(1)
    client_socket2 = ClientSocketThread(2)
    user1_input_thread = UserInputThread(client_socket1)
    user2_input_thread = UserInputThread(client_socket2)

    client_socket1.start()
    client_socket2.start()
    client_socket1.join()
    client_socket2.join()

    user1_input_thread.start()
    user2_input_thread.start()

    user1_input_thread.join()
    user2_input_thread.join()

if __name__ == "__main__":
    main()






