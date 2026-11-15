import random
import threading
import socket
import json

""" Handles master logic, connections will be handled by other files.
    Sockets created in other files will be: client_1, client_2, slave_1, slave_2, listener.
    Master acts as a server for the Client-Master relationship, and a client for the Master-Slave relationship.
"""



# sets the sleep time for each job.
OPTIMAL_SLEEP_TIME = 2
NONOPTIMAL_SLEEP_TIME = 10

# connection names
connections = {}

# dict stores each job and its optimal slave. (Scalable)
optimal_slave_for_job = {"job_a": "slave_1", "job_b": "slave_2"}

slave_1_workload = 0
slave_2_workload = 0

def determine_job_time(job_type, slave_type) -> int:
    """Returns the sleeping time based off of slave and job type."""
    if job_type == "job_a":
        if slave_type == "slave_a":
            time = OPTIMAL_SLEEP_TIME
        else:
            time = NONOPTIMAL_SLEEP_TIME
    elif job_type == "job_b":
        if slave_type == "slave_b":
            time = OPTIMAL_SLEEP_TIME
        else:
            time = NONOPTIMAL_SLEEP_TIME
    return time

def which_slave(time) -> str:
    """Decides which slave to give the work to based off workloads."""
    total_workload_1 = slave_1_workload + time
    total_workload_2 = slave_2_workload + time
    if total_workload_1 > total_workload_2:
        return "slave_1"
    elif total_workload_2 > total_workload_1:
        return "slave_2_"
    else:
        return random.choice(["slave_1", "slave_2"])

def add_job(time, slave) -> None:
    global slave_1_workload, slave_2_workload
    if slave == "slave_1":
        slave_1_workload += time
    else:
        slave_2_workload += time

def job_complete(time, slave) -> None:
    global slave_1_workload, slave_2_workload
    if slave == "slave_1":
        slave_1_workload -= time
    else:
        slave_2_workload -= time

def display_job_completed(job_id) -> None:
    print()
    print(f"Job #{job_id} completed by slave.")

def display_sent_to_slave(job_id, slave) -> None:
    print()
    print(f"Job #{job_id} was sent to {slave}.")


def client_sender(connection):
    """For now, just sends a one time message to show connection"""
    message = "Hi, I'm connected"
    message = message.encode()
    connection.sendall(message)
    print("Sent message to client")


def client_receiver(client_connection):
    """For now, just receives jobs and prints received"""
    with client_connection:
        while True:
            data = client_connection.recv(1024)
            job_id, job_type = json.loads(data.decode("utf-8"))
            print(f"Received Job type{job_type}, Job ID {job_id}")


def slave_sender(slave_connection):
    """For now just sends to the slave a random message"""
    with slave_connection:
        message = "Testing"
        message.encode()
        slave_connection.sendall(message)
        print("sent slave a message")


def slave_receiver(slave_connection):
    """For now just receives the test from slave."""
    with slave_connection:
        while True:
            data = slave_connection.recv(1024)
            message = data.decode()
            print(message)

def append_and_start(my_thread1, my_thread2):
    """Appends each thread to the list and starts them - helper for identify_connection."""
    threads.append(my_thread1)
    threads.append(my_thread2)
    my_thread1.start()
    my_thread2.start()

def identify_connection(connection, address):
    """Once the listener receives a connection, this function redirects and starts the proper thread.
       Once a thread is initialized, its connection continues to be dealt with in its proper thread."""
    with connection:
        message = connection.recv(1024)
        message = message.decode()
        if message == "client_1":
            client1_receiver = threading.Thread(target = client_receiver, args=(connection,))
            client1_sender = threading.Thread(target= client_sender, args=(connection,))
            append_and_start(client1_receiver, client1_sender)

        elif message == "client_2":
            client2_receiver = threading.Thread(target=client_receiver, args=(connection,))
            client2_sender = threading.Thread(target=client_sender, args=(connection,))
            append_and_start(client2_receiver, client2_sender)

        elif message == "slave_1":
            slave1_receiver = threading.Thread(target=slave_receiver, args=(connection,))
            slave1_sender = threading.Thread(target=slave_sender, args=(connection,))
            append_and_start(slave1_receiver, slave1_sender)

        elif message == "slave_2":
            slave2_receiver = threading.Thread(target=slave_receiver, args=(connection,))
            slave2_sender = threading.Thread(target=slave_sender, args=(connection,))
            append_and_start(slave2_receiver, slave2_sender)


def listener():
    """This general listener listens for unprecedented connections. """
    general_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    general_listener.bind(("127.0.0.1", 5000))
    general_listener.listen()

    while True:
        connection, address = general_listener.accept()
        new_receiver = threading.Thread(target = identify_connection, args = (connection, address))
        threads.append(new_receiver)
        new_receiver.start()




threads = []
my_listener = threading.Thread(target= listener).start()
threads.append(my_listener)
#calculator = threading.Thread()
#threads.append(calculator)

#lock = threading.Lock()

for thread in threads:
    thread.join()









