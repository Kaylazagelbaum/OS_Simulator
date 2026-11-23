import random
import threading
import socket
import json
from collections import deque

""" Handles master logic, connections will be handled by other files.
    Sockets created in other files will be: client_1, client_2, slave_1, slave_2, listener.
    Master acts as a server for the Client-Master relationship, and a client for the Master-Slave relationship.
"""


job_queue_lock = threading.Lock()
workload_a_lock = threading.Lock()
workload_2_lock = threading.Lock()
slave_1_jobs_lock = threading.Lock()
slave_2_jobs_lock = threading.Lock()
# sets the sleep time for each job.
OPTIMAL_SLEEP_TIME = 2
NONOPTIMAL_SLEEP_TIME = 10

# connection names
connections = {}

# dict stores each job and its optimal slave. (Scalable)
optimal_slave_for_job = {"job_a": "slave_1", "job_b": "slave_2"}

# slave workloads should be locked when accessed
slave_1_workload = 0
slave_2_workload = 0

slave_1_jobs = deque([])
slave_2_jobs = deque([])

client1_messages = deque([])
client2_messages = deque([])


jobs = deque([
    ("a", 10001),
    ("b", 10002),
    ("a", 10003),
    ("b", 10004),

    ("a", 10005),
    ("a", 10006),
    ("a", 10007),
    ("a", 10008),
    ("a", 10009),
    ("a", 10010),

    ("b", 10011),
    ("b", 10012),
    ("b", 10013),
    ("b", 10014),
    ("b", 10015),
    ("b", 10016),

    ("a", 10017),
    ("b", 10018),
    ("b", 10019),
    ("a", 10020)
]) # the jobs queue, entered in random values for the test

# make sure there's error handling so could only be job a or b
def determine_job_time(job_type, slave_type) -> int:
    """Returns the sleeping time based off of slave and job type."""
    job_type = job_type.lower()
    if job_type == "a":
        if slave_type == "slave_a":
            time = OPTIMAL_SLEEP_TIME
        else:
            time = NONOPTIMAL_SLEEP_TIME
    elif job_type == "b":
        if slave_type == "slave_b":
            time = OPTIMAL_SLEEP_TIME
        else:
            time = NONOPTIMAL_SLEEP_TIME
    return time

def which_slave(job_type) -> str:
    """Decides which slave to give the work to based off workloads. Locks the entire time to
       ensure that the workloads don't change in the interim."""
    global slave_1_workload, slave_2_workload
    slave_1_time = determine_job_time(job_type, "slave_a")
    slave_2_time = determine_job_time(job_type, "slave_b")
    with workload_a_lock:
        with workload_2_lock:
            total_workload_1 = slave_1_workload + slave_1_time
            total_workload_2 = slave_2_workload + slave_2_time
            if total_workload_1 < total_workload_2:
                slave_1_workload += slave_1_time
                return "slave_1"
            elif total_workload_2 < total_workload_1:
                slave_2_workload += slave_2_time
                return "slave_2"
            else: # in the case of exact same timing
                slave = random.choice(["slave_1", "slave_2"])
                if slave == "slave_1":
                    slave_1_workload += slave_1_time
                else:
                    slave_2_workload += slave_2_time
                return slave

'''def add_job(time, slave) -> None:
    """Adds a job to the workload with the lock."""
    global slave_1_workload, slave_2_workload
    if slave == "slave_1":
        with workload_1_lock:
            slave_1_workload += time
    else:
        with workload_2_lock:
            slave_2_workload += time'''

def job_complete(time, slave) -> None:
    """Called when slave sends a completion method - removes job from the workload
       with the lock."""
    global slave_1_workload, slave_2_workload
    if slave == "slave_1":
        with workload_a_lock:
            slave_1_workload -= time
    else:
        with workload_2_lock:
            slave_2_workload -= time

def constant_job_handling():
    """the thread which is always checking the queue, even if it's currently empty, for jobs, determines which slave to send to and updates the
       queue and workloads."""
    while True:
        if not jobs: # if empty, spin
            continue
        with job_queue_lock:
            curr_job_type, curr_job_ID = jobs.popleft()
        slave = which_slave(curr_job_type)
        if slave == "slave_1": # just a test for now
            print(f"Sending job to slave_1, job type: {curr_job_type}, job ID {curr_job_ID}")
            slave_1_jobs.append((curr_job_type, curr_job_ID))

        else:
            print(f"Sending job to slave_2, job type: {curr_job_type}, job ID {curr_job_ID}")
            slave_2_jobs.append((curr_job_type, curr_job_ID))



'''def display_job_completed(job_id) -> None:
    print()
    print(f"Job #{job_id} completed by slave.")

def display_sent_to_slave(job_id, slave) -> None:
    print()
    print(f"Job #{job_id} was sent to {slave}.")'''


def client_sender(connection):
    """For now, just sends a one time message to show connection. Will send done messages."""
    message = "Hi, I'm connected"
    message = message.encode()
    connection.sendall(message)
    print("Sent message to client")


def client_receiver(client_connection):
    """For now, just receives jobs and prints received"""
    while True:
        data = client_connection.recv(1024)
        if not data:
            break
        job_type, job_id = json.loads(data.decode("utf-8"))
        print(f"Received Job type {job_type}, Job ID {job_id}")
        jobs.append((job_type, job_id))


'''def slave_sender(slave_connection, jobs_list, lock):
    """For now just sends to the slave a random message. Will send jobs."""
    #TODO - does it need a lock? only if another function ends up accessing it. tbd.
    #with slave_connection:
    message = "Testing"
    message = message.encode()
    slave_connection.sendall(message)
    print("sent slave a message")
    while True:
        if not jobs_list:
            continue # spin while empty
        with lock:
            job = jobs_list.popleft()
        json_job = json.dumps(job).encode()
        slave_connection.sendall(json_job)'''


def slave_receiver(slave_connection, slave_type):
    """For now just receives the test from slave."""
    while True:
        data = slave_connection.recv(1024)
        if not data:
            break
        message = data.decode() # should be a done message, will have to send to client
        print(message)

def start_and_join(my_thread1, my_thread2):
    """Appends each thread to the list and starts them - helper for identify_connection."""
    my_thread1.start()
    my_thread2.start()
    my_thread1.join()
    my_thread2.join()

def identify_connection(connection):
    """Once the listener receives a connection, this function redirects and starts the proper thread.
       Once a thread is initialized, its connection continues to be dealt with in its proper thread."""
    with connection:
        message = connection.recv(1024)
        message = message.decode()
        if message == "client_1":
            client1_receiver = threading.Thread(target = client_receiver, args=(connection,))
            client1_sender = threading.Thread(target= client_sender, args=(connection,))
            start_and_join(client1_receiver, client1_sender)

        elif message == "client_2":
            client2_receiver = threading.Thread(target=client_receiver, args=(connection,))
            client2_sender = threading.Thread(target=client_sender, args=(connection,))
            start_and_join(client2_receiver, client2_sender)

        elif message == "slave_1":
            slave1_receiver = threading.Thread(target=slave_receiver, args=(connection, "slave_1"))
            #slave1_sender = threading.Thread(target=slave_sender, args=(connection, slave_1_jobs, slave_1_jobs_lock))
            start_and_join(slave1_receiver, slave1_sender)

        elif message == "slave_2":
            slave2_receiver = threading.Thread(target=slave_receiver, args=(connection, "slave_2"))
            #slave2_sender = threading.Thread(target=slave_sender, args=(connection, slave_2_jobs, slave_2_jobs_lock))
            start_and_join(slave2_receiver, slave2_sender)

        else:
            print("Unknown connection.")




def listener():
    """This general listener listens for unprecedented connections. """
    general_listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    general_listener.bind(("127.0.0.1", 7073
                           ))
    general_listener.listen()

    while True:
        connection, address = general_listener.accept()
        new_receiver = threading.Thread(target = identify_connection, args = (connection,))
        threads.append(new_receiver)
        new_receiver.start()




threads = []
if __name__ == "__main__":
    my_listener = threading.Thread(target=listener)
    my_listener.start()
    calculator = threading.Thread(target=constant_job_handling)
    calculator.start()
    my_listener.join()
    calculator.join()














