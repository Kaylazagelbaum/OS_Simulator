import threading
import socket
import time

from tovas_code import slave_a_jobs_lock, slave_b_jobs_lock, slave_a_jobs, slave_b_jobs

SERVER_IP = 'localhost'
SERVER_PORT = 7073

OPTIMIZED_SLEEP = 2
NON_OPTIMIZED_SLEEP = 10


class SlaveSocketThread(threading.Thread):
    """ Creates a socket thread to communicate with the master. Assesses the slave type and assigns the proper queue and lock accordingly."""

    def __init__(self, slave_type):
        super().__init__()
        self.slave_type = slave_type
        if self.slave_type == "a":
            self.optimized_type = "a"
            self.job_lock = slave_a_jobs_lock
            self.job_queue = slave_a_jobs
        elif self.slave_type == "b":
            self.optimized_type = "b"
            self.job_lock = slave_b_jobs_lock
            self.job_queue = slave_b_jobs
        self.slave_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


    def run(self):
        """ Attempts to connect to the master, prints an error and closes the socket if the connection fails. Once connected it creates an instance of the
            job receiver thread using the appropriate lock and queue. The job receiver thread extracts the job type and job id from the queue and passes it
            back to the slave socket thread. The slave socket thread executes the job and sends a message to the master when it is finished."""

        try:
            self.slave_socket.connect((SERVER_IP, SERVER_PORT))
            self.slave_socket.send(self.slave_type.encode('utf-8'))
            print("Connected to server")

            job_receiver = JobReceiverThread(self.job_lock, self.job_queue)
            job_receiver.start()
            job_type, job_id = job_receiver.return_data()
            # job execution returns a message indicating that the job has been completed which is encapsulated by the message variable
            message = self.execute_job(job_type, job_id)
            self.slave_socket.send(message.encode('utf-8'))

        except ConnectionRefusedError:
            print("Connection refused")
            self.slave_socket.close()


    def execute_job(self, job_type, job_id):
        """ Prints statements for user clarity and sleeps for the specified amount of time. Returns a message indicating the job id of the completed job."""

        print("Executing Job " + job_id)
        if job_type == self.optimized_type:
            time.sleep(OPTIMIZED_SLEEP)
        else:
            time.sleep(NON_OPTIMIZED_SLEEP)
        return "Completed job " + job_id


class JobReceiverThread(threading.Thread):
    """ This thread class is responsible for getting the next job and returning the information for further use. """

    def __init__(self, which_job_lock, which_job_queue):
        super().__init__()
        self.data = None
        self.job_lock = which_job_lock
        self.job_queue = which_job_queue


    def run(self):
        while True:
            with self.job_lock:
                self.data = self.job_queue.get()
            if self.data:
                print("Slave received data: ", self.data)
            else:
                  break


    def return_data(self):
        if self.data:
            job_type, job_id = self.data
            return job_type, job_id
        else:
            return "No job data found."


def main():
    slave_a_socket_thread = SlaveSocketThread("a")
    slave_a_socket_thread.start()
    slave_b_socket_thread = SlaveSocketThread("b")
    slave_b_socket_thread.start()


if __name__ == '__main__':
    main()