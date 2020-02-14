# This package is written in python 3.x
import sys
import zmq
import logging


class Worker(object):
    """Worker"""
    def __init__(self, process_id):
        self.context = zmq.Context()
        self.c_connection = self.context.socket(zmq.SUB)
        self.c_connection.setsockopt(zmq.SUBSCRIBE, bytes(process_id, "utf-8"))
        self.c_connection.connect('tcp://127.0.0.1:5555')
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s  [%(levelname)-8s]: %(message)s')
        self.process_id = process_id

    """Main job for worker"""
    def do_job(self):
        while True:
            msg = self.c_connection.recv_multipart()
            # print(msg)
            msg[0] = msg[0].decode()
            msg[1] = msg[1].decode()
            if msg[0] == self.process_id:
                logging.info("{}: {}".format(msg[0], msg[1]))

                if msg[1] == "die":
                    sys.exit(0)


def create_worker(process_id):
    """Target function for process pool manager create a worker and let the worker do job"""
    worker = Worker(process_id)
    worker.do_job()
