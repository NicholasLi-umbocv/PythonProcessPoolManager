# This package is written in python 3.x
import sys
import zmq
import time
import uuid
import logging
import threading
import multiprocessing


class CommunicatableProcess(multiprocessing.Process):
    def __init__(self, group=None, name=None, target=None, args=(), kwargs={}):
        super(CommunicatableProcess, self).__init__(group=group, target=target, name=name, args=args, kwargs=kwargs)


class PoolManager(object):
    close = False
    monitor = None
    running = False
    target = None
    process_cnt = 0
    process_pool = {}
    idle_process = []

    """Create a process pool manager object"""
    def __init__(self, process_cnt, target):
        self.context = zmq.Context()
        self.p_connection = self.context.socket(zmq.PUB)
        self.p_connection.bind('tcp://127.0.0.1:5555')
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s  [%(levelname)-8s]: %(message)s')
        self.process_cnt = process_cnt
        self.target = target

    """Run the processes"""
    def run(self):
        if self.running:
            return
        self._init_processes()
        self.running = True

        self.monitor = threading.Thread(target=self._monitor)
        self.monitor.start()

    """Return if monitor thread is alive"""
    def monitor_is_alive(self):
        return self.monitor.is_alive()

    """Get an idle process. Return None if there is no idle process."""
    def get(self):
        for process_id in self.idle_process:
            return process_id
        return None

    """Tell an idle process to work, raise exception if the process is not idle."""
    def tell_process_to_work(self, process_id, msg):
        if process_id not in self.idle_process:
            try:
                raise Exception("Error: {} is not idled".format(process_id))
            except Exception as e:
                logging.error(e)
        else:
            self.idle_process.remove(process_id)
            self.p_connection.send_multipart((bytes(process_id, "utf-8"), bytes(msg, "utf-8")))

    """Return process pool via iterator"""
    def iteritems(self):
        return self.process_pool.items()

    """Close all processes"""
    def close_processes(self):
        self.close = True
        self.monitor.join()
        for process_id, process in self.process_pool.items():
            logging.info("Send die to {}".format(process_id))
            self.p_connection.send_multipart((bytes(process_id, "utf-8"), b"die"))
            # process.p_connection.send("die")

        for process_id, process in self.process_pool.items():
            process.join(10)
            if process.is_alive():
                logging.warning("{} is still alive, forcilly terminate".format(process_id))
                process.terminate()
                process.join(5)
        sys.exit(0)

    """Initialize all processes"""
    def _init_processes(self):
        if self.running:
            return

        for process_num in range(1, self.process_cnt + 1):
            self._start_process("p-{}-{}".format(str(uuid.uuid1()), process_num))

    """Start process"""
    def _start_process(self, process_id):

        p = CommunicatableProcess(target=self.target, args=(process_id,))
        self.process_pool[process_id] = p
        self.idle_process.append(process_id)
        p.start()

    """Monitor function"""
    def _monitor(self):
        while not self.close:
            for process_id, process in self.process_pool.items():
                if not process.is_alive():
                    if process_id in self.idle_process:
                        self.idle_process.remove(process_id)
                    logging.info("{} is not alive".format(process_id))
                    process.join(10)
                    self._start_process(process_id)

            time.sleep(10)
