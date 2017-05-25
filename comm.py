from threading import Thread, Lock
from queue import Queue
from mpi4py import MPI

import commutils


class BaseCommAdapter(object):
    def __init__(self, comm, logging=False):
        self.logging = logging
        self._comm = comm

    def __getattr__(self, name):
        return getattr(self._comm, name)

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def ssend(self, msg, dest, tag=0):
        self.ssend(msg, dest=dest, tag=tag)

    def srecv(self, source, tag=0):
        return self.srecv(source=source, tag=tag)

    def isfaulty(self, procid):
        return self._comm.isfaulty(procid)

    def _log(self, msg, *args, **kwargs):
        if self.logging:
            M = '{:15}: {}'.format(type(self).__name__, msg)
            commutils.log(M, *args, **kwargs)


class MPICommAdapter(BaseCommAdapter):
    def __init__(self, comm, logging=False):
        super().__init__(comm, logging=logging)
        size = comm.Get_size()
        self._queues = [Queue() for i in range(size)]
        self._to_close = size - 1
        self._recv_thread = Thread(target=self._recv_daemon)
        self.__send_lock = Lock()

    def open(self):
        self._log('Starting receiver thread')
        self._recv_thread.start()

    def close(self):
        self._log('Closing communicator')
        for i in range(self.Get_size()):
            if i != self.Get_rank():
                self._comm.send(('ctl', 'exit'), dest=i)
        self._log('Waiting for receiver thread to finish')
        self._recv_thread.join()

    def send(self, msg, dest, tag=0):
        self.ssend(msg, dest=dest, tag=tag)

    def recv(self, source, tag=0):
        return self.srecv(source=source, tag=tag)

    def iprobe(self, source, tag=0):
        self._comm.iprobe(source=source, tag=tag)

    ssend = send

    srecv = recv

    def _recv_daemon(self):
        self._log('Receiver thread started')
        status = MPI.Status()
        while True:
            self._log(self._to_close)
            if self._to_close == 0:
                break
            t, msg = self._comm.recv(source=MPI.ANY_SOURCE, status=status,
                                     tag=MPI.ANY_TAG)
            source = status.Get_source()
            self._log('Received {} message "{}" from {}'
                      .format(t, msg, source))
            if t == 'ctl' and msg == 'exit':
                self._to_close -= 1
            elif t == 'app':
                self._queues[source].put(msg)


COMM_WORLD = MPICommAdapter(MPI.COMM_WORLD, logging=False)
