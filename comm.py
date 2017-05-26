from threading import Thread, Lock
from queue import Queue
from mpi4py import MPI

import commutils
from enums import CsState, MsgTag


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
    def __init__(self, comm, logging=False, m=1):
        super().__init__(comm, logging=logging)
        size = comm.Get_size()
        self._m = m
        self._recv_thread = Thread(target=self._recv_daemon)
        self._cs_state = CsState.OUT
        self._id = comm.Get_rank()
        self._lrd = 0
        self._clock = 0
        self._used_by = [0 for i in size]
        self._perm_delayed = [0 for i in size]
        self._lock = Lock()

    def open(self):
        self._log('Starting receiver thread')
        self._recv_thread.start()

    def close(self):
        self._log('Closing communicator')

    def send(self, msg, dest, tag=MsgTag.REQUEST):
        self.ssend(msg, dest=dest, tag=tag)

    def recv(self, source, tag=MsgTag.REQUEST):
        return self.srecv(source=source, tag=tag)

    def iprobe(self, source, tag=MsgTag.REQUEST):
        self._comm.iprobe(source=source, tag=tag)

    ssend = send

    srecv = recv

    def _recv_daemon(self):
        self._log('Receiver thread started')
        status = MPI.Status()
        while True:
            if self.iprobe(source=MPI.ANY_SOURCE, tag=MsgTag.REQUEST):
                pass
            if self.iprobe(source=MPI.ANY_SOURCE, tag=MsgTag.NOT_USED):
                pass
            result = self._comm.recv(source=MPI.ANY_SOURCE, status=status, tag=MPI.ANY_TAG)

    def acquire_resource(self, k, type='basic'):
        with self._lock:
            self._cs_state = CsState.TRYING
            self._lrd = self._clock + 1
            all_indexes = range(0, self._comm.Get_size())
            indexes_to_check = set(all_indexes) - set(self._id)
            for j in indexes_to_check:
                self.send(
                    {'lrd': self._lrd, 'id': self._id, 'k': k, 'type': type}, j, MsgTag.REQUEST)
                self._used_by[j] += self._m
            self._used_by[self._id] = k
        while True:
            with self._lock:
                if sum(self._used_by) <= self._m:
                    break
        self._cs_state = CsState.IN

    def release_resource(self, k):
        with self._lock:
            self._cs_state = CsState.OUT
            indexes_to_check = set(range(0, self._comm.Get_size()))
            for j in indexes_to_check:
                if self._perm_delayed[j] > 0:
                    self._comm.send(
                        {'lrd': self._lrd, 'id': self._id, 'k': k, 'type': type}, j, MsgTag.NOT_USED)
            self._perm_delayed = [0 for i in self._comm.Get_size()]


COMM_WORLD = MPICommAdapter(MPI.COMM_WORLD, logging=False)
