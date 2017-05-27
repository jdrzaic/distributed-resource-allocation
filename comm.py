from threading import Thread, Lock
from mpi4py import MPI
import sys
import commutils
from enums import CsState, REQUEST, NOT_USED


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

    def send(self, msg, dest, tag=0):
        self.send(msg, dest=dest, tag=tag)

    def recv(self, source, status, tag=0):
        return self.recv(source=source, status=status, tag=tag)

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
        self._id = comm.Get_rank()
        self._cs_state = CsState.OUT
        self._lrd = sys.maxsize
        self._clock = 0
        self._used_by = [0] * size
        self._perm_delayed = [0] * size
        self._prio = True if comm.Get_rank() == 0 else False
        self._lock = Lock()

    def open(self):
        self._recv_thread.start()

    def close(self):
        self._log('Closing communicator')

    def send(self, msg, dest, tag=REQUEST):
        self._comm.send(msg, dest=dest, tag=tag)

    def recv(self, source, status, tag=REQUEST):
        return self._comm.recv(source=source, status=status, tag=tag)

    def Isend(self, msg, dest, tag=REQUEST):
        self._comm.Isend(msg, dest=dest, tag=tag)

    def Irecv(self, source, status, tag=REQUEST):
        return self._comm.Irecv(source=source, status=status, tag=tag)

    def Iprobe(self, source, tag=REQUEST):
        return self._comm.Iprobe(source=source, tag=tag)

    def _recv_daemon(self):
        self._log('Receiver thread started for process with id {0}'.format(self._id))
        status = MPI.Status()
        while True:
            if self.Iprobe(source=MPI.ANY_SOURCE, tag=REQUEST):
                with self._lock:
                    result = self.recv(source=MPI.ANY_SOURCE, status=status, tag=REQUEST)
                    self._log('Received request for permission for {0} instances of type {1} from '
                              '{2}'.format(result['k'], result['type'], result['id']))
                    self._clock = max(self._clock, result['lrd'])
                    less_op = self._lrd < result['lrd'] or \
                              (self._lrd == result['lrd'] and self._id < result['id'])
                    self._prio = self._cs_state is not CsState.OUT and less_op
                    if not self._prio or (self._prio and self._perm_delayed[result['id']]):
                        self._log('Sending permission for {0} {1}'.format(self._m,'instances'))
                        self.send(
                            {'lrd': self._lrd, 'id': self._id, 'k': self._m, 'type': type},
                            result['id'], NOT_USED)
                        self._log('Sent permission to {0}'.format(result['id']))
                    else:
                        if self._used_by[self._id] != self._m:
                            self._log(
                                'Sending permission for {0} {1}'.format(
                                    self._m - self._used_by[self._id], ' instances'))
                            self.send(
                                {'lrd': self._lrd, 'id': self._id,
                                 'k': (self._m,  - self._used_by[self._id]), 'type': type},
                                result['id'], NOT_USED)
                            self._log("Sent permission to {0}".format(result['id']))
                        self._perm_delayed[result['id']] += 1
            if self.Iprobe(source=MPI.ANY_SOURCE, tag=NOT_USED):
                with self._lock:
                    result = self.recv(source=MPI.ANY_SOURCE, status=status, tag=NOT_USED)
                    self._used_by[result['id']] -= result['k']

    def acquire_resource(self, k, type='basic'):
        with self._lock:
            self._cs_state = CsState.TRYING
            self._lrd = self._clock + 1
            all_indexes = range(self._comm.Get_size())
            indexes_to_check = [ind for ind in all_indexes if ind != self._id]
            for j in indexes_to_check:
                self._log(
                    'Sending request for {0} instances of type "{1}" to {2}'.format(k, type, j))
                self.send(
                    msg={'lrd': self._lrd, 'id': self._id, 'k': k, 'type': type}, dest=j, tag=REQUEST)
                self._log("Sent request to {0}".format(j))
                self._used_by[j] += self._m
            self._used_by[self._id] = k
        while True:
            with self._lock:
                if sum(self._used_by) <= self._m:
                    self._cs_state = CsState.IN
                    break
        self._log("Acquired {0} resources".format(k))

    def release_resource(self, k):
        self._log("Releasing {0} resources".format(k))
        with self._lock:
            self._cs_state = CsState.OUT
            indexes_to_check = list(range(self.Get_size()))
            for j in indexes_to_check:
                if self._perm_delayed[j] > 0:
                    self._log(
                        'Sending permission for {0} {1}'.format(k, ' instances'))
                    self.send(
                        {'lrd': self._lrd, 'id': self._id, 'k': k, 'type': type}, j, NOT_USED)
            self._perm_delayed = [0 for i in self._comm.Get_size()]


COMM_WORLD = MPICommAdapter(MPI.COMM_WORLD, logging=False)
