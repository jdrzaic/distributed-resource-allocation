from threading import Thread, Lock
from mpi4py import MPI
import sys
from time import sleep
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
    def __init__(self, comm, logging=False, res=[], info_logging=True):
        super().__init__(comm, logging=logging)
        self._info_logging = info_logging
        num_proc = comm.Get_size()
        res = [(-1, 1)] + res
        num_types = len(res)
        self._m = dict(res)
        self._recv_thread = Thread(target=self._recv_daemon)
        self._log_thread = Thread(target=self._log_daemon)
        self._id = comm.Get_rank()
        self._cs_state_gm = CsState.OUT
        self._cs_state = dict([(i, CsState.OUT) for i, _ in res])
        self._lrd = sys.maxsize
        self._clock = 0
        self._used_by = dict([(i, [0] * num_proc) for i, _ in res])
        self._perm_delayed = dict([(i, [0] * num_proc) for i, _ in res])
        self._prio = True if comm.Get_rank() == 0 else False
        self._lock = Lock()

    def open(self):
        self._recv_thread.start()
        if self._info_logging:
            self._log_thread.start()

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

    def _log_daemon(self):
        while True:
            # print(self._cs_state)
            if any(map(lambda s: self._cs_state[s] == CsState.TRYING, self._cs_state)):
                commutils.log('Trying for {0} resources'.format(
                    [self._used_by[i][self._id] for i in self._cs_state]))
            elif any(map(lambda s: self._cs_state[s] == CsState.IN, self._cs_state)):
                commutils.log('Working with {0} resources'.format(
                    [self._used_by[i][self._id] for i in self._cs_state]))
            else:
                commutils.log('Resting...')
            sleep(1)

    def _recv_daemon(self):
        self._log('Receiver thread started for process with id {0}'.format(self._id))
        status = MPI.Status()
        while True:
            if self.Iprobe(source=MPI.ANY_SOURCE, tag=REQUEST):
                with self._lock:
                    result = self.recv(source=MPI.ANY_SOURCE, status=status, tag=REQUEST)
                    i = result['type']
                    self._log('Received request for permission for {0} instances of type {1} from '
                              '{2}'.format(result['k'], i, result['id']))
                    self._clock = max(self._clock, result['lrd'])
                    less_op = self._lrd < result['lrd'] or \
                              (self._lrd == result['lrd'] and self._id < result['id'])
                    self._prio = self._cs_state[i] is not CsState.OUT and less_op
                    if not self._prio or (self._prio and self._perm_delayed[i][result['id']]):
                        self._log('Sending permission for {0} of {1}'.format(self._m[i] ,i))
                        self.send(
                            {'lrd': self._lrd, 'id': self._id, 'k': self._m[i], 'type': i},
                            result['id'], NOT_USED)
                        self._log('Sent permission to {0}'.format(result['id']))
                    else:
                        if self._used_by[i][self._id] != self._m[i]:
                            diff = self._m[i] - self._used_by[i][self._id]
                            self._log(
                                'Sending permission for {0} of {1}'.format(diff, i))
                            self.send(
                                {'lrd': self._lrd, 'id': self._id,
                                 'k': diff, 'type': i},
                                result['id'], NOT_USED)
                            self._log("Sent permission to {0}".format(result['id']))
                        self._perm_delayed[i][result['id']] += 1
            if self.Iprobe(source=MPI.ANY_SOURCE, tag=NOT_USED):
                with self._lock:
                    result = self.recv(source=MPI.ANY_SOURCE, status=status, tag=NOT_USED)
                    i = result['type']
                    self._log("Received permission from {0}".format(result['id']))
                    self._used_by[i][result['id']] -= result['k']
    def acquire_gm(self): self.acquire_resource([(-1, 1)])
    def release_gm(self): self.release_resource([(-1, 1)])

    def acquire_resource(self, k):
        if (k != [(-1, 1)]): 
            self._log("Getting GM")
            self.acquire_gm()
        with self._lock:
            for i, _ in k: self._cs_state[i] = CsState.TRYING
            self._lrd = self._clock + 1
            all_indexes = range(self._comm.Get_size())
            indexes_to_check = [ind for ind in all_indexes if ind != self._id]
            for i, ki in k:
                for j in indexes_to_check:
                    self._log(
                        'Sending request for {0} instances of type "{1}" to {2}'.format(ki, i, j))
                    self.send(
                        msg={'lrd': self._lrd, 'id': self._id, 'k': ki, 'type': i}, dest=j, tag=REQUEST)
                    self._log("Sent request to {0}".format(j))
                    self._used_by[i][j] += self._m[i]
                self._used_by[i][self._id] = ki
        while True:
            with self._lock:
                if all([sum(self._used_by[i]) <= self._m[i] for i, _ in k]) :
                    for i, _ in k: self._cs_state[i] = CsState.IN
                    break
        self._log("Acquired {0} resources".format(k))
        if (k != [(-1, 1)]): 
            self._log("Releasing GM")
            self.release_gm()

    def release_resource(self, k):
        self._log("Releasing {0} resources".format(k))
        with self._lock:
            for i, ki in k:
                self._cs_state[i] = CsState.OUT
                indexes_to_check = list(range(self.Get_size()))
                for j in indexes_to_check:
                    if self._perm_delayed[i][j] > 0:
                        self._log(
                            'Sending permission for {0} of {1}'.format(ki, i))
                        self.send(
                            {'lrd': self._lrd, 'id': self._id, 'k': ki, 'type': i}, j, NOT_USED)
                        self._log("Sent permission to {0}".format(j))
                self._perm_delayed[i] = [0] * self._comm.Get_size()


COMM_WORLD = MPICommAdapter(MPI.COMM_WORLD, logging=False)
