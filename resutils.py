from enums import CsState, MsgTag
import sys
import comm
import commutils
from mpi4py import MPI
from threading import Lock


def create_comm():
    acomm = comm.MPICommAdapter(MPI.COMM_WORLD, logging=True)
    acomm.open()
    return acomm


class Process(object):

    def __init__(self, comm, m):
        self._comm = comm
        self._m = m
        self._cs_state = CsState.OUT
        self._id = comm.Get_rank()
        self._lrd = 0
        self._clock = 0
        self._used_by = [0 for i in comm.Get_size()]
        self._perm_delayed = [0 for i in comm.Get_size()]
        self._lock = Lock()

    def acquire_resource(self, k, type='basic'):
        with self._lock:
            self._cs_state = CsState.TRYING
            self._lrd = self._clock + 1
            all_indexes = range(0, self._comm.Get_size())
            indexes_to_check = set(all_indexes) - set(self._id)
            for j in indexes_to_check:
                self._comm.send(
                    {'lrd': self._lrd, 'id': self._id, 'k': k, 'type': type}, j, MsgTag.REQUEST)
                self._used_by[j] += self._m
            self._used_by[self._id] = k
        while True:
            with self._lock:
                if sum(self._used_by) <= self._m:
                    break
            # move to other thread
            for j in indexes_to_check:
                if self._comm.iprobe(source=j, tag=MsgTag.PERMISSION):
                    self._used_by[j] -= 1
        self._cs_state = CsState.IN

    def release_resource(self, k):
        with self._lock:
            self._cs_state = CsState.OUT
            for j in range(0, self._comm.Get_size()):
                if self._perm_delayed[j] > 0:
                    self._comm.send(
                        {'lrd': self._lrd, 'id': self._id, 'k': k, 'type': type}, j, MsgTag.NOT_USED)
            self._perm_delayed = [0 for i in self._comm.Get_size()]

    def run_receiver_thread(self):
        pass

    def run(self):
        pass

def usage():
    print('Usage: mpirun -np 2 python3 {} 4'.format(sys.argv[0]),
          file=sys.stderr)


def main(argv):
    res = argv[1]  # number of resources
    commutils.set_comm(create_comm())
    comm = commutils.comm()
    proc = Process(comm, res)
    proc.run()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        usage()
    else:
        main(sys.argv)