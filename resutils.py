from enums import CsState, MsgTag


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

    def acquire_resource(self, k, type='basic'):
        self._cs_state = CsState.TRYING
        self._lrd = self._clock + 1
        all_indexes = range(0, self._comm.Get_size())
        indexes_to_check = set(all_indexes) - set(self._id)
        for j in indexes_to_check:
            self._comm.send(
                {'lrd': self._lrd, 'id': self._id, 'k': k, 'type': type}, j, MsgTag.REQUEST)
            self._used_by[j] += self._m
        self._used_by[self._id] = k
        while sum(self._used_by) > self._m:
            for j in indexes_to_check:
                if self._comm.iprobe(source=j, tag=MsgTag.PERMISSION):
                    self._used_by[j] -= 1
        self._cs_state = CsState.IN

    def release_resource(self, k):
        self._cs_state = CsState.OUT
        for j in range(0, self._comm.Get_size()):
            if self._perm_delayed[j] > 0:
                self._comm.send(
                    {'lrd': self._lrd, 'id': self._id, 'k': k, 'type': type}, j, )

