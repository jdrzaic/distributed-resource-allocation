import time as tm
from mpi4py import MPI

_start_time = tm.time()


_comm = MPI.COMM_WORLD


_PRINT_PREFIX = '\033[0m[   ]\033[34m[{time:6.0f}ms]\033[32m[ID:{id:3d}]: \033[0;1m'
_PRINT_POSTFIX = '\033[0m'
_LOG_PREFIX  = '\033[0m[LOG]\033[34m[{time:6.0f}ms]\033[32m[ID:{id:3d}]: \033[0m'
_LOG_POSTFIX = ''

def set_comm(comm):
    """ Set communicator used to obtain process IDs.

    Defaults to `mpi4py.MPI.COMM_WORLD`."""
    global _comm
    _comm = comm


def comm():
    """ Return the MPI communcator used. """
    return _comm


def time():
    """ Return time elapsed from starting the programm. """
    return tm.time() - _start_time


def procid():
    """ Return the process ID of the current process. """
    return _comm.Get_rank()


def numproc():
    """ Return the number of processes in the communicator. """
    return _comm.Get_size()


def pt(*args, **kwargs):
    """ Print data decorated with current time and process ID. """
    id = kwargs.pop('comm', _comm).Get_rank()
    print(_PRINT_PREFIX.format(time=time()*1000, id=id), end='')
    print(*args, **kwargs)
    print(_PRINT_POSTFIX, end='')


def log(*args, **kwargs):
    """ Log data decorated with current time and process ID. """
    id = kwargs.pop('comm', _comm).Get_rank()
    print(_LOG_PREFIX.format(time=time()*1000, id=id), end='')
    print(*args, **kwargs)
    print(_LOG_POSTFIX, end='')


if __name__ == "__main__":
    pt("Hello!")
