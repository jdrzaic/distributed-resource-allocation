import sys
import time
from mpi4py import MPI


import commutils
import comm


def create_comm():
    acomm = comm.MPICommAdapter(MPI.COMM_WORLD, logging=True, m=2)
    acomm.open()
    return acomm

def main():
    commutils.set_comm(create_comm())
    comm = commutils.comm()

    if commutils.procid() == 0:
        while True:
            msg = commutils.time()
            comm.send(msg, dest=1, tag=1)
            time.sleep(0.5)

    elif commutils.procid() == 1:
        while True:
            comm.recv(source=0, tag=1)
            time.sleep(0.5)


def usage():
    print('Usage: mpirun -np 2 python3 {} [1|2]'.format(sys.argv[0]),
          file=sys.stderr)


if __name__ == "__main__":
    if len(sys.argv) != 1:
        usage()
    else:
        main()