import sys
import time
import json
from mpi4py import MPI


sys.path.append('../src/')
import commutils
import comm


def create_comm():
    return MPI.COMM_WORLD


def main():
    commutils.set_comm(create_comm())
    comm = commutils.comm()

    if commutils.procid() == 0:
        while True:
            msg = commutils.time()
            commutils.pt('Sending msg "{}"'.format(msg))
            comm.send(msg, dest=1, tag=1)
            time.sleep(0.5)

    elif commutils.procid() == 1:
        while True:
            msg = comm.recv(source=0, tag=1)
            commutils.pt('Received msg "{}"'.format(msg))
            time.sleep(0.5)


def usage():
    print('Usage: mpirun -np 2 python3 {} [1|2]'.format(sys.argv[0]),
          file=sys.stderr)


if __name__ == "__main__":
    if len(sys.argv) != 1:
        usage()
    else:
        main()