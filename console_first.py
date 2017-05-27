import sys
import comm
import commutils
from mpi4py import MPI
from time import sleep


def create_comm(m):
    acomm = comm.MPICommAdapter(MPI.COMM_WORLD, logging=True, m=m)
    acomm.open()
    return acomm


def usage():
    print('Usage: mpirun -np 2 python3 {} 4'.format(sys.argv[0]),
          file=sys.stderr)


def main(argv):
    res = int(argv[1])  # number of resources
    commutils.set_comm(create_comm(res))
    acomm = commutils.comm()
    if commutils.procid() == 1:
        acomm.acquire_resource(res-1)
        sleep(5)
        acomm.release_resource(res-1)
    if commutils.procid() == 3:
        sleep(2)
        acomm.acquire_resource(2)
        sleep(4)
        acomm.release_resource(2)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        usage()
    else:
        main(sys.argv)