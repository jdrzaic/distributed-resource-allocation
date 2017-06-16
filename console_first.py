import sys
import comm
import commutils
from mpi4py import MPI
from time import sleep
import random


def create_comm(m):
    acomm = comm.MPICommAdapter(MPI.COMM_WORLD, logging=True, m=m, info_logging=True)
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
        sleep(1)
        acomm.acquire_resource(2)
        sleep(6)
        acomm.release_resource(2)
    elif commutils.procid() == 3:
        sleep(2)
        acomm.acquire_resource(2)
        sleep(4)
        acomm.release_resource(2)
    elif commutils.procid() == 0:
        acomm.acquire_resource(1)
        sleep(3)
        acomm.release_resource(1)
    else:
        sleep(commutils.procid())
        num_res = random.randint(1,res-1)
        acomm.acquire_resource(num_res)
        sleep(random.randint(1,4))
        acomm.release_resource(num_res)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        usage()
    else:
        main(sys.argv)
