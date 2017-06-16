import sys
import comm_general as comm
import commutils
from mpi4py import MPI
from time import sleep


def create_comm(res):
    acomm = comm.MPICommAdapter(MPI.COMM_WORLD, logging=False, res=res, info_logging=True)
    acomm.open()
    return acomm


def usage():
    print('Usage: mpirun -np 2 python3.6 {}',
          file=sys.stderr)


def main(argv):
    try:
        commutils.set_comm(create_comm([(0, 5), (1, 4), (2, 4)]))
        acomm = commutils.comm()
        if commutils.procid() == 0:
            acomm.acquire_resource([(0, 3), (1, 2)])
            sleep(5)
            acomm.release_resource([(0, 3), (1, 2)])
        if commutils.procid() == 1:
            sleep(2)
            acomm.acquire_resource([(0, 3), (1, 2)])
            sleep(4)
            acomm.release_resource([(0, 3), (1, 2)])
        if commutils.procid() == 2:
            acomm.acquire_resource([(2, 4)])
            sleep(4)
            acomm.release_resource([(2, 4)])
    except e:
        print(e)


if __name__ == "__main__":
    if len(sys.argv) != 1:
        usage()
    else:
        main(sys.argv)
