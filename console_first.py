from enums import CsState, MsgTag
import sys
import comm
import commutils
from mpi4py import MPI


def create_comm(m):
    acomm = comm.MPICommAdapter(MPI.COMM_WORLD, logging=True, m=m)
    acomm.open()
    return acomm


def usage():
    print('Usage: mpirun -np 2 python3 {} 4'.format(sys.argv[0]),
          file=sys.stderr)


def main(argv):
    res = argv[1]  # number of resources
    commutils.set_comm(create_comm(res))
    acomm = commutils.comm()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        usage()
    else:
        main(sys.argv)