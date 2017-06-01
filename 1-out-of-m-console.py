import sys
from mpi4py import MPI
from time import sleep
from threading import Thread, Lock
from enums import CsState, REQUEST, PERMISSION


class Process():
	def __init__(self, comm, logging=False, m=1, info_logging=False):
		self._comm = comm
		self._info_logging = info_logging
		self._numer_of_processes = comm.Get_size()
		self._m = m
		self._id = comm.Get_rank()
		self._wait_perm = [0] * self._numer_of_processes
		self._perm_delayed = [0] * self._numer_of_processes
		self._clock = 0
		self._lrd = 0
		self._cs_state = CsState.OUT
		self._nb_perm = 0

		self._log_thread = Thread(target=self._log)
		self._communicate_thread = Thread(target=self._communicate)
		self._lock = Lock()

	def start(self):
		print(self._id, " started...")
		self._communicate_thread.start()
		if self._info_logging:
			self._log_thread.start()

	def _log(self):
		while True:
			if self._cs_state == CsState.IN:
				print('Working...', self._id)
			elif self._cs_state == CsState.TRYING:
				print('Trying to get resource...', self._id)
			else:
				print('Resting...')
	        sleep(1)


	def _communicate(self):
		while True:
			if self._comm.Iprobe(source=MPI.ANY_SOURCE, tag=PERMISSION):
				print(self._id, ": Got permission")
				with self._lock:
					permission = comm.recv(source=MPI.ANY_SOURCE, tag=PERMISSION)
					self._wait_perm[permission['id']] -= permission['count']
					if (self._cs_state == CsState.TRYING and self._wait_perm[permission['id']] == 0):
						self._nb_perm += 1

        	if self._comm.Iprobe(source=MPI.ANY_SOURCE, tag=REQUEST):
    			print(self._id, ": Got request")
        		with self._lock:
        			request = comm.recv(source=MPI.ANY_SOURCE, tag=REQUEST)
        			self._clock = max(clock_i, request['lrd_i'])
        			prio = self._cs_state == CsState.IN or (self._cs_state == CsState.TRYING and (self._lrd < request['lrd'] or lrd == request['lrd'] and self._id < request['id']))

        			if prio:
        				self._perm_delayed[request['id']] += 1
        			else:
        				permission = {'id': self._id, 'count': 1}
        				self._comm.isend(permission, dest=request['id'], TAG=PERMISSION)


	def release_resource(self):
		with self._lock:
			print(self._id, " :Release_resource..")
			self._cs_state = CsState.OUT
			for i in range(self._numer_of_processes):
				if self._perm_delayed[i] is not 0:
					permission = {'id': self._id, 'count': 1}
					self._comm.isend(permission, dest=i, TAG=PERMISSION)
					self._perm_delayed[i] = 0


	def acquire_resource(self):
		with self._lock:
			print(self._id, ": Acquire_resource...")
			self._cs_state = CsState.TRYING
			self._lrd = self._clock + 1
			self._nb_perm = 0
			indexes_to_check = [ind for ind in range(self._numer_of_processes) if ind != self._id]
			for i in indexes_to_check:
				data = {'id': self._id, 'lrd': self._lrd}
				request = self._comm.isend(data, dest=i, tag=REQUEST)
				self._wait_perm[i] += 1

		while True:
			with self._lock:
				if self._nb_perm >= self._numer_of_processes - self._m:
					self._cs_state = CsState.IN
					print(self._id, ": Got resource...")
					break

	def get_id(self):
		return self._id


def main():
	process = Process(MPI.COMM_WORLD, m=3)
	process.start()

	if process.get_id()%3 == 0:
		sleep(1)
		process.acquire_resource()
		sleep(3)
		process.release_resource()
		sleep(1)
		process.acquire_resource()
		sleep(2)
		process.release_resource()

	if process.get_id()%3 == 1:
		sleep(3)
		process.acquire_resource()
		sleep(1)
		process.release_resource()
		sleep(5)
		process.acquire_resource()
		sleep(5)
		process.release_resource()		

	if process.get_id()%3 == 2:
		sleep(1)
		process.acquire_resource()
		sleep(2)
		process.release_resource()
		sleep(2)
		process.acquire_resource()
		sleep(1)
		process.release_resource()


if __name__ == "__main__":
    main()
