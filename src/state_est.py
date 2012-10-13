#!/usr/bin/env python

from __future__ import with_statement
import socket
import sys
import threading
import signal
import time
import numpy as np
import decimal
from DataGenerator import DataGenerator
from StateEstimator import StateEstimator
import scipy as sp
import pdb 

avgs = []
state = []
cv = threading.Condition()
comp_state_est_cv = threading.Condition()
globcount=0

RECONNECT_INTERVAL = 2

class client(threading.Thread):	
	def __init__(self, ip, port):
		self.ip = ip
		self.port = port
		self.lines = []
		self.cond = threading.Condition() # lock for lines
		threading.Thread.__init__(self)

	def getLine(self):
		with self.cond:
			while len(self.lines) < 1:
				self.cond.wait()
			self.cond.notifyAll()
			return self.lines.pop(0)
	
	def processLine(self, msg):
		l = msg.strip(':')
		with self.cond:
			self.lines.append(l)
			self.cond.notifyAll()

	def run(self):
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Set up TCP socket
                while True:
                    try:
                        s.connect((self.ip, self.port))
                        break
                    except socket.error:
                        time.sleep(RECONNECT_INTERVAL)
                        print 'datacapture server not ready'
		
		#while self.event.isSet():
		while True:
                        msg = s.recv(1024)

                        if not msg:
                                msg='0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0'
                                s.shutdown(socket.SHUT_RDWR) 
                                s.close()
                                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Set up TCP socket
                                while True: 
                                        print 'trying to reconnect to datacapture server'
                                        try:
                                                s.connect((self.ip, self.port))
                                                print 'datacapture connection restored!'
                                                break
                                        except socket.error:
                                                time.sleep(RECONNECT_INTERVAL)
                                        self.processLine(msg)

			self.processLine(msg)
			
		print "client interrupted"
		s.close()

class serverThread(threading.Thread):
        """maintains connection from local client
        """

	#def __init__(self, run_event):
	def __init__(self):
		#self.event = run_event
		threading.Thread.__init__(self)

	def getState(self):
		with cv:
			global state
			while state == ([0] * 48):
				cv.wait()
			s = state
			print s
			state = map(lambda x: 0, state)
			#cv.notifyAll()
			return s

	def listToStringList(self, avglist):
		return (map(lambda x: str(x), avglist))

	def run(self):
		host_local = socket.gethostbyname(socket.gethostname())
		port_local = 20000
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Set up TCP socket
		s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		s.bind((host_local, port_local)) # bind server to port
		s.listen(1) # 1 pending connection
		client, addr = s.accept() # Get a connection
		print "Got a connection..."
		
		#while self.event.isSet():
		while 1:
			try:
				avgs_str_list = self.listToStringList(self.getState())
				print "==avgs_str_list=="
				print avgs_str_list
				client.sendall(','.join(avgs_str_list) + ":")
			except IOError:
				print "IO Error, exiting"
				break
		print "server interrupted"
		client.close()
		s.close()

#def compavg(clients, run_event):
def compavg(clients):
	signal.signal(signal.SIGINT, signal.SIG_DFL)
	y = np.zeros(shape=(6,8)) # observation matrix
	m = 8
	n = 8
	dg = DataGenerator(n=n, m=m);
	se = StateEstimator(dg.EstimationMatrix)

	while True:
		global globcount
		totals = [0]*8
		averages = [0]*8 # [avg1, avg2, avg3,..., avg8]
		
		for c in clients:
			line = c.getLine().strip().split(",")
			if len(line) != 8:
				line = [0.0, time.time(), 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
			for i in range(len(line)):
				if  i == 1:
					continue
				if i > 1:
					#print line[i]
					totals[i-1] += float(line[i])
				else:
					totals[0] += float(line[0])
		averages = map(lambda x: x / len(clients), totals)
		#with comp_state_est_cv:
		with cv:
			y[globcount,:]=averages
			globcount += 1
			if globcount == 6:
				times = y[:,1].reshape(-1, 1)
				count = 0
				timeno = 0
				observation = sp.zeros((48, 3))
				for t, row in zip(times, y):
					for id, val in enumerate(row):
						if timeno == 1:
							timeno += 1
							continue
						observation[count, :] = sp.array([id, t[0] , val ] ).reshape(1, -1)
						count += 1		
						timeno += 1    			
				xhat = sp.zeros( (6, 8) )
				T = 6
				stepsize = 1
				
				for t in range( 0, T + 1 - stepsize, stepsize ):
					newstate,t0 = se.estimate( observation[t*m:(t+stepsize)*m,:] )	
					xhat[t:(t+stepsize),:] = se.interpolate(newstate,t0,times[t:(t+stepsize),:])
				
				global state					
				state = xhat.reshape(-1).tolist()
				globcount=0
				cv.notifyAll()
				
	print "interrupted"

if __name__ == '__main__':

	client_threads = []
	ip = socket.gethostbyname(socket.gethostname())
	port = 10000
	#run_event = threading.Event()
	#run_event.set()
	if len(sys.argv) == 1:
		#client_threads.append(client(ip, port, run_event))
		client_threads.append(client(ip, port))
		print "Default connection to host=" + ip + " port=" + str(port)
	else:
		print "Starting... host=" + ip + " port=" + str(20000)
		for i in range(1, len(sys.argv)):
			ip, port = sys.argv[i].strip().split(":")
			#client_threads.append(client(ip, int(port), run_event))
			client_threads.append(client(ip, int(port)))
	#st = serverThread(run_event)
	st = serverThread()
	st.start()
	for th in client_threads:
		th.start()
	print "client threads: " + str(client_threads)
	#compavg(client_threads, run_event)
	compavg(client_threads)
