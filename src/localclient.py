#!/usr/bin/env python

from __future__ import with_statement
import socket
import sys
import threading
import signal
from decimal import *
import time

state = [0] * 48
cv = threading.Condition()

class client(threading.Thread):
	
	def __init__(self, ip, port, run_event):
		self.ip = ip
		self.port = port
		self.event = run_event
		threading.Thread.__init__(self)
	
	def processBuffer(self, msg):
		l = msg.split(":")
		self.setAvgs(l[len(l)-2].split(","))
		#print l
	
	def setAvgs(self, msg):
		with cv:
			global avgs
			avgs = msg
			cv.notifyAll()
	
	def run(self):
		while 1:
			try:
				s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Set up TCP socket
				s.connect((self.ip, self.port))
			except socket.error:
				time.sleep(2);
				print "Waiting for input from state Estimator"
			
			while self.event.isSet():
				try:
					msg = s.recv(4096)
					print msg
					self.processBuffer(msg)
				except IOError:
					break
		s.close()

def getState():
	with cv:
		global state
		while state == ([0] * 48):
			cv.wait()
		s = state
		state = map(lambda x: 0, avgs) #clears avgs
		cv.notifyAll()
		return s
	
if __name__ == '__main__':
	
	signal.signal(signal.SIGINT, signal.SIG_DFL) # Press ctrl-C to quit

	client_threads = []
	ip = socket.gethostbyname(socket.gethostname())
	port = 20000
	run_event = threading.Event()
	run_event.set()
	if len(sys.argv) == 1:
		client_threads.append(client(ip, port, run_event))
	else:
		for i in range(1, len(sys.argv)):
			ip, port = sys.argv[i].strip().split(":")
			client_threads.append(client(ip, int(port), run_event))
	for th in client_threads:
		th.start()
		
	try:
		while True:
			a = getState()
			print "LocalClient"
			print '0:%s' % a[0]
			print '1:%s' % a[1]
			print '2:%s' % a[2]
			print '3:%s' % a[3]
			print '4:%s' % a[4]
			print '5:%s' % a[5]
			print '6:%s' % a[6]
			print '7:%s' % a[7]
		
	except KeyboardInterrupt:
		pass
	finally:
		run_event.clear()
