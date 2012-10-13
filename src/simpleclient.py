import socket
import sys
import re
import time
import threading
import signal
from decimal import *

data=""
cv=threading.Condition()

class clientThread(threading.Thread):
	
	def __init__(self, ip, port):
		self.ip=ip
		self.port=port
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.sock.connect((self.ip, self.port)) # connect to server
		threading.Thread.__init__(self)
		
	def run(self):
		sum_lat = 1
		num_lines = 1 # keep track of total number of lines for avg. latency
		leftover = ""
		try:
			while True:
				dat = self.sock.recv(2)
				print str(dat)
				if not dat:
					break
		finally:
			self.sock.close()


if __name__ == "__main__":
	IP = socket.gethostbyname(socket.gethostname())
	PORT = 4712
	if len(sys.argv) == 1:
		print "No IP or port; defaulting to host=" + IP + " port=" + str(PORT)
	else:
		print "Current host=" + IP
		IP=sys.argv[1]
		PORT=int(sys.argv[2])
		print "Starting... host=" + IP + " port=" + str(PORT)

	#st = serverThread()
	ct = clientThread(IP, PORT)
	#st.start()
	ct.start()
	signal.signal(signal.SIGINT, signal.SIG_DFL) # Press ctrl-C to quit
