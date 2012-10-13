import socket
import sys
import time
import threading
import signal
from decimal import *
import SocketServer
import ssl
import data_logger
import pdb

data=""
cv=threading.Condition()

class clientThread(threading.Thread):
	
	def __init__(self, ip, port):
		self.ip=ip
		self.port=port
		self.asock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.sock=ssl.wrap_socket(self.asock)
		self.sock.connect((self.ip, self.port)) # connect to server
		threading.Thread.__init__(self)
                self.datalogger = data_logger.DataLogger()
		
	def run(self):
		sum_lat = 1
		num_lines = 1 # keep track of total number of lines for avg. latency
		leftover = ""
		try:
			while True:
				data=self.sock.recv(4096)
				self.datalogger.log(data)
				curr_time=Decimal(str(time.time()*1000))
				print data
				if not data:
					break
				leftover, sum_lat, num_lines = self.process_data(leftover, data, curr_time, sum_lat, num_lines)
		finally:
			avg_lat = Decimal(sum_lat) / Decimal(num_lines)
			print "Average latency = " + str(avg_lat) # in ms
			self.sock.close()

	# Breaks the data into individual lines
	def process_data(self, leftover, data, curr_time, sum_lat, num_lines):
		lines = data.split(':')
		lines[0] = leftover + lines[0] # prepend the leftover from previous reception
		last_full_line=lines[len(lines)-2]
		self.setData(last_full_line)
		for l in lines:
			fields = l.split(',')	
			if len(fields) == 8:
				sum_lat += (curr_time - Decimal(fields[1]))
				num_lines += 1
			else:
				leftover = l

		return leftover, sum_lat, num_lines
	
	def setData(self, line):
		with cv:
			global data
			data = line
			cv.notifyAll()


class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):
	def getData(self):
		with cv:
			global data
			while len(data) == 0:
				cv.wait()
			val = data
			data = ""
			cv.notifyAll()
			return val

	def handle(self):
		while True:
			try:
				line = [self.getData(), ":"]
				#print line
				self.request.sendall(''.join(line))
			except IOError:
				break
	
class serverThread(threading.Thread):

	def __init__(self):
		threading.Thread.__init__(self)

	def getData(self):
		with cv:
			global data
			while len(data) == 0:
				cv.wait()
			val = data
			data = ""
			cv.notifyAll()
			return val
	
	def run(self):
		host_local = socket.gethostbyname(socket.gethostname())
		port_local = 10000
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Set up TCP socket
		s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		s.bind((host_local, port_local)) # bind server to port
		s.listen(1) # 1 pending connection
		client, addr = s.accept() # Get a connection
		print "Got a connection..."
		
		while True:
			try:
				line = [self.getData(), ":"]
				pdb.set_trace();
				client.sendall(''.join(line))
			except IOError:
				break

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
	pass

if __name__ == "__main__":
	IP = socket.gethostbyname(socket.gethostname())
	PORT = 4712
	requires_server = False
	requires_latency = False
	requires_store = False
	if len(sys.argv) == 1:
		print "No IP or port; defaulting to host=" + IP + " port=" + str(PORT)
	else:
		print "Current host=" + IP
		IP=sys.argv[1]
		PORT = sys.argv[2]
		print "Starting... host=" + IP + " port=" + str(PORT)

	ct = clientThread(IP, int(PORT))
	ct.start()
	
	SocketServer.TCPServer.allow_reuse_address = 1
	server = ThreadedTCPServer((socket.gethostbyname(socket.gethostname()), 10000), ThreadedTCPRequestHandler)
	ip, port = server.server_address
	
	server_thread = threading.Thread(target=server.serve_forever)
	server_thread.start()
	
	signal.signal(signal.SIGINT, signal.SIG_DFL) # Press ctrl-C to quit
