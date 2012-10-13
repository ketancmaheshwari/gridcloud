#!/usr/bin/env python

from __future__ import with_statement
import socket
import SocketServer
import threading
from OpenSSL import SSL

import sys
import signal
import random
import time

import pdb
from DataGenerator import DataGenerator

data = () # global data to send to clients
dg=DataGenerator(n=1, m=8, randseed=1024) # data generation engine

class ThreadedTCPRequestHandler(SocketServer.BaseRequestHandler):
	def setup(self):
		"""setup is called to set up the new client connection."""
		self.connection_lock = threading.Lock()
		self.write_lock = threading.Lock()
		self.connected = True

	def finish(self):
		"""finish is called if the connection to the client is closed."""
		with self.connection_lock:
			SocketServer.BaseRequestHandler.finish(self)
			self.connected = False

	def handle(self):
		while True:
			#msg = [random.random() for i in range(7)]
			data=dg.generate(t=time.time(), id_range=(0,7))
			msg=[item for sublist in data for item in sublist]
			msg.insert(1,time.time()*1000)
			#print "==="
			#print msg
			#print "==="
			try:
				self.request.sendall(str(msg).strip('[]')+':')
				time.sleep(1/30.0)
			except IOError:
				break
			except SSL.SysCallError:
				break

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
#	pass
	def __init__(self, server_address, RequestHandlerClass, bind_and_activate=True):
		SocketServer.BaseServer.__init__(self, server_address, RequestHandlerClass)
		ctx = SSL.Context(SSL.SSLv23_METHOD)
		cert = '/home/ketan/demo/multi_threaded/cert.pem'
		key = '/home/ketan/demo/multi_threaded/key_rsa'
		ctx.use_privatekey_file(key)
		ctx.use_certificate_file(cert)
		self.socket = SSL.Connection(ctx, socket.socket(self.address_family, self.socket_type))

		if bind_and_activate:
			self.server_bind()
			self.server_activate()
			
			
	def shutdown_request(self,request):
		try:
			request.shutdown()		
		except SSL.SysCallError:
			pass
                except SSL.Error:
                        pass


if __name__ == "__main__":
	if len(sys.argv) == 1:
		HOST = socket.gethostbyname(socket.gethostname())
		PORT = 4712
		print "No IP or port entered; defaulting to "+ HOST +" "+ str(PORT)
	else:
		HOST = sys.argv[1]
		PORT = int(sys.argv[2])

	SocketServer.TCPServer.allow_reuse_address = 1
	server = ThreadedTCPServer((HOST, PORT), ThreadedTCPRequestHandler)
	ip, port = server.server_address

	# Start a thread with the server -- that thread will then start one
	# more thread for each request
	server_thread = threading.Thread(target=server.serve_forever)
	# Exit the server thread when the main thread terminates
	server_thread.daemon = False
	server_thread.start()
	print "Server loop running ...",
	
	signal.signal(signal.SIGINT, signal.SIG_DFL) # Press ctrl-C to quit
