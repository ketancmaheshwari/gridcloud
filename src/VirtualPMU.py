from DataGenerator import DataGenerator
from GridMessageProtocol import *
import socket
import time
import argparse
from threading import Thread, Lock, RLock

DEBUG = True
NDEV = 1

class VirtualPMU():

  def __init__(self, host, port, devid=0, ndev=1, worker_data=[]):
    # worker_data : list of ( worker_id, tuple of pmu_ids )
    self.devid,self.ndev = devid,ndev
    self.ip = ip_to_int(socket.inet_aton(socket.gethostbyname(host)))
    self.port = port
    # self.worker_dict = dict( [ (worker_id,set(pmus)) for worker_id,pmus in worker_data ] )
    self.connections = []
    self.lock = Lock()
    self.send_count = 0

  def add_connection(self, c):
    with self.lock:
      self.connections.append(c)

  def send(self, rec):
    pmu_id = rec.devid
    srec = Cmd_SEND(rec).pack()
    dead_connections = []
    with self.lock:
      conn = list(self.connections) # local copy
    sent = 0
    for c in conn: # TODO:  Should do this in parallel instead...
      with c.lock:
        # if c.worker_id in self.worker_dict and pmu_id in self.worker_dict[c.worker_id]:
        if c.worker_id != None:
          try:
            # c.sendall(srec)
            c.async_send(srec)
            sent += 1
          except socket.error as e:
            print e
            dead_connections.append(c)
    # Update connection state information
    if len(dead_connections) > 0:
      with self.lock:
        for dc in dead_connections:
          self.connections.remove(dc)
    if self.send_count % (30*NDEV/8) == 0:
      self.send_count = 0
      print "Sent to %d workers"%sent
    self.send_count += 1

class SendingThread(Thread):

  def __init__(self,pmu,dg,start_time,time_period,time_frequency):
    # pmu: VirtualPMU
    # dg : DataGenerator
    # devid : Device id
    Thread.__init__(self)
    self.daemon = True
    self.pmu,self.dg = pmu,dg
    self.start_time, self.time_period = start_time,time_period
    self.time_frequency = time_frequency
  

  def run(self):
    recs = [PMURecord(devid=self.pmu.devid + k) for k in range(0,self.pmu.ndev,8)]
    # rec = PMURecord(devid=self.pmu.devid)
    
    next_clocktime = self.start_time
    count = 0
    
    while True:
      # print "[%f]: Generator run"%time.time()
      # Generate data
      y = dg.generate(t=next_clocktime,id_range=(self.pmu.devid,self.pmu.devid+self.pmu.ndev))
      y = y.ravel() #flatten y
      ts = long(next_clocktime/TIME_FACTOR)
      for k in range(0,self.pmu.ndev,8):
        recs[k/8].timestamp = ts
        recs[k/8].values = y[k:k+8]
      
      # rec.timestamp = long(next_clocktime/TIME_FACTOR)
      # rec.values = y.ravel()
      
      # wait until it is time to send data
      wait_until(next_clocktime)
      next_clocktime += self.time_period
      
      # Send data
      # print "Sending data"
      for rec in recs:
        self.pmu.send(rec)
      
      if DEBUG or count % self.time_frequency == 0:
        print "[%f]: Data generated for ids %d to %d, time=%d"%(
            time.time(), self.pmu.devid, self.pmu.devid+self.pmu.ndev, ts)
      
      count += 1

class PMUConnectionHandler(Thread):
  def __init__(self,pmu,conn,addr):
    # s : Connected stream
    Thread.__init__(self)
    self.daemon = True
    self.s,self.addr = conn,addr
    self.pmu = pmu
    self.slock = RLock() # sending lock
    self.lock = Lock() # for updating state information
    self.pmu.add_connection(self)
    self.worker_id = None
    self.messenger = MessageSender(self)
    self.messenger.start()
  
  def sendall(self,msg):
    with self.slock:
      self.s.sendall(msg)
  def close(self):
    with self.slock:
      self.s.close()
  def async_send(self,msg):
    self.messenger.sendall(msg)
    
  def run(self):
    try:
      while True:
        cmd = self.s.recv(4)
        if cmd == "PING":
          c = Cmd_PING().unpack_stream(cmd, self.s)
          if DEBUG: print c
          self.async_send( Cmd_PONG( c.ts ).pack() )
        elif cmd == "GETR":
          c = Cmd_GETR().unpack_stream(cmd, self.s)
          print c
          """
          # Used to check if it agrees with configuration,
          # but this is not needed
          with self.slock:
            if c.worker_id not in self.pmu.worker_dict:
              s.sendall(Cmd_RGET(resp="NOPE").pack())
            else:
              with self.lock:
                self.worker_id = c.worker_id
              s.sendall(Cmd_RGET(resp="OKAY").pack())
          """
          with self.lock:
            self.worker_id = c.worker_id
          """
        # RPUB not used
        elif cmd == "RPUB":
          c = Cmd_RPUB().unpack_stream(cmd, self.s)
          if len(c.data) == 0:
            with self.slock:
              s.sendall( Cmd_PUBP( self.pmu.ip, self.pmu.port, [self.pmu.devid] ).pack() )
          else:
            with self.pmu.lock:
              for worker_id, pmus in c.data:
                self.pmu.worker_dict[worker_id] = set(pmus)
          """
        else:
          print "ERROR: Command %s not implemented"%cmd
    except Exception as e:
      print e

def wait_until(t):
  # Sleeps the thread until specified time t +- 1 millisecond
  while True:
    diff = t - time.time()
    # print "%f secs to %f, sleeping"%(diff,t)
    if diff <= 1e-3:
      break
    else:
      time.sleep(diff)

class SocketAcceptThread(Thread):
  def __init__(self,s,pmu):
    Thread.__init__(self)
    self.daemon = True
    self.pmu = pmu
    self.s = s
  def run(self):
    while True:
      conn,addr = self.s.accept()
      print "Connection from ", addr
      PMUConnectionHandler(self.pmu,StreamWrapper(conn),addr).start()

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Connects to GMS (HOST,PORT) to upload measurements.')
  parser.add_argument('--gms_host', metavar='GMS_HOST', dest='GMS_HOST', action='store', default='127.0.0.1',help='GMS ip / domain name to connect to')
  parser.add_argument('--gms_port', metavar='GMS_PORT', dest='GMS_PORT', action='store', type=int, choices=range(0,65535), default=6000,help='GMS PORT to connect to')
  parser.add_argument('--host', metavar='HOST', dest='HOST', action='store', default='127.0.0.1',help='HOST to start server on')
  parser.add_argument('--port', metavar='PORT', dest='PORT', action='store', type=int, choices=range(0,65535), default=8001,help='PORT to start server on')
  parser.add_argument('-n', metavar='N', dest='n', action='store', type=int, default=1,help='number of dimensions in state vector')
  parser.add_argument('-m', metavar='M', dest='m', action='store', type=int, default=1,help='number of dimensions in measurement vector ( >= n )')
  parser.add_argument('-e', metavar='Err_Std', dest='error_std', action='store', type=float, default=1e-2,help='Standard deviation of error in measurement')
  parser.add_argument('-r', metavar='random_seed', dest='randseed', action='store', type=int, default=1023,help='Random seed used to generate data')
  parser.add_argument('-s', metavar='start_time', dest='start_time', action='store', type=float, default=time.time(),help='Starting time to run state estimation')
  parser.add_argument('-f', metavar='time_frequency', dest='time_frequency', action='store', type=float, default=30.0,help='Frequency of state estimation run (Hz)')
  parser.add_argument('-i', metavar='device_id', dest='devid', action='store', type=int, default=0,help='Device id of this virtual PMU')
  parser.add_argument('-u', metavar='num_devices', dest='ndev', action='store', type=int, default=1,help='Number of devices that this should simulate')
  parser.add_argument('--release', dest='release', action='store_true', help='Turn off debug prints')
  args = parser.parse_args()
  args.time_period = 1.0 / args.time_frequency
  args.n *= 8
  args.m *= 8
  args.devid *= 8
  args.ndev *= 8
  NDEV = args.ndev
  print args
  
  DEBUG = not args.release
  if args.m < args.n:
    print "ERROR: M < N"
    exit(0)
  elif args.devid < 0 or args.devid+args.ndev > args.m:
    print "ERROR: device_id < 0 or >= M"
    exit(0)
  
  # args.HOST = '10.0.2.2'       # The remote host
  # args.HOST = '127.0.0.1'       # The remote host (localhost)
  # args.HOST = 'testingtesting123.cloudapp.net'       # The remote host on Azure
  # args.PORT = 8000         # The port used on server
  
  ''' Start listening on HOST,PORT '''
  #s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  #s.bind(('', args.PORT))
  #s.listen(5)
  
  # Initialize data generator
  dg = DataGenerator(n=args.n, m=args.m, error_std=args.error_std, randseed=args.randseed)
  pmu = VirtualPMU(args.HOST, args.PORT, devid=args.devid, ndev=args.ndev, worker_data=[])
  
  ''' Connect to the GMS server'''
  # Connection to GMS
  #gms_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  #gms_s.connect((args.GMS_HOST, args.GMS_PORT))
  # gms_s.settimeout(3.0)
  
  # Start SendingThread
  st = SendingThread(pmu, dg, args.start_time, args.time_period, args.time_frequency)
  st.start()
  print Cmd_PUBP( pmu.ip, pmu.port, range(pmu.devid,pmu.devid+pmu.ndev,8) ).pack()
  
  # Ask GMS to publish measurements
  #gms_handler = PMUConnectionHandler(pmu, StreamWrapper(gms_s), gms_s.getsockname())
  #gms_handler.start()
  #with gms_handler.slock:
    # for low in range(pmu.devid, pmu.devid+pmu.ndev, 20*8):
      # high = min( low+20*8, pmu.devid+pmu.ndev )
      # gms_handler.s.sendall( Cmd_PUBP( pmu.ip, pmu.port, range(low,high,8) ).pack() )
      # time.sleep(0.5)
    
    #gms_handler.s.sendall( Cmd_PUBP( pmu.ip, pmu.port, range(pmu.devid,pmu.devid+pmu.ndev,8) ).pack() )
   # print Cmd_PUBP( pmu.ip, pmu.port, range(pmu.devid,pmu.devid+pmu.ndev,8) ).pack()
    # gms_handler.s.sendall( Cmd_PUBP( pmu.ip, pmu.port, [pmu.devid] ).pack() )
  
  # Serve forever
  #SocketAcceptThread(s,pmu).start()
  
  try:
    while True:
      time.sleep(1000.0)
  except KeyboardInterrupt as e:
    print e
    exit(0)
  
