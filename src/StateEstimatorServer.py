from StateEstimator import StateEstimator
from DataGenerator import DataGenerator
from GridMessageProtocol import *

import socket
# import cProfile
import time,sys
import numpy as np
# import matplotlib.pyplot as plt
import argparse
from threading import Thread, Lock, RLock
from collections import defaultdict, deque
from Queue import Queue # Thread-safe queue



ESTIMATED_STATE_BASE_DEV_ID = 100000
DEBUG = True
STARTTIME = 0

''' Globally shared datastructures '''
EXPECTED_RECS = 0
recs = deque([],EXPECTED_RECS)
recs_lock = Lock()
recs_last_timestamps = defaultdict(int)

class Uploader(Thread):
  def __init__(self, state_estimation_server, work_queue):
    Thread.__init__(self)
    self.daemon = True
    self.se = state_estimation_server
    self.q = work_queue

  def send(self, conn, devid, msg):
    dcs = []
    for c in conn:
      if c.worker_id != None and devid in c.states:
        try:
          # c.sendall( msg )
          c.async_send( msg )
        except socket.error as e:
          print "DEAD:",e
          self.se.del_connection(c)
          dcs.append(c)
    return dcs
  
  def run(self):
    dead_connections = []
    while True:
      xhat,times = self.q.get()
      local_recs = PMURecord.from_array(times,ESTIMATED_STATE_BASE_DEV_ID,xhat)
      c = Cmd_SEND()
      try:
        with self.se.lock:
          # Remove dead connections from state
          if len(dead_connections) > 0:
            for dc in dead_connections:
              self.se.del_connection(dc)
            dead_connections = []
          conn = list(self.se.connections)
        if len(conn) == 0:
          continue
        for r in local_recs:
          c.rec = r
          if DEBUG: 
            print "OUT SEND",c.rec.timestamp,c.rec.devid
            if c.rec.timestamp > 1337328714593547L:
              print "Timestamp too big, crash!"
              sys.stdout.flush()
              exit(-1)
          dcs = self.send( conn, c.rec.devid, c.pack() )
          # remove dead connections
          if len(dcs) > 0:
            dead_connections.extend(dcs)
            for dc in dcs:
              conn.remove(dc)
      except Exception as e:
        print e
      self.q.task_done()


def wait_until(t):
  # Sleeps the thread until specified time t +- 1 millisecond
  while True:
    diff = t - time.time()
    if diff <= 1e-3:
      break
    else:
      # print "%f secs to %f, sleeping"%(diff,t)
      time.sleep(diff)
    
class StateEstimationServer():
  
  def __init__(self, host, port, pmu_list, state_list):
    self.ip = ip_to_int(socket.inet_aton(socket.gethostbyname(host)))
    self.port = port
    self.pmus = pmu_list
    self.states = state_list
    self.lock = RLock()
    self.worker_pmus_dict = dict()
    self.worker_states_dict = dict()
    self.connections = set()

  def add_connection(self, c):
    with self.lock:
      self.connections.add(c)
  
  def del_connection(self, c):
    with self.lock:
      try:
        self.connections.remove(c)
        print "DELETED CONNECTION", c
      except ValueError as e:
        # connection already removed
        pass
  
  def send(self, devid, msg):
    # Raises socket.error exception
    # Currently not used.
    with self.lock:
      conn = list(self.connections)
    for c in conn:
      if c.worker_id != None and devid in c.states:
        with c.slock:
          c.s.sendall( msg )
          
class StateEstimationConnectionHandler(Thread):
  def __init__(self, conn, addr, state_estimation_server):
    Thread.__init__(self)
    self.daemon = True
    self.s = conn
    self.addr = addr
    self.worker_id = None
    self.pmus = set()
    self.states = set()
    self.slock = RLock()
    self.se = state_estimation_server
    self.se.add_connection(self)
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
  
  def send_subscription(self):
    # Raises socket.error exception
    with self.slock:
      self.s.sendall( Cmd_SUBP(self.se.ip, self.se.port, self.se.pmus).pack() )
  
  def send_publication(self):
    # Raises socket.error exception
    with self.slock:
      self.s.sendall( Cmd_PUBP(self.se.ip, self.se.port, self.se.states).pack() )
    
  def run(self):
    global recs, recs_lock, recs_last_timestamps
    try:
      while True:
        cmd = self.s.recv(4)
        if cmd == "PING":
          c = Cmd_PING().unpack_stream(cmd, self.s)
          if DEBUG: print c
          self.async_send( Cmd_PONG( c.ts ).pack() )
        elif cmd == "RLST":
          c = Cmd_RLST().unpack_stream(cmd, self.s)
          print c
          # TODO:  Don't know how to use this information yet
          """
        elif cmd == "RSUB":
          c = Cmd_RSUB().unpack_stream(cmd, self.s)
          desired_pmus = set(self.se.pmus)
          received_pmus = set([ id for id in pmus for worker_id,pmus in c.data])
          if desired_pmus != received_pmus:
            # Reply with SUBP
            self.send_subscription()
          else:
            # Update mapping for workers to pmus
            with self.se.lock:
              for worker_id, pmus in c.data:
                self.se.worker_pmus_dict[worker_id] = set(pmus)
        elif cmd == "RPUB":
          c = Cmd_RPUB().unpack_stream(cmd, self.s)
          desired_states = set(self.se.states)
          received_states = set([ id for id in states for worker_id,states in c.data])
          if desired_states != received_states:
            # Reply with PUBP
            self.send_publication()
          else:
            # Update mapping for workers to states
            with self.se.lock:
              for worker_id, states in c.data:
                self.se.worker_states_dict[worker_id] = set(states)
          """
        elif cmd == "PUTR":
          c = Cmd_PUTR().unpack_stream(cmd, self.s)
          print c
          if DEBUG: print "RECV PMU LIST:",c.pmus
          if DEBUG: print "WANT PMU LIST:",self.se.pmus
          """
          with self.se.lock:
            if c.worker_id in self.se.worker_pmus_dict and set(c.pmus) == self.se.worker_pmus_dict[c.worker_id]:
              self.pmus = set(c.pmus)
              self.worker_id = c.worker_id
              resp = "OKAY"
            else:
              resp = "NOPE"
          with self.slock:
            self.s.sendall( Cmd_RPUT(resp=resp).pack() )
          """
          self.pmus = set(c.pmus) & set(self.se.pmus)
          self.worker_id = c.worker_id
        elif cmd == "GETR":
          c = Cmd_GETR().unpack_stream(cmd, self.s)
          print c
          """
          with self.se.lock:
            if c.worker_id in self.se.worker_states_dict and set(c.pmus) == self.se.worker_states_dict[c.worker_id]:
              self.states = set(c.pmus)
              self.worker_id = c.worker_id
              resp = "OKAY"
            else:
              resp = "NOPE"
          with self.slock:
            self.s.sendall( Cmd_RGET(resp=resp).pack() )
          """
          self.states = set(c.pmus) & set(self.se.states)
          self.worker_id = c.worker_id
        elif cmd == "SEND":
          c = Cmd_SEND().unpack_stream(cmd, self.s)
          if DEBUG: 
            print "SEND",c.rec.timestamp,c.rec.devid,self.pmus
            if c.rec.timestamp > 1337328714593547L:
              print "Timestamp too big, crash!"
              sys.stdout.flush()
              exit(-1)
          if self.worker_id != None and c.rec.devid in self.pmus:
            with recs_lock:
              if recs_last_timestamps[c.rec.devid] < c.rec.timestamp:
                recs_last_timestamps[c.rec.devid] = c.rec.timestamp
                # recs[(c.rec.devid,c.rec.timestamp)] = c.rec
                recs.append(c.rec)
    except Exception as e:
      self.se.del_connection(self)
      print e

class StateEstimatorThread(Thread):
  def __init__(self, state_estimation_server, start_time, time_period, work_queue):
    Thread.__init__(self)
    self.daemon = True
    self.se = state_estimation_server
    self.start_time = start_time
    self.time_period = time_period
    self.queue = work_queue
    
  def run(self):
    global recs, recs_lock
    # Wait until the pre-designated start time
    last_clocktime = args.start_time
    wait_until(last_clocktime)
    
    while True:
      print "[%f]: SE run"%(time.time() - STARTTIME*TIME_FACTOR),
      # print "with %d workers."%(len(self.se.connections)-1)
      # getr.tlow = last_timestamp
      # s.sendall(getr.pack())
      # s.flush()
      # recs = getr.interpret_response(s)
      with recs_lock:
        local_recs = recs
        # recs = dict()
        recs = deque([],EXPECTED_RECS)
      
      print "%d records received, max_used = %d"%(len(local_recs), EXPECTED_RECS)
      if len(local_recs) > 0:
        # values = sorted(local_recs.itervalues(), key=lambda r: -r.timestamp)
        # values = values[:EXPECTED_RECS]
        values = local_recs
        times_set = set( [r.timestamp for r in values] )
        last_timestamp = max(times_set)+1
        # times_long = np.array(times_set, dtype=int64)
        times = np.array( [t*TIME_FACTOR for t in times_set] )
        times.shape = (times.shape[0],1)
        observation_matrix = np.vstack( [r.to_observation_matrix() for r in values] )
        
        newstate,t0 = se.estimate( observation_matrix )
        xhat = se.interpolate(newstate,t0,times)
        print "State estimated for times in [%d, %d]"%(min(times_set)-STARTTIME,max(times_set)-STARTTIME)
        self.queue.put( (xhat,times) )
        
      # wait until it is time to state estimation again
      while time.time() > last_clocktime:
        last_clocktime += self.time_period
      wait_until(last_clocktime)

class SocketAcceptThread(Thread):
  def __init__(self,s,state_estimation_server):
    Thread.__init__(self)
    self.daemon = True
    self.state_estimation_server = state_estimation_server
    self.s = s
  def run(self):
    while True:
      conn,addr = self.s.accept()
      print "Connection from ", addr
      StateEstimationConnectionHandler(StreamWrapper(conn), addr, self.state_estimation_server).start()

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Listens to (HOST,PORT) and runs state estimation.')
  parser.add_argument('--gms_host', metavar='HOST', dest='GMS_HOST', action='store',
             default='127.0.0.1',help='GMS ip / domain name to connect to')
  parser.add_argument('--gms_port', metavar='PORT', dest='GMS_PORT', action='store', type=int, choices=range(0,65535),
             default=6000,help='GMS PORT to connect to')
  parser.add_argument('--host', metavar='HOST', dest='HOST', action='store',
             default='127.0.0.1',help='HOST ip / domain name that PMUs connect to')
  parser.add_argument('--port', metavar='PORT', dest='PORT', action='store', type=int, choices=range(0,65535),
             default=8004,help='PORT that PMUs connect to')
  parser.add_argument('-n', metavar='N', dest='n', action='store', type=int,
             default=1,help='number of dimensions in state vector')
  parser.add_argument('-m', metavar='M', dest='m', action='store', type=int,
             default=1,help='number of dimensions in measurement vector ( >= n )')
  parser.add_argument('-e', metavar='Err_Std', dest='error_std', action='store', type=float,
             default=1e-2,help='Standard deviation of error in measurement')
  parser.add_argument('-r', metavar='random_seed', dest='randseed', action='store', type=int,
             default=1023,help='Random seed used to generate data')
  parser.add_argument('-s', metavar='start_time', dest='start_time', action='store', type=float,
             default=time.time(),help='Starting time to run state estimation')
  parser.add_argument('-f', metavar='time_frequency', dest='time_frequency', action='store', type=float,
             default=5.0,help='Frequency of state estimation run (Hz)')
  parser.add_argument('--release', dest='release', action='store_true', help='Turn off debug prints')
  args = parser.parse_args()
  args.time_period = 1.0 / args.time_frequency
  args.n *= 8
  args.m *= 8
  EXPECTED_RECS = int(2 * args.m * 30 / args.time_frequency / 8)
  recs = deque([],EXPECTED_RECS)
  STARTTIME = long( args.start_time / TIME_FACTOR )
  
  DEBUG = not args.release
  if args.m < args.n:
    print "ERROR: M < N"
    exit(0)
  print args
  
  pmu_list = range(0, args.m, 8)
  state_list = range(ESTIMATED_STATE_BASE_DEV_ID, ESTIMATED_STATE_BASE_DEV_ID+args.n, 8)
  
  ''' Connect to the Isis server to query sensor data '''
  # args.HOST = '10.0.2.2'       # The remote host
  # args.HOST = '127.0.0.1'       # The remote host (localhost)
  # args.HOST = 'testingtesting123.cloudapp.net'       # The remote host on Azure
  # args.PORT = 8000         # The port used on server
  
  # Set up server
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  s.bind(('', args.PORT))
  s.listen(5)
  # s.settimeout(3.0)
  state_estimation_server = StateEstimationServer( args.HOST, args.PORT, pmu_list, state_list )
  
  # Initialize data generator and state estimator
  dg = DataGenerator(n=args.n, m=args.m, error_std=args.error_std, randseed=args.randseed)
  se = StateEstimator(dg.EstimationMatrix)
  
  # Start uploading thread
  last_timestamp = -1
  work_queue = Queue()
  uploader = Uploader(state_estimation_server,work_queue)
  uploader.start()
  
  # Start State Estimator Thread
  se_thread = StateEstimatorThread(state_estimation_server, args.start_time, args.time_period, work_queue)
  se_thread.start()
  
  ''' Connect to GMS '''
  gms_s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  gms_s.connect((args.GMS_HOST, args.GMS_PORT))
  # s2.settimeout(3.0)
  gms_handler = StateEstimationConnectionHandler( StreamWrapper(gms_s), gms_s.getsockname(), state_estimation_server)
  gms_handler.start()
  gms_handler.send_subscription()
  gms_handler.send_publication()
  
  SocketAcceptThread(s, state_estimation_server).start()
  
  try:
    while True:
      time.sleep(1000.0)
  except KeyboardInterrupt as e:
    print e
    s.close()
    exit(0)

  
