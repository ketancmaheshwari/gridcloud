import struct
import random
import numpy as np
import time, socket
from threading import Thread, Lock
from Queue import Queue # Thread-safe queue
from sys import stdout
# from threading import Lock

ENDIAN = "<" # "<" for little-endian, ">" for big-endian, "=" for native
TIME_FACTOR = 1.0e-6 # Timestamps treated as microseconds
# DEBUG = True
DEBUG = False



class StreamWrapper():
  def __init__(self, s):
    self.s = s
    self.s.settimeout(10.0)
  def recv(self, bufsize):
    # print "RECV:"
    b = ""
    while len(b) < bufsize:
      b += self.s.recv( bufsize - len(b) )
      # m = self.s.recv( bufsize - len(b) )
      # stdout.write(repr(m))
      # stdout.flush()
      # b += m
    return b
  def sendall(self, msg):
    return self.s.sendall(msg)
  def close(self):
    return self.s.close()
  def connect(self, addr):
    self.s.connect(addr)
    
    
class MessageSender(Thread):
  def __init__(self, s):
    Thread.__init__(self)
    self.daemon = True
    self.s = s
    self.q = Queue()
    self.saved_exception = None
  def sendall(self,msg):
    if self.q == None:
      if self.saved_exception != None:
        raise self.saved_exception
      else:
        return
    self.q.put(msg)
  def clear_exception(self):
    self.saved_exception = None
  def exit(self):
    if self.q != None:
      self.q.put(SystemExit)
  def run(self):
    try:
      while True:
        msg = self.q.get()
        if msg == None:
          self.s.close()
          self.q = None
          break
        if msg == SystemExit:
          raise SystemExit
        self.s.sendall(msg)
        self.q.task_done()
    except (socket.error,SystemExit) as e:
      self.saved_exception = e
      self.q = None # Release resources
      self.s.close()

def ip_to_int(ip, is_network=False):
  if is_network:
    return struct.unpack(ENDIAN+"I",ip)[0]
  else:
    return struct.unpack("I",ip)[0]
def int_to_ip(i, is_network=False):
  if is_network:
    ip = struct.unpack("BBBB",struct.pack(ENDIAN+"I",i))
  else:
    ip = struct.unpack("BBBB",struct.pack("I",i))
  return ".".join(["%d"%x for x in ip])



class PMURecord():
  def __init__(self, devid=0, timestamp=0, values=[0]*8):
    self.devid = devid
    self.timestamp = timestamp
    self.values = values
  
  def __str__(self):
    return "DEV_ID:%d  TIMESTAMP:%d  VALUES:"%(self.devid,self.timestamp)+(" ".join(["%f"%v for v in self.values]))

  def copy(self):
    return PMURecord(devid=self.devid, timestamp=self.timestamp, values=list(self.values))
    
  def pack(self):
    return struct.pack(ENDIAN+"iq8d",self.devid,self.timestamp,
          self.values[0],
          self.values[1],
          self.values[2],
          self.values[3],
          self.values[4],
          self.values[5],
          self.values[6],
          self.values[7])
  
  def unpack(self, cstring):
    res = list(struct.unpack(ENDIAN+"iq8d", cstring))
    self.devid = res[0]
    self.timestamp = res[1]
    self.values = res[2:10]
    return self
    
  def unpack_stream(self, s):
    # s  : Stream implementing recv(buf_size)
    return self.unpack( s.recv(PMURecord.len()) )
    
  def __eq__(self, other):
    if isinstance(other, PMURecord):
      return self.devid == other.devid and self.timestamp == other.timestamp and self.values == other.values
    else:
      return False
    
  def __ne__(self,other):
    return not self.__eq__(other)
    
  @staticmethod
  def len():
    return 4+8+(8*8)
    
  @staticmethod
  def from_array(times,id,x):

    """
    times : T - array of double timestamps (seconds)
    id  : int   Device ID of the first data dimension
    x   : T by n array of data
    returns a list of PMURecords for the data
    """
    
    T = times.shape[0]
    assert( x.shape[0] == T )
    n = x.shape[1]
    if n%8 != 0:
      x = np.hstack( x, np.zeros(T, 8-(n%8)) )
    recs = []
    for i in range(0, n, 8):
      devid = id + i
      for t,val in zip(times,x[:,i:i+8]):
        recs.append(PMURecord(devid=devid, timestamp=long(t[0]/TIME_FACTOR), values=val.ravel()))
    return recs
    
  def to_observation_matrix(self):
    arr = np.zeros((8,3))
    arr[ :, 0 ] = np.arange( self.devid, self.devid+8 )
    arr[ :, 1 ] = self.timestamp * TIME_FACTOR
    arr[ :, 2 ] = self.values
    return arr
    
  @staticmethod
  def random():
    return PMURecord(random.randint(0, 200) * 8,
          long(random.randint(0, 2000) * (1.0/30.0 * 1.0E6)),
          [random.random() for i in range(8)])
          
  def match_range(self, tlow=-1, thigh=-1, devlow=-1, devhigh=-1):
    if tlow != -1 and self.timestamp < tlow:
      return False
    elif thigh != -1 and self.timestamp > thigh:
      return False
    elif devlow != -1 and self.devid < devlow:
      return False
    elif devhigh != -1 and self.devid > devhigh:
      return False
    else:
      return True




class Cmd_PING():
  def __init__(self, timestamp=time.time()):
    if type(timestamp) == float:
      self.ts = long( timestamp / TIME_FACTOR )
    elif type(timestamp) == long or type(timestamp) == int:
      self.ts = timestamp
    else:
      raise NotImplementedError()
  def pack(self):
    return struct.pack(ENDIAN+"4sq","PING",self.ts)
  # def unpack(self, cstring):
    # cmd,self.ts = struct.unpack(ENDIAN+"4sq", cstring)
    # return self
  def unpack_stream(self, cmd, s):
    # cmd: 4-byte command string already read from s
    # s  : Stream implementing recv(buf_size)
    assert( cmd == "PING" )
    self.ts = struct.unpack(ENDIAN+"q", s.recv(8))[0]
    return self
  def diff(self, other):
    # ts: 64bit signed integer
    # Returns difference in time in seconds
    return (other.ts - self.ts) * TIME_FACTOR
  def __eq__(self, other):
    if isinstance(other, Cmd_PING):
      return self.ts == other.ts
    else:
      return False
  def __ne__(self,other):
    return not self.__eq__(other)
  def __str__(self):
    return " ".join([str(x) for x in ["PING",self.ts]])
    




class Cmd_PONG():
  def __init__(self, timestamp=0):
    self.ts = timestamp
  def pack(self):
    return struct.pack(ENDIAN+"4sq","PONG",self.ts)
  def unpack_stream(self,cmd,s):
    # cmd: 4-byte command string already read from s
    # s  : Stream implementing recv(buf_size)
    assert( cmd == "PONG" )
    self.ts = struct.unpack(ENDIAN+"q", s.recv(8))[0]
    return self
  def __str__(self):
    return " ".join([str(x) for x in ["PONG",self.ts]])



class Cmd_PUBP():
  def __init__(self, ip=0, port=0, pmu_id_list=[]):
    # self.ip = struct.unpack("I",ip)[0]
    self.ip = ip
    self.port = port
    self.ids = pmu_id_list
  def pack(self):
    n = len(self.ids)
    l = [self.ip, self.port, n]
    l.extend(self.ids)
    # print l
    return "PUBP"+struct.pack(ENDIAN+"IHi%di"%n, *l)
  def unpack_stream(self, cmd, s):
    # cmd: 4-byte command string already read from s
    # s  : Stream implementing recv(buf_size)
    assert( cmd == "PUBP" )
    self.ip,self.port,n_pmus = struct.unpack(ENDIAN+"IHi", s.recv(4+2+4))
    # print "PUBP:",int_to_ip(self.ip),self.port,n_pmus,
    self.ids = struct.unpack(ENDIAN+"%di"%n_pmus, s.recv(n_pmus*4))
    # print self.ids
    return self
  def __str__(self):
    return " ".join([str(x) for x in ["PUBP",int_to_ip(self.ip),self.port,self.ids]])
    
"""
class Cmd_RPUB():
  def __init__(self,data=[]):
    # data = list of ( Worker_id, tuple of pmu_id )
    self.data = data
    
  # def unpack(self,cstring):
    # low,high = 0,8
    # cmd,n_workers = struct.unpack("4si",cstring[low:high])
    # self.data = []
    # for i in range(n_workers):
      # low,high = (high,high+8)
      # worker_id,n_pmus = struct.unpack("ii",cstring[low:high])
      # low,high = (high,high+n_pmus*4)
      # pmus = struct.unpack("%di"%n_pmus, cstring[low:high])
      # self.data.append( (worker_id, pmus) )
    # return self
  
  def pack(self):
    l = ["RPUB", struct.pack(ENDIAN+"i",len(self.data))]
    for worker_id, pmus in self.data:
      l.append( struct.pack(ENDIAN+"i",worker_id) )
      l.append( struct.pack(ENDIAN+"%di"%len(pmus), *pmus) )
    return "".join(l)
  def unpack_stream(self, cmd, s):
    # cmd: 4-byte command string already read from s
    # s  : Stream implementing recv(buf_size)
    assert( cmd == "RPUB" )
    n_workers = struct.unpack(ENDIAN+"i",s.recv(4))
    self.data = []
    for i in range(n_workers):
      worker_id,n_pmus = struct.unpack(ENDIAN+"ii",s.recv(8))
      pmus = struct.unpack(ENDIAN+"%di"%n_pmus, s.recv(n_pmus*4))
      self.data.append( (worker_id, pmus) )
    return self
"""

class Cmd_GETR():
  def __init__(self,worker_id=0, pmu_list=[]):
    self.worker_id = worker_id
    self.pmus = pmu_list
  def pack(self):
    n = len(self.pmus)
    l = ["GETR",self.worker_id,len(self.pmus)]
    l.extend(self.pmus)
    return struct.pack(ENDIAN+"4sii%di"%n,*l)
  def unpack_stream(self, cmd, s):
    # cmd: 4-byte command string already read from s
    # s  : Stream implementing recv(buf_size)
    assert( cmd == "GETR" )
    self.worker_id,n_pmus = struct.unpack(ENDIAN+"ii",s.recv(8))
    self.pmus = struct.unpack(ENDIAN+"%di"%n_pmus, s.recv(n_pmus*4))
    return self
  def __str__(self):
    return " ".join([str(x) for x in ["GETR",self.worker_id,self.pmus]])


"""
class Cmd_RGET():
  def __init__(self, resp="OKAY")
    self.response = resp
  @staticmethod
  def cmd():
    return "RGET"
  def pack(self):
    return struct.pack(ENDIAN+"4s4s","RGET",self.response)
  def unpack_stream(self, cmd, s):
    # cmd: 4-byte command string already read from s
    # s  : Stream implementing recv(buf_size)
    assert( cmd == "RGET" )
    self.response = struct.unpack(ENDIAN+"4s",s.recv(4))[0]
    return self
"""



class Cmd_SEND():
  def __init__(self, record=None):
    # record: type PMURecord
    self.rec = record
  def pack(self):
    return struct.pack(ENDIAN+"4s","SEND")+self.rec.pack()
  def unpack_stream(self, cmd, s):
    # cmd: 4-byte command string already read from s
    # s  : Stream implementing recv(buf_size)
    assert( cmd == "SEND" )
    self.rec = PMURecord()
    self.rec.unpack_stream(s)
    return self
  def __str__(self):
    return " ".join([str(x) for x in ["SEND",self.rec]])

class Cmd_LIST():
  def __init__(self, low=0, high=-1):
    self.low,self.high = low,high
  def pack(self):
    return struct.pack(ENDIAN+"4sii","LIST",self.low,self.high)
  def unpack_stream(self):
    # cmd: 4-byte command string already read from s
    # s  : Stream implementing recv(buf_size)
    assert( cmd == "LIST" )
    self.low,self.high = struct.unpack(ENDIAN+"ii", s.recv(8))
    return self
  def __str__(self):
    return " ".join([str(x) for x in ["LIST",self.low,self.high]])

class Cmd_RLST():
  def __init__(self, pmu_list=[]):
    self.pmus = pmu_list
  def pack(self):
    n = len(self.pmus)
    l = ["RLST", n]
    l.extend(self.pmus)
    return struct.pack(ENDIAN+"4si%di"%n, *l)
  def unpack_stream(self, cmd, s):
    # cmd: 4-byte command string already read from s
    # s  : Stream implementing recv(buf_size)
    assert( cmd == "RLST" )
    n_pmus = struct.unpack(ENDIAN+"i",s.recv(4))[0]
    self.pmus = struct.unpack(ENDIAN+"%di"%n_pmus, s.recv(n_pmus*4))
    return self
  def __str__(self):
    return " ".join([str(x) for x in ["RLST",self.pmus]])



class Cmd_SUBP():
  def __init__(self, ip=0, port=0, pmu_list=[]):
    self.ip, self.port = ip,port
    self.pmus = pmu_list
  def pack(self):
    n = len(self.pmus)
    l = ["SUBP", self.ip, self.port, n]
    l.extend(self.pmus)
    return struct.pack(ENDIAN+"4sIHi%di"%n, *l)
  def unpack_stream(self, cmd, s):
    # cmd: 4-byte command string already read from s
    # s  : Stream implementing recv(buf_size)
    assert( cmd == "SUBP" )
    self.ip, self.port, n_pmus = struct.unpack(ENDIAN+"IHi", s.recv(4+2+4))
    self.pmus = struct.unpack(ENDIAN+"%di"%n_pmus, s.recv(n_pmus*4))
    return self
  def __str__(self):
    return " ".join([str(x) for x in ["SUBP",int_to_ip(self.ip),self.port,self.pmus]])

"""
class Cmd_RSUB():
  def __init__(self, data=[]):
    # data : list of ( worker_id, tuple of pmu_ids )
    self.data = data
  def pack(self):
    l = ["RSUB", struct.pack(ENDIAN+"i",len(self.data))]
    for worker_id, pmus in self.data:
      l.append( struct.pack(ENDIAN+"i",worker_id) )
      l.append( struct.pack(ENDIAN+"%di"%len(pmus), *pmus) )
    return "".join(l)
  def unpack_stream(self, cmd, s):
    # cmd: 4-byte command string already read from s
    # s  : Stream implementing recv(buf_size)
    assert( cmd == "RSUB" )
    n_workers = struct.unpack(ENDIAN+"i",s.recv(4))
    self.data = []
    for i in range(n_workers):
      worker_id,n_pmus = struct.unpack(ENDIAN+"ii",s.recv(8))
      pmus = struct.unpack(ENDIAN+"%di"%n_pmus, s.recv(n_pmus*4))
      self.data.append( (worker_id, pmus) )
    return self
"""

class Cmd_PUTR():
  def __init__(self,worker_id=0, pmu_list=[]):
    self.worker_id = worker_id
    self.pmus = pmu_list
  def pack(self):
    n = len(self.pmus)
    l = ["PUTR",self.worker_id,len(self.pmus)]
    l.extend(self.pmus)
    return struct.pack(ENDIAN+"4sii%di"%n,*l)
  def unpack_stream(self, cmd, s):
    # cmd: 4-byte command string already read from s
    # s  : Stream implementing recv(buf_size)
    assert( cmd == "PUTR" )
    self.worker_id,n_pmus = struct.unpack(ENDIAN+"ii",s.recv(8))
    self.pmus = struct.unpack(ENDIAN+"%di"%n_pmus, s.recv(n_pmus*4))
    return self
  def __str__(self):
    return " ".join([str(x) for x in ["PUTR",self.worker_id,self.pmus]])

"""
class Cmd_RPUT():
  def __init__(self, resp="OKAY")
    self.response = resp
  @staticmethod
  def cmd():
    return "RPUT"
  def pack(self):
    return struct.pack(ENDIAN+"4s4s","RPUT",self.response)
  def unpack_stream(self, cmd, s):
    # cmd: 4-byte command string already read from s
    # s  : Stream implementing recv(buf_size)
    assert( cmd == "RPUT" )
    self.response = struct.unpack(ENDIAN+"4s",s.recv(4))[0]
    return self
"""

class Cmd_WORK():
  def __init__(self,ip=0, port=0):
    self.ip = ip
    self.port = port
  def pack(self):
    return struct.pack(ENDIAN+"4sIH","WORK",self.ip,self.port)
  def unpack_stream(self, cmd, s):
    # cmd: 4-byte command string already read from s
    # s  : Stream implementing recv(buf_size)
    assert( cmd == "WORK" )
    self.ip,self.port = struct.unpack(ENDIAN+"IH",s.recv(6))
    print "WORK:",self.ip,self.port
    return self
  def __str__(self):
    return " ".join([str(x) for x in ["WORK",int_to_ip(self.ip),self.port]])

class Cmd_RWOR():
  def __init__(self,id=0, group_id=0):
    self.id = id
    self.group_id = group_id
  def pack(self):
    return struct.pack(ENDIAN+"4sii","RWOR",self.id,self.group_id)
  def unpack_stream(self, cmd, s):
    # cmd: 4-byte command string already read from s
    # s  : Stream implementing recv(buf_size)
    assert( cmd == "RWOR" )
    self.id,self.group_id = struct.unpack(ENDIAN+"ii",s.recv(8))
    return self
  def __str__(self):
    return " ".join([str(x) for x in ["RWOR",self.id,self.group_id]])

class Cmd_CONP():
  def __init__(self,ip=0, port=0, pmu_list=[]):
    self.ip = ip
    self.port = port
    self.pmus = pmu_list
  def pack(self):
    n = len(self.pmus)
    l = ["CONP", self.ip, self.port, n]
    l.extend(self.pmus)
    # print "CONP:","4sIHi%di"%n, l
    return struct.pack(ENDIAN+"4sIHi%di"%n,*l)
  def unpack_stream(self, cmd, s):
    # cmd: 4-byte command string already read from s
    # s  : Stream implementing recv(buf_size)
    assert( cmd == "CONP" )
    self.ip, self.port, n = struct.unpack(ENDIAN+"IHi",s.recv(10))
    self.pmus = struct.unpack(ENDIAN+"%di"%n, s.recv(4*n))
    return self
  def __str__(self):
    return " ".join([str(x) for x in ["CONP",int_to_ip(self.ip),self.port,self.pmus]])

class Cmd_RCON():
  def __init__(self):
    pass
  def pack(self):
    return struct.pack(ENDIAN+"4s","RCON")
  def unpack_stream(self, cmd, s):
    # cmd: 4-byte command string already read from s
    # s  : Stream implementing recv(buf_size)
    assert( cmd == "RCON" )
    return self
  def __str__(self):
    return " ".join([str(x) for x in ["RCON"]])

class Cmd_CONS():
  def __init__(self,ip=0, port=0, pmu_list=[]):
    self.ip = ip
    self.port = port
    self.pmus = pmu_list
  def pack(self):
    n = len(self.pmus)
    l = ["CONS", self.ip, self.port, n]
    l.extend(self.pmus)
    return struct.pack(ENDIAN+"4sIHi%di"%n,*l)
  def unpack_stream(self, cmd, s):
    # cmd: 4-byte command string already read from s
    # s  : Stream implementing recv(buf_size)
    assert( cmd == "CONS" )
    self.ip, self.port, n = struct.unpack(ENDIAN+"IHi",s.recv(10))
    self.pmus = struct.unpack(ENDIAN+"%di"%n, s.recv(4*n))
    return self
  def __str__(self):
    return " ".join([str(x) for x in ["CONS",int_to_ip(self.ip),self.port,self.pmus]])

class Cmd_RCOS():
  def __init__(self):
    pass
  def pack(self):
    return struct.pack(ENDIAN+"4s","RCOS")
  def unpack_stream(self, cmd, s):
    # cmd: 4-byte command string already read from s
    # s  : Stream implementing recv(buf_size)
    assert( cmd == "RCOS" )
    return self
  def __str__(self):
    return " ".join([str(x) for x in ["RCOS"]])

class Cmd_DISC():
  def __init__(self,ip=0,port=0):
    self.ip, self.port = ip,port
  def pack(self):
    return struct.pack(ENDIAN+"4sIH","DISC",self.ip,self.port)
  def unpack_stream(self, cmd, s):
    # cmd: 4-byte command string already read from s
    # s  : Stream implementing recv(buf_size)
    assert( cmd == "DISC" )
    self.ip,self.port = struct.unpack(ENDIAN+"IH", s.recv(6))
    return self
  def __str__(self):
    return " ".join([str(x) for x in ["DISC",self.ip,self.port]])

class Cmd_RDIS():
  def __init__(self):
    pass
  def pack(self):
    return struct.pack(ENDIAN+"4s","RDIS")
  def unpack_stream(self, cmd, s):
    # cmd: 4-byte command string already read from s
    # s  : Stream implementing recv(buf_size)
    assert( cmd == "RDIS" )
    return self
  def __str__(self):
    return " ".join([str(x) for x in ["RDIS"]])

class Cmd_LATN():
  def __init__(self):
    pass
  def pack(self):
    return struct.pack(ENDIAN+"4s","LATN")
  def unpack_stream(self, cmd, s):
    # cmd: 4-byte command string already read from s
    # s  : Stream implementing recv(buf_size)
    assert( cmd == "LATN" )
    return self
  def __str__(self):
    return " ".join([str(x) for x in ["LATN"]])

class Cmd_LATR():
  def __init__(self,data=[]):
    # data is list of (ip,port,latency_value) tuple
    self.data = data
  def pack(self):
    n = len(self.data)
    return struct.pack(ENDIAN+"4si","LATR",n) + "".join([struct.pack(ENDIAN+"IHi", ip,port,latency) for (ip,port,latency) in self.data])
  def unpack_stream(self, cmd, s):
    # cmd: 4-byte command string already read from s
    # s  : Stream implementing recv(buf_size)
    assert( cmd == "LATR" )
    n = struct.unpack(ENDIAN+"i", s.recv(4))
    self.data = []
    for i in range(n):
      ip,port,latency = struct.unpack(ENDIAN+"IHi",s.recv(10))
      self.data.append( (ip,port,latency) )
    return self
  def __str__(self):
    return " ".join([str(x) for x in ["LATR",self.data]])


