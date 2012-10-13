import os
import Queue
import thread
from threading import Event
#from threading import currentThread

def flush(event, cmd_queue, wbuffer):
    """writes data to file from. intended to be used in thread
        
        wbuffer writer buffer queue
        log_dir log directory
        log_index index in circular log
    """
    while True: # this will exit when program is killed
        while not event.isSet():
            event.wait()
        #print currentThread(), "---woke up!!!"
        cmd = cmd_queue.get()
        # join strings from queue or buffer for qlength times
        data = ''.join([ wbuffer.get() for i in xrange(cmd['qlength']) ]) 
        with open(cmd['filename'], cmd['mode']) as s:
            s.write(data)
        #print currentThread(), "---writing done!!!"
        event.clear()


class DataLogger:
    """Logs data in circular files asynchronously 
        
    """
    def __init__(self, directory=os.path.join(os.getcwd(), "data_capture_log"), 
            log_count=10, logsize=1, buffer_cache_fraction=0.5):
        # adjust buffersize according to logsize
        self.log_dir = directory
        self.log_count = log_count
        self.log_index = 0
        self.logsize = 1048576 * logsize

        self.wbuffer = Queue.Queue()
        self.cmd_queue = Queue.Queue()
        self.buffer_byte_length = 0 # length of concentated strings currently in Queue
        self.wbuffer_limit = self.logsize * buffer_cache_fraction
        self.event = Event()

        thread.start_new_thread(flush, (self.event, self.cmd_queue, self.wbuffer, ))

        self.log_naming = os.path.join(self.log_dir, "%i.log")


        #directory initialition
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
            print "log directory created at " + self.log_dir
            # initialize empty log files
            for filename in [self.log_naming % i for i in xrange(self.log_count)]:
                open(filename, 'w').close()

    def log(self, data, force_flush=False):
        '''almost nonblocking logging. wakes up thread when cache size is exceeded

            data string data
            force_flush True force the data to be written to log file
        '''
        self.wbuffer.put(data, block=False)
        self.buffer_byte_length += len(data)
        if self.buffer_byte_length >= self.wbuffer_limit or force_flush:
            self.buffer_byte_length = 0
            cmd = {}
            if os.path.getsize(self.log_naming % self.log_index) + len(data) <= self.logsize: # compare size in byte
                cmd['mode'] = 'a'
            else: # next file may exceed logsize if size of data is greater than logsize
                self.log_index = (self.log_index + 1) % self.log_count
                cmd['mode'] = 'w'
            cmd['filename'] = os.path.join(self.log_dir, self.log_naming % self.log_index)
            cmd['qlength'] =  self.wbuffer.qsize()

            self.cmd_queue.put(cmd, block=False)
            self.event.set()
