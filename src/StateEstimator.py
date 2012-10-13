from DataGenerator import DataGenerator

import scipy as sp
from scipy.sparse import csr_matrix, lil_matrix, dok_matrix
from scipy.sparse.linalg import lsqr

import time

import numpy.linalg
import numpy

class StateEstimator():
    ''' 
    Estimates state x(t) given y(t) and A such that y(t) = A * x(t)
    Uses 1st order approximation on x(t) = x(0) = t \dot x
    Finds the least square solution to:
        [ A   ,  t(0) A          [ x(0)                 [ y(0) 
          A   ,  t(1) A            \dot x ]               y(1) 
          A   ,  t(2) A      *                      =     y(2) 
         ...  ,   ...                                     y(.) 
          A   ,  t(n) A  ]                                y(n) ]
    '''
    def __init__(self,A=None):
        self.setEstimationMatrix(A)
    
    def setEstimationMatrix(self,A):
        self.A = A
        self.m = 0 if A == None else A.shape[0]
        self.n = 0 if A == None else A.shape[1]
    
    def estimate(self, observations):
        ''' observations: K by 3 array of [id, time, value] rows
                K = m*T
                id is 0-indexed
            returns a n by 2 vector of state and derivative
        '''
        K = observations.shape[0]
        t0 = min( observations[:,1] )
        t = time.time()
        rows = observations[:,0]
        
        #What is this?
        timestamps = sp.sparse.spdiags( observations[:,1]-t0, 0, K, K, "csr" )
        
        b = observations[:,2]
        
        A = sp.sparse.hstack( [ self.A[ rows, : ], timestamps.dot(self.A[ rows, : ]) ] )
        A = A.tocsr()
        
        x = lsqr(A, b, iter_lim=100)[0]
        x = x.reshape(2,-1).T
        
        return (x,t0)
        
    def interpolate(self, state, t0, times):
        ''' Interpolates the state at times indicated by <times>
            State: x(0) and \dot x, n by 2 vector
            Times: T by 1 vector of time(s) to interpolate
            Returns T by n vector of interpolated state
        '''
        T = times.shape[0]
        times.shape = (T,1)
        n = state.shape[0]
        x0 = state[:,0].T
        xt = state[:,1].T
        x0.shape = ( 1,n )
        xt.shape = ( 1,n )
        x = x0.repeat( T, 0 ) + sp.dot( times-t0, xt )
        return x
        
if __name__ == "__main__":
    import matplotlib.pyplot as plt
    m = 8
    n = 8
    
    dg = DataGenerator(n=n,m=m)
    
    se = StateEstimator(dg.EstimationMatrix)
    
    time_offset = time.time()
    # time_offset = 0.0
    times = sp.arange(time_offset+0.0, time_offset+1.7, 1.0/3.0).reshape(-1,1)
    T = times.shape[0]
    print "==times.shape[0]=="
    print times.shape[0]
    # state = sp.hstack( [ (sp.pi-times)*sp.cos(2*times), (1+times)*sp.sin(3*times)] )
    state = sp.vstack( [ dg.generate_state(t).T for t in times ] )
    print "==state.shape=="
    print state.shape
    print "===state==="
    print state
    # y = A.dot( state.T ).T
    y = sp.vstack( [ dg.generate(t,id_range=(0,m)).T for t in times ] )
    print "==y.shape=="
    print y.shape
    print "==y=="
    print y
    # exit(0)
    observations = sp.zeros( (T * m, 3) ) #initialization
    count = 0
    
    for t,row in zip(times,y):
        for id,val in enumerate(row):
            observations[count, :] = sp.array( [ id, t[0], val ] ).reshape(1,-1)
            count += 1
    
    print "==observations.shape=="
    print observations.shape
    print "==observations=="
    print observations
    print "==T*m=="
    print T*m
    
    stepsize = 1 # 5hz
    xhat = sp.zeros( state.shape )
    
    for t in range( 0, T + 1 - stepsize, stepsize ):
        newstate,t0 = se.estimate( observations[t*m:(t+stepsize)*m,:] )
        xhat[t:(t+stepsize),:] = se.interpolate(newstate,t0,times[t:(t+stepsize),:])
    print "==xhat=="
    print xhat
    
    """
    #numpy.set_printoptions(threshold='nan') #helps print the full matrix
    print "==xhat.shape=="
    print xhat.shape
    print "==xhat=="
    print xhat
    
    
    plt.plot(times,state,label='Original')
    plt.plot(times,xhat,label='Fitted')
    plt.legend(loc=2)
    plt.show()
    plt.plot(times, y)
    plt.show()
    raw_input()
   """ 
