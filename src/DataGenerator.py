import random, time
import numpy as np
import scipy as sp
from scipy.sparse import lil_matrix
import pdb

class DataGenerator():
    """
    Generates random multi-dimensional time-series data for state estimation
    with a fixed random seed so as to allow different components to generate
    the same set of data.
    """
    def __init__(self, n=8*10, m=8*12, error_std=1e-2, randseed=1023):
        """
        Initializes random data generator.
        n : number of dimension in state vector
        m : number of dimensions in measurement vector
        
        Data, at time t, is generated as follows,
        A := m-by-n random matrix
        M := n-by-n mixing matrix
        c1,...,c4 := n-by-1 random coefficient vectors
        eps(t) := m-by-1 random measurement error vector
        x(t) = M * ( c1 * sin( c2*t + c3 ) + c4 )
        y(t) = A * x(t) + eps(t)
        """
        
        self.n = n
        self.m = m
        # self.id = id_range
        self.error_std = error_std
        saved_state = np.random.get_state()
        np.random.seed(randseed)
        self.EstimationMatrix = lil_matrix((m,n))
        prob = 5.0*(m+n) / (m*n)
        # print prob
        for i in range(m):
          for j in range(n):
            if np.random.rand() <= prob:
              self.EstimationMatrix[i,j] = np.random.randn()
        self.EstimationMatrix = self.EstimationMatrix.tocsr()
        self.MixingMatrix = np.random.randn(n,n)
        self.coeffs = np.random.rand(n,4)
        np.random.set_state(saved_state)
        
    def generate_state(self, t):
        xt = self.MixingMatrix.dot(
            self.coeffs[:,0] * np.sin(np.mod(self.coeffs[:,1]*t,2*np.pi) + self.coeffs[:,2]) + self.coeffs[:,3])
        return xt
        
    def generate_measurement(self, t, state, id_range=(0,1)):
	xt = state
        xt.shape = (self.n,1)
        l,h = id_range
        assert( l >= 0 and l < h and h <= self.m)
        yt = self.EstimationMatrix[ l:h, : ].dot( xt ) + self.error_std*np.random.randn(h-l, 1)
        return yt
    
    def generate(self, t, id_range=(0,1)):
        xt = self.generate_state(t)
        return self.generate_measurement(t, xt, id_range)
    
    def display_all(self):
        print "==now printing estimation matrix=="
        print self.EstimationMatrix
        print "==done with estimation matrix=="
        print "==now printing mixing matrix=="
        print self.MixingMatrix
        print "==done with mixing matrix=="


if __name__ == "__main__":
    dg = DataGenerator(n=4,m=5)
    print dg.EstimationMatrix.todense()
    # This part should be same across different runs
    #dg.display_all()
    for t in 1333507454.17+np.arange(10.0,15.0,0.3):
        print t, dg.generate_state(t)
        # This part should vary across runs
        print [random.random() for i in range(10) ]
    
    
    
