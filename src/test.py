import random, time
import numpy as np

#A test program to test data generator
from DataGenerator import DataGenerator
from GridMessageProtocol import *

dg=DataGenerator(n=1, m=8, randseed=1024)
#print "0:0"
#print "-1:1"
while True:
    x=dg.generate_measurement(t=time.time(), id_range=(0,8))
    alist=[item for sublist in x for item in sublist]
    print "0:%f" %alist[5]