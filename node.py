from socket import *
import time
from threading import *
import signal
import sys
import uuid
import copy
import random
import network
from NetworkMessageTypes import *


## Defining a node structure
# ID: the identifer of this node
# IPADDR: the Ip address of this node
# Ctrlport: the port that is opento accept incoming messages

class Node():
    ID = random.randrange(1000) + 1000
    IPADDR = "localhost"
    ctrlport = 2001
    
    
    def join(self,address):
    
        return 0
    
    def get_successor(self):
        
        return 0
        
    def get_predicessor(self):
        
        return 0
    
    
    


## Global Access
## TODO: fingers, successors
# thisnode: instance of thisnode
# fingers: list of nodes we know about
# successors: short list of next nodes for network balence
#
thisnode = Node()

fingers = []

successors = []




##
#
#
def Main():

    #raw_cmd = input("C or S")
    
    
    #if raw_cmd == "S":
        listenerThread = Thread(target= network.connection_listener, args =(thisnode,))
        listenerThread.start()
        time.sleep(1)
    #if raw_cmd == "C":
        tempsoc = socket(AF_INET, SOCK_STREAM)
        tempsoc.connect(('localhost',2001))
        time.sleep(1)
        message = '123556'
        tempsoc.send(bytes([MessageType.IS_PREISESOR]))
        
        
Main()