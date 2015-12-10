##import md5
import hashlib
from bisect import bisect
class Ring(object):
    
    def __init__(self):
        self.ring = dict()
        self.keys = []
    
    def add(self,node):
        num = self.key(node.address())
        self.keys.append(num)
        self.keys.sort()
    
    #return the position of a node in the list
    #list is sorted so use bisect to fetch quickly
    def pos(self,node):
        key = self.key(node.address())
        pos = bisect(self.keys,key)
        return pos 
    
    #Use md5 to generate the hash   
    #and return it in integer form
    def key(self,aString):
        #m = md5.new()
        m = hashlib.md5()
        m.update(aString)
        return map(ord, m.digest())
    
    #Check to see if the three nodes are in
    #ascending order according to the ring...
    def ascending(self,o,n,m):
        x = self.key(o.address())
        a = self.key(n.address())
        b = self.key(m.address())
        if((b > a and a > x) or (a > x and x > b) or (x > b and b > a)):
            return True
        else:
            return False
