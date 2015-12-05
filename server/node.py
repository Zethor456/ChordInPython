class Node():
    def __init__(self,ip,port,filePort,nodeId=None):
        self.ip = ip
        self.port = int(port)
        self.filePort = int(filePort)
        #TODO node id's are somewhat redundant
        #more for human readable identification
        self.id = nodeId

    def toString(self):
        return "node {0.id} at {0.ip}:{0.port},{0.filePort}".format(self)

    def __hash__(self):
        return hash((self.id))
    def __eq__(self,other):
        if(other==None):
            return False
        else:
            return (self.id)==(other.id)
    
    def address(self):
        return "{0.ip}:{0.port}".format(self)
    
    def sortingSucc(self, comp):
        if self.id > comp.id:
            return int(self.id) + 1000
        elif self.id == comp.id:
            return 2000
        else:   
            return int(self.id)
    def sortingPred(self, comp):
        if self.id > comp.id:
            return -(int(self.id) - 1000)
        elif self.id == comp.id:
            return - 1000
        else:
            return -int(self.id)
