
class Node():
    def __init__(self,ip,port,filePort,nodeId=None):
        self.ip = ip
        self.port = int(port)
        self.filePort = int(filePort)
        self.id = nodeId

    def toString(self):
        return "node {0.id} at {0.ip}:{0.port},{0.filePort}".format(self)

    def __hash__(self):
        return hash((self.id))
    def __eq__(self,other):
        return (self.id)==(other.id)
    
    def address(self):
        return "{0.ip}:{0.port}".format(self)