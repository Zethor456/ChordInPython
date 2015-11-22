from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor
from twisted.protocols.basic import FileSender
import pickle
import sys

class ChordServer():
    def __init__(self,host,target):
        self.connections = []
        self.predecessors = []
        self.successors = []
        self.fingers = []
        self.host = host
        self.target = target

    def readInput(self):
        while(True):
            print ">"
            cmd = raw_input()
            for c in self.connections:
                c.transport.write(cmd)

    def run(self):
        print("Initializing " + self.host.toString())
        reactor.connectTCP(self.target.ip,self.target.port,ChordFactory(self.connections))#@UndefinedVariable
        reactor.listenTCP(self.host.port,ChordFactory(self.connections))#@UndefinedVariable
        reactor.callInThread(self.readInput)#@UndefinedVariable
        reactor.run()#@UndefinedVariable
        
class Message():
    def __init__(self,node,messageType):
        self.node = node
        self.type = messageType
    @staticmethod
    def serialize(msg):
        return pickle.dumps(msg)
    @staticmethod
    def deserialize(msg):
        return pickle.loads(msg)

class Node():
    def __init__(self,ip,port,filePort,nodeId):
        self.ip = ip
        self.port = int(port)
        self.filePort = int(filePort)
        self.id = nodeId

    def toString(self):
        return "node {0.id} at {0.ip}:{0.port},{0.filePort}".format(self)



class Chord(LineReceiver):
    def __init__(self, connections):
        self.port = None
        self.state = "CONNECTING"
        self.connections = connections

    def connectionMade(self):
        global connection
        connection = self
        self.sendLine("Hello!")
        
    def dataReceived(self,data):
        print("Received {0}".format(data))
        
class FileProtocal(FileSender):
    def __init__(self,files):
        self.files = files

class ChordFactory(Factory):
    def __init__(self,connections):
        self.connections = connections
        
    def buildProtocol(self, addr):
        return Chord(self.connections)
    
    def startedConnecting(self,connector):
        print("Attempting to connect!")
    def clientConnectionFailed(self,transport,reason):
        print("Connection Failed")

        
if __name__ == '__main__':
    #Pass in args from 1 and on
    args = sys.argv[1:]
    
    #FYI Node(ip,port,filePort,nodeId)
    #note * just unpacks the list
    #TODO replace nodeId with real id instead of port number
    host = Node(*(args[0:3]+[args[1]]))    
    target = Node(*(args[3:6]+[args[4]]))
    server = ChordServer(host,target)
    server.run()