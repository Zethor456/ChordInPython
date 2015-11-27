from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor
from twisted.protocols.basic import FileSender
from message import Message, Find, Notify, Join, Hello
from node import Node
import sys
import re

class ChordServer():
    def __init__(self,host,target):
        self.connections = {} #is a dict mapping node to connection
        self.predecessor = None
        self.successor = None
        self.fingers = [] #TODO implement finger tables or some approx
        self.host = host #The Node of the server
        self.me = host
        self.target = target #The initial server to connect to
        self.state = "ALONE"

    def readInput(self):
        print "Request files with: get [file]"
        pattern = re.compile("get\s+(.+)$")
        while(True):
            cmd = raw_input()
            match = pattern.match(cmd)
            if match:
                self.query(match.group(1))
            else:
                for c in self.connections.itervalues():
                    c.sendLine(Message(cmd).tobytes())

    def run(self):
        print("Initializing " + self.host.toString())
        reactor.connectTCP(self.target.ip,self.target.port,ChordFactory(self))#@UndefinedVariable
        reactor.listenTCP(self.host.port,ChordFactory(self))#@UndefinedVariable
        reactor.callInThread(self.readInput)#@UndefinedVariable
        reactor.run()#@UndefinedVariable
    ##########################
    #SERVER UTILITY FUNCTIONS#
    ##########################
    
    def add(self,node,connection):
        self.connections[node]=connection
        
    def remove(self,connection):
        toDelete = None
        for n,c in self.connections.iteritems():
            if c==connection:
                toDelete = n
        del self.connections[toDelete]
        
        
    def setState(self,state):
        print "Setting state to "+state
        self.state = state
    
    def setPred(self,pred):
        print "Setting predecessor to {0}".format(pred.toString())
        self.predecessor = pred
    
    def setSucc(self,succ):
        print "Setting successor to {0}".format(succ.toString())
        self.successor = succ
    ##########################
    #CHORD SPECIFIC FUNCTIONS#
    ##########################
    
    ### need to be implemented(Guelor)
    ##Send a file request message to the other server
    ##
    def query(self,aFile):
        print "Looking for {0}!".format(aFile)
        for c in self.connections.itervalues():
                    c.sendLine(Find(self.me,aFile).tobytes())
        #TODO send the file query
    
    def notify(self,node):
        if self.predecessor == None:
            self.setPred(node.node) 
    
    def create(self):
        print "Initializing Chord at {0}".format(self.me.toString())
        self.setState("CONNECTED")
        self.predecessor = None
        self.setSucc(self.me)
    
    def join(self,node):
        self.predecessor = None
        node.sendLine(Join(self.me).tobytes())
    
    
    #Bulk of the work need to be here note...
    def handleMsg(self,node,msg):
        #Initial handshake when connecting
        #Node id's are shared
        if isinstance(msg,Hello):
            print "Received Hello"
            node.node = msg.node
            self.add(msg.node,node)
            if self.state == "ALONE":
                self.join(node)
            return
        
        #Once connected request to join is sent
        #A Notify is returned with the new successor
        if self.state == "ALONE":
            if isinstance(msg, Notify):
                print "Received: {1} node {0}".format(msg.node.id,msg.msg)
                self.setSucc(node.node)
                self.setState("CONNECTED")
            else:
                print "Incorrect or Unknown Message Received: {0}".format(msg.msg)

        #The server is now connected to the Chord Network
        #The remaining Chord functionality should go in here
        elif self.state == "CONNECTED":
            if isinstance(msg, Notify):
                print "Received: {1} node {0}".format(msg.node.id,msg.msg)
                self.notify(node.node)
            elif isinstance(msg, Join):
                #TODO Actually figure out who the successor is for node.node
                node.sendLine(Notify(self.me).tobytes())
                print "Received: {1} node {0}".format(msg.node.id,msg.msg)
            elif isinstance(msg,Find):
                #TODO File querying behaviour in here
                #Server sends port for it's FileProtocol
                #to handle the actual data transfer
                print "Node {0} is searching for {1}".format(msg.node.id,msg.file)
            else:
                print "Incorrect or Unknown Message Received: {0}".format(msg.msg)
        else:
            print ("In bad state?!? Received: "+msg.msg)

class Chord(LineReceiver):
    def __init__(self, factory):
        self.port = None
        self.factory = factory
        self.node = None

    def connectionMade(self):
        server = self.factory.server
        #TODO not actually setting to rawMode?!?
        self.setRawMode()
        self.sendLine(Hello(server.me).tobytes())
        
    def dataReceived(self,data):
        msg = Message.deserialize(data)
        self.factory.server.handleMsg(self,msg)
        
    def rawDataReceived(self,data):
        msg = Message.deserialize(data)
        self.factory.server.handleMsg(self,msg)
    
    def connectionLost(self, reason):
        self.factory.server.remove(self)

class FileProtocal(FileSender):
    def __init__(self,files):
        self.files = files

class ChordFactory(Factory):
    def __init__(self,server):
        self.server = server
        
    def buildProtocol(self, addr):
        return Chord(self)
    
    def startedConnecting(self,connector):
        print("Attempting to connect!")
    
    def clientConnectionFailed(self,transport,reason):
        print("A connection Failed")
        self.server.create()
    def clientConnectionLost(self,connector,reason):
        print("Lost connection")
        
if __name__ == '__main__':
    #Pass in args from 1 and on
    args = sys.argv[1:]
    
    #FYI Node(ip,port,filePort,nodeId)
    #note * just unpacks the list
    host = Node(*(args[0:4]))    
    target = Node(*(args[4:7]))
    server = ChordServer(host,target)
    server.run()