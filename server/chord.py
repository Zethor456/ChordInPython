from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor
from twisted.protocols.basic import FileSender
from message import Message, Find, Notify, Join, Inform
from node import Node
import sys
import re
from hash import Ring

class ChordServer():
    def __init__(self,host,target):
        self.connections = {} #is a dict mapping node to connection
        self.nodes = {} #is a dict mapping connections to nodes
        self.predecessor = None
        self.successor = None
        self.fingers = Ring()
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
        self.nodes[connection]=node
        
    def remove(self,connection):
        #toDelete = None
        #for n,c in self.connections.iteritems():
        #    if c==connection:
        #        toDelete = n
        #del self.connections[toDelete]
        if (connection in self.nodes):
            toDelete = self.nodes[connection]
            del self.connections[toDelete]
            del self.nodes[connection]
        
        
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
    
    def query(self,aFile):
        print "Looking for {0}!".format(aFile)
        #TODO send the file query
    
    def notify(self,node,msg):
        if self.predecessor == None:
            self.setPred(msg.node) 
    
    def inform(self,node,successor):
        print "informing node of successor"
        node.sendLine(Inform(successor).tobytes())
 
    def find_successor(self,node,msg):
        #if there is only one node eg the pred==succ
        #or the key is between this node and it's successor
        #or the wrap around case when pred > succ
        key = self.fingers.key(msg.target.address())
        pred = self.fingers.pos(self.me)
        succ = self.fingers.pos(self.successor)
        if(pred==succ or (pred<key and key <succ and pred < succ) or (pred>succ and (key > pred or key < succ))):
            #use the current connection to inform
            if (msg.target == msg.node):
                self.add(msg.node, node)
                node.sendLine(Inform(self.successor).tobytes())
            #create a connection back to the new node
            elif(not msg.target in self.connections):
                reactor.connectTCP(msg.target.ip,msg.target.port,ChordFactory(self,Inform(self.successor)))#@UndefinedVariable
            #or this node is already connected for some reason so use that
            else:
                self.inform(self.connections[msg.target])
            #update my successor to the new node
            self.setSucc(msg.target)
        else:
            #forward the join request to my successor
            self.connections[self.successor].sendLine(Join(self.me,msg.target).tobytes())
            print("Forward Join msg to succesor")
    
    def create(self):
        print "Initializing Chord at {0}".format(self.me.toString())
        self.fingers.add(self.me)   
        self.setState("CONNECTED")
        self.predecessor = None
        self.setSucc(self.me)
    
    def join(self,node):
        self.predecessor = None
        node.sendLine(Join(self.me,self.me).tobytes())
    
    def informed(self,node,msg):
        self.add(node,msg.node)
        self.setSucc(msg.node)
    
    def handleMsg(self,node,msg):     
        #Once connected request to join is sent
        #Eventually node is informed of it's new successor
        if self.state == "WAITING":
            if isinstance(msg, Inform):
                print "Received: {1} from node {0}".format(msg.node.id,msg.msg)
                self.informed(node,msg)
                self.setState("CONNECTED")
            else:
                print "Incorrect or Unknown Message Received: {0}".format(msg.msg)

        #The server is now connected to the Chord Network
        #The remaining Chord functionality should go in here
        elif self.state == "CONNECTED":
            if isinstance(msg, Notify):
                print "Received: {1} from node {0}".format(msg.node.id,msg.msg)
                self.notify(node,msg)
            elif isinstance(msg, Join):
                print "Received: {1} from node {2} for node {0}".format(msg.target.id,msg.msg,msg.node.id)
                self.find_successor(node,msg)
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
        if (server.state == "ALONE"):
            server.setState("WAITING")
            server.join(self)
        elif (self.factory.callback):
            if(isinstance(self.factory.callback,Message)):
                self.sendLine(self.factory.callback.tobytes())
            else:
                self.factory.callback(self)
        
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
    def __init__(self,server,callback=None):
        self.server = server
        self.callback = callback
        
    def buildProtocol(self, addr):
        return Chord(self)
    
    def startedConnecting(self,connector):
        print("Attempting to connect")
    
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