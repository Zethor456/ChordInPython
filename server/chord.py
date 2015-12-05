from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor
from twisted.protocols.basic import FileSender
from message import Message, Find, Notify, Join, Inform, Ping, Pong
from node import Node
import sys
import re
from hash import Ring

class ChordServer():
    def __init__(self,host,target):
        self.connections = {} #is a dict mapping node to connection
        self.nodes = {} #is a dict mapping connections to nodes
        self.predecessors = []
        self.successors = []
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
            if cmd == "show":
                print "Node{0.id} has:".format(self.me)
                print "Predecessor"
                for p in self.predecessors:
                    print p.toString()
                print "Successor"
                for s in self.successors:
                    print s.toString()
                print "Connections"
                for n in self.connections.keys():
                    print n.toString()
                
            else:
                for c in self.connections.itervalues():
                    c.sendLine(Message(cmd).tobytes())
                    
    def stabilize(self):
        print "Stabilizing"
        if(self.successors != [] and self.successors[0] in self.connections):
            self.connections[self.successors[0]].sendLine(Ping(self.me).tobytes())
        reactor.callLater(20,self.stabilize)#@UndefinedVariable

    def run(self):
        print("Initializing " + self.host.toString())
        reactor.connectTCP(self.target.ip,self.target.port,ChordFactory(self))#@UndefinedVariable
        reactor.listenTCP(self.host.port,ChordFactory(self))#@UndefinedVariable
        reactor.callInThread(self.readInput)#@UndefinedVariable
        reactor.callInThread(self.stabilize)#@UndefinedVariable
        reactor.run()#@UndefinedVariable
    ##########################
    #SERVER UTILITY FUNCTIONS#
    ##########################
    
    def add(self,node,connection):
        if(not isinstance(node,Node) or not isinstance(connection, Chord)):
            raise Exception()
        self.connections[node]=connection
        self.nodes[connection]=node
    
    def addd(self,connection,node):
        self.add(node, connection)
    
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
    
    #TODO, sort the predecessors as they come in
    #see setSucc for reference.
    def setPred(self,pred):
        print "Setting predecessor to {0}".format(pred.toString())
        self.predecessors.append(pred)
    
    #The successor list is sorted
    #according to the hash values
    #which unfortunately wraps around
    #so I've defined a function Hash.ascend
    #handle the math for the various cases
    def setSucc(self,succ):
        print "Setting successor to {0}".format(succ.toString())
        #new successor
        if (self.successors == []):
            self.successors.append(succ)
        #special case for started node.
        elif (self.successors[0]==self.me):
            self.successors[0]=succ
        #new 1st pos succesor
        elif (self.fingers.ascending(self.me, succ, self.successors[0])):
            self.successors.insert(0,succ)
        #2nd and on successors
        else:
            i = 1
            for s in self.successors:
                i = i + 1 
                if (self.fingers.acsending(self.me, s, succ)):
                    break
            self.successors.insert(i,succ)    
    
    def balencePred(self): 
        self.predecessors.sort(key = lambda node : node.sortingPred(self.me))
    ##########################
    #CHORD SPECIFIC FUNCTIONS#
    ##########################
    
    def query(self,aFile):
        print "Looking for {0}!".format(aFile)
        #TODO send the file query
    
    def notify(self, node):
        if(self.predecessors == [] or self.fingers.ascending(self.predecessors[0], node, self.me)):
            self.setPred(node)
    
    def prepareNotify(self,node):
        print "Notifying node{0}".format(node.id)
        if (node in self.connections):
            msg = Notify(self.me)
            self.setSucc(node)
            self.connections[node].sendLine(msg.tobytes())
        else:
            reactor.connectTCP(node.ip,node.port,ChordFactory(self,self.sendNotify,[node]))#@UndefinedVariable

    def sendNotify(self,connection,node):
        #Check in case successor has changed
        #Guard against possible race condition
        if(self.successors[0]==node):
            self.add(node, connection)
            self.setSucc(node)
            self.connections[node].sendLine(Notify(self.me).tobytes())
            
    def inform(self,node,successor):
        print "informing node of successor"
        node.sendLine(Inform(successor).tobytes())
 
    def find_successor(self,node,msg):
        #if there is only one node eg the pred==succ
        #or the key is between this node and it's successor
        #or the wrap around case when pred > succ
        key = self.fingers.key(msg.target.address())
        pred = self.fingers.pos(self.me)
        succ = self.fingers.pos(self.successors[0])
        if(pred==succ or (pred<key and key <succ and pred < succ) or (pred>succ and (key > pred or key < succ))):
            #use the current connection to inform
            if (msg.target == msg.node):
                self.add(msg.node, node)
                node.sendLine(Inform(self.successors[0]).tobytes())
            #create a connection back to the new node
            elif(not msg.target in self.connections):
                reactor.connectTCP(msg.target.ip,msg.target.port,ChordFactory(self,Inform(self.successors[0])))#@UndefinedVariable
            #or this node is already connected for some reason so use that
            else:
                self.inform(self.connections[msg.target])
            #update my successor to the new node
            self.setSucc(msg.target)
        else:
            #forward the join request to my successor
            self.connections[self.successors[0]].sendLine(Join(self.me,msg.target).tobytes())
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
        self.setSucc(msg.node)
        if(not msg.node in self.connections):
            reactor.connectTCP(msg.node.ip,msg.node.port,ChordFactory(self,self.addd,[msg.node]))#@UndefinedVariable
    
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
                self.notify(msg.node)
            elif isinstance(msg, Join):
                print "Received: {1} from node {2} for node {0}".format(msg.target.id,msg.msg,msg.node.id)
                self.find_successor(node,msg)
            elif isinstance(msg,Find):
                #TODO File querying behaviour in here
                #Server sends port for it's FileProtocol
                #to handle the actual data transfer
                print "Node {0} is searching for {1}".format(msg.node.id,msg.file)
            elif isinstance(msg,Ping):
                print "Received: {1} from node {0}".format(msg.node.id,msg.msg)
                if (self.predecessors == []):
                    node.sendLine(Pong(self.me,None).tobytes())
                else:
                    node.sendLine(Pong(self.me,self.predecessors[0]).tobytes());
            elif isinstance(msg, Pong):
                print "Received Pong from Node{0}".format(msg.source.id)
                if (msg.node == None):
                    print "Notifying successor about myself"
                    node.sendLine(Notify(self.me).tobytes())
                elif(msg.node == self.me):
                    return
                elif(self.fingers.ascending(self.me, msg.node, self.successors[0])):
                    self.prepareNotify(msg.node)
                else:
                    node.sendLine(Notify(self.me).tobytes())
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
                if(self.factory.args):
                    self.factory.callback(self,self.factory.args[0])
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
    def __init__(self,server,callback=None, args=[]):
        self.server = server
        self.callback = callback
        self.args = args
        
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
