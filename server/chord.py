from twisted.internet.protocol import Factory, Protocol
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor
from twisted.protocols.basic import FileSender
from message import Message, Find, Notify, Join, Inform, Ping, Pong
from node import Node
import sys
import re
from hash import Ring
import os

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
        self.path    = os.getcwd()
        self.files   = [] #A list of all the file from donwnload dir and shared
        self.sharedPath = "C:\Users\Guelor\My Documents\LiClipse Workspace\Chord\ChordInPython/userspace/Shared"
        self.downloadPath = "C:\Users\Guelor\My Documents\LiClipse Workspace\Chord\ChordInPython/userspace/Downloads"
        self.verbose = False
        self.requestedFileName =""
        self.indexes = {} #is a dict mapping of nodes to array of file names

    def readInput(self):
        print "Request files with: get [file]"
        print "Search available files with: ls [term]"
        pattern    = re.compile("get\s+(.+)$")
        patternTwo = re.compile("ls\s+(.+)$")
        while(True):
            cmd = raw_input()
            match = pattern.match(cmd)
            matchLs = patternTwo.match(cmd)
            if match:
                self.requestedFileName = match.group(1) # kind of hacked it for now...There's a better solution
                self.query(self.me,match.group(1), self.indexes)
            elif matchLs: #TODO: Need to traverse the tree and collect all available files that matches the search term
                print "ls: "+ matchLs.group(1)
                self.query(self.me, matchLs.group(1), self.indexes) 
            elif cmd == "show":
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
            elif cmd =="debug":
                self.verbose = not self.verbose
                if self.verbose:
                    print "Turned on log messages"
                else:
                    print "Turned off log messages"
            else:
                print "Sending string: "+cmd
                for c in self.connections.itervalues():
                    c.sendLine(Message(cmd).tobytes())
                   
                    
    def stabilize(self):
        self.log("Stabilizing")
        if(self.successors != [] and self.successors[0] in self.connections):
            self.connections[self.successors[0]].sendLine(Ping(self.me).tobytes())
        reactor.callLater(20,self.stabilize)#@UndefinedVariable

    def run(self):
        self.getSharedDirect()
        print("Initializing " + self.host.toString())
        reactor.connectTCP(self.target.ip,self.target.port,ChordFactory(self))#@UndefinedVariable
        reactor.listenTCP(self.host.port,ChordFactory(self))#@UndefinedVariable
        reactor.callInThread(self.readInput)#@UndefinedVariable
        reactor.callInThread(self.stabilize)#@UndefinedVariable
        reactor.listenTCP(self.host.filePort,FileFactory(self))
        reactor.run()#@UndefinedVariable
    ##########################
    #SERVER UTILITY FUNCTIONS#
    ##########################
    
    def log(self,msg):
        if self.verbose:
            print msg
    
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
    
    def query(self,node,aFile, index):
        newFind = Find(node, aFile, index)
        self.connections[self.successors[0]].sendLine(newFind.tobytes())
    
    def checkForFile(self,aFile):
        self.getSharedDirect()
        temp = []
        print "Looking for {0}!".format(aFile)
        for i in self.files:
            if aFile == i:
                temp.append(i)
                print "200: Your request for {0} was found!".format(aFile)
                return True
            else:
                searchedTerm = re.search(aFile, i)
                if searchedTerm:
                    print "There's a macth!"
                else:
                    print "There's no match!" 
                    return False
        if not temp:
            print "404: Your request for {0} was not found!".format(aFile)
            return False

    #List all the file names in the Shared directory
    def getSharedDirect(self):
        self.log("Getting my Files")
        del self.files[:]
        self.log("Root is: " + os.getcwd())
        self.files = os.listdir(self.sharedPath)
        for f in self.files:
            self.log("Found " + f)
            
    def fileExist(self, message): #TODO keep sending the file request along the chain #When the file is found connect to the node and send the file
        self.getSharedDirect()
        node = message.node
        for f in self.files:
            if ( message.file == f):
                p=open(self.sharedPath+"/"+message.file, "rb")
                reactor.connectTCP(node.ip,node.filePort,FileFactory(self, self.sendFile, [p]))
                print "200***: The requested file {0} was found!".format(message.file)
                return
            else:
                print "200***: Searching for {0}!".format(message.file)
                #TODO: Need to grab at least 9 files that this current node has plus the matched one
                searchedTerm = re.search(message.file, f)
                if searchedTerm:
                    self.indexes[node] = self.files
                    for key in  self.indexes:
                        if True: # need a better condition, just testing for now
                            self.query(node, message.file, self.indexes)
                            print "filePort1: {}!".format(key.filePort)
                            print "filePort1: {}!".format(node.filePort)
                            print "Length: {}".format(len(self.indexes))
                            #return
                    print "Key Value: {0}".format(self.indexes[key])
                    #print "key: %s , value: %s" % (key.filePort,  self.indexes[key])
                    return
        #if the file isn't found, forward the request
        self.query(node, message.file, self.indexes)
        
    
    def sendFile(self, connection, aFile):
        connection.sender = FileSender()
        deffered = connection.sender.beginFileTransfer(aFile, connection.transport, lambda x: x) #check me later
        deffered.addCallback(lambda r: connection.transport.loseConnection())
        
        
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
                if(msg.node == self.me):
                    print "{0} not found on network".format(msg.file)
                else:
                    self.fileExist(msg)
                    print "Node {0} wants file: {1}".format(msg.node.id,msg.file)
            elif isinstance(msg,Ping):
                self.log("Received: {1} from node {0}".format(msg.node.id,msg.msg))
                if (self.predecessors == []):
                    node.sendLine(Pong(self.me,None).tobytes())
                else:
                    node.sendLine(Pong(self.me,self.predecessors[0]).tobytes());
            elif isinstance(msg, Pong):
                self.log("Received Pong from Node{0}".format(msg.source.id))
                if (msg.node == None):
                    self.log("Notifying successor about myself")
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


class FileProtocal(Protocol):
    def __init__(self,factory):
        self.factory = factory
        self.rcv = None
        
    def connectionMade(self):
        if (self.factory.callback):
            if(self.factory.args):
                self.factory.callback(self,self.factory.args[0])
            else:
                self.factory.callback(self)
        else:
            #Store the file io object so we can write the data later
            #TODO actually set the correct name of the file...
            #arguably this should be sent before the actual data in a Message...
            #maybe change this to a Line Receiver and in connection Made
            #wait for a string to be sent
            #hacked  it for now... still need work
            self.rcv = open(self.factory.server.downloadPath+"/"+ self.factory.server.requestedFileName, "wb")

    def connectionLost(self, reason):
        if (not self.rcv == None):
            print "File transfer complete"
            self.rcv.close()
        else:
            print reason.getErrorMessage()
    
    def dataReceived(self, data):
        self.rcv.write(data)
    
class FileFactory(Factory):
    def __init__(self, server, callback=None, args=[]):
        self.server   = server
        self.callback = callback
        self.args     = args
        
    def buildProtocol(self, addr):
        return FileProtocal(self)
    
    def startedConnecting(self,connector):
        print("Attempting to transfer file")
    
    def clientConnectionFailed(self,transport,reason):
        print("Attempt to transfer file failed")
        
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
