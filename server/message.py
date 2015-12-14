import pickle

class Message():
    def __init__(self,msg):
        self.msg = msg
    def tobytes(self):
        return Message.serialize(self)
    @staticmethod
    def serialize(msg):
        return pickle.dumps(msg)
    @staticmethod
    def deserialize(msg):
        return pickle.loads(msg)

class NotifySuc(Message):
    def __init__(self,node):
        Message.__init__(self, "i_think_you_are_my_pred")
        self.node = node

class Inform(Message):
    def __init__(self,node):
        Message.__init__(self, "i_think_i_am_your_successor")
        self.node = node
        
class NotifyPred(Message):
    def __init__(self,node):
        Message.__init__(self, "i_think_im_your_predecessor")
        self.node = node
        
class Notify(Message):
    def __init__(self,node):
        Message.__init__(self, "i_think_you_are_my_pred")
        self.node = node
        
class Find(Message):
    def __init__(self,node,aFile,aIndex):
        Message.__init__(self, "file_request")
        self.node   = node
        self.file   = aFile
        self.index  = aIndex
        
class Ping(Message):
    def __init__(self,node):
        Message.__init__(self, "are_you_here?")
        self.node = node

class Pong(Message):
    def __init__(self,source,node=None):
        Message.__init__(self, "I_am_here")
        self.source = source
        self.node = node

class Join(Message):
    def __init__(self,node,target):
        Message.__init__(self, "find_my_successor")
        self.node = node
        self.target = target
        
class FindSuccessor(Message):
    def __init__(self,node,target):
        Message.__init__(self, "find_my_successor")
        self.node = node
        self.target = target
