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

class Notify(Message):
    def __init__(self,node):
        Message.__init__(self, "i_think_im_your_successor")
        self.node = node
        
class Find(Message):
    def __init__(self,node,aFile):
        Message.__init__(self, "file_request")
        self.node = node
        self.file = aFile

class Join(Message):
    def __init__(self,node):
        Message.__init__(self, "find_my_successor")
        self.node = node
        
class Hello(Message):
    def __init__(self,node):
        Message.__init__(self, "connecting!")
        self.node = node