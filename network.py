import socket
import sys
import NetworkMessageTypes
import re
from threading import *

MAX_CONNECTIONS = 5


def connection_listener(currentnode):
    servCtrl = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servCtrl.bind(('localhost',currentnode.ctrlport))
    servCtrl.listen(MAX_CONNECTIONS)
    
    while 1:
         conn, addr = servCtrl.accept()
         
         t = Thread(target=handlerFunction, args=(conn, addr,))
         t.start()
    
    
    
    return

def handlerFunction(conn,addr):
    
    data = conn.recv(4092)
    
    print(int.from_bytes(data, byteorder = 'big'))
         
    
    return