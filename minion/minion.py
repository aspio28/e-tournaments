import socket
import time
import pickle
from TournamentsLogic import *

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
listen_addr = ("172.18.0.30",8020)
sock.bind(listen_addr)
sock.listen()

Stuart = Minion()
server_addr = ('172.18.0.20', 8080)

send_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
send_sock.connect(server_addr) #Going to have to change i guess, need to know who it needs to connect
send_sock.sendto(pickle.dumps(("Banana?",listen_addr)),server_addr)
send_sock.close()

while True:
    conn, addr = sock.accept()
    data = conn.recv(1024)
    match = pickle.loads(data)
    
    Stuart.do_a_match(match)
    send_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    send_sock.connect(server_addr) #Going to have to change i guess, need to know who it needs to connect
    send_sock.sendto(pickle.dumps("Le Poofe Guacamole"),server_addr)
    send_sock.close()