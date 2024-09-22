import pickle
import socket
import time
from TournamentsLogic import *
from Players import *
from utils import DNS_ADDRESS, send_to, receive_from, send_and_wait_for_answer, get_from_dns, send_addr_to_dns, send_ping_to, send_echo_replay 

tournament_type = {'Knockout': KnockoutTournament,
                   'FreeForAll': FreeForAllTournament}

class ServerNode:
    port = 8080
    str_rep = 'Server'
    
    def __init__(self, server_ip: str):
        
        self.requests = {'ping': send_echo_replay,
                        'Failed': None,
                        }
        
        self.address = (server_ip, self.port)
        self.serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.serverSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.serverSocket.bind(self.address)
        print(f"Listening at {self.address}")
        self.serverSocket.listen(5)
        
        while True:
            try:                    
                result = send_addr_to_dns(self.str_rep, self.address)
                if result: 
                    break
            except Exception as err:
                print(err)    
        try:
            while True:                
                conn, address = self.serverSocket.accept()
                print('CONNECTED: ', address)
                self.attend_connection(conn, address)
        finally:
            self.serverSocket.close()
              
    def attend_connection(self, connection: socket, address):
        status = False
        received = receive_from(connection, 3)
        try:
            decoded = pickle.loads(received)
            if self.requests.get(decoded[0]):
                function_to_answer = self.requests.get(decoded[0])
                status = function_to_answer(decoded[1], connection, address)

        except Exception as err:
            print(err, "Failed request") 
            answer = pickle.dumps(['Failed', (None,)])
            send_to(answer, connection)
                 
        finally:
            connection.close()
        return status
               

ip_address = input("Insert the node ip-address: ")
node = ServerNode(ip_address)  
      
player_types = {'random': RandomPlayer, 
                'greedy': GreedyPlayer
                }


sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

sock.bind(('172.18.0.20', 8080))
sock.listen(1)

minion_address_list = []
minion_busy_list = []
while True:
    
    conn, addr = sock.accept()
    
    data = conn.recv(1024)
    data = pickle.loads(data)
    print(data)
    if data[0] == "Banana?":
        minion_address_list.append(data[1])
        
        # To remove repeated instances of a minion address (may need a better fix)
        temporal_list = minion_address_list
        minion_address_list = []
        [minion_address_list.append(x) for x in temporal_list if x not in minion_address_list]
    
    else:            
        with conn:
            
            if data[0] == "Create new tournament":
                players = []
                for i in range(len(data[2])):
                    players.append(player_types[data[2][i][0]](data[2][i][1]))
                    
                tournament = tournament_type[data[1]](True ,players=players)
            
            ended = False
            
            while not ended:
                ended, match = tournament.next_match()
                if ended:
                    break
                
                while len(minion_address_list) == 0:
                    time.sleep(1)
                    print("Waiting for free minions")
                               
                minion = minion_address_list.pop()
                match = pickle.dumps(match)
                print(minion)
                send_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                send_sock.connect(minion) #Going to have to change i guess, need to know who it needs to connect
                send_sock.sendto(match,minion)
                send_sock.close()
                
                minion_busy_list.append(minion)
                
                conn, addr = sock.accept()
                data = conn.recv(1024)
                data = pickle.dumps(data)
                
                if data == "Le Poofe Guacamole":
                    minion_busy_list.pop(minion_busy_list.index(addr))
                    minion_address_list.append(addr)

            print(f'Recibido del cliente: {tournament}')
            
            #conn.sendto(b'Hola cliente!', addr)
            time.sleep(1)
            
