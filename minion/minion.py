import socket
import time
import pickle
from Players import *
from TicTacToe import *
from utils import DNS_ADDRESS, send_to, receive_from, send_and_wait_for_answer, get_from_dns, send_addr_to_dns, send_ping_to, send_echo_replay 

 
player_types = {'random': RandomPlayer, 
                'greedy': GreedyPlayer
                }
           
def get_players_instances(player_ids, address):
    sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    sock.connect(address) # TODO
    request = pickle.dumps(['get_player', (player_ids,)])
    all_good, data = send_and_wait_for_answer(request, sock, 4)
    records = pickle.loads(data) [1]
    return [(player_type, name) for id, name, player_type in records]

class MinionNode:
    port = 8020
    str_rep = 'Minion'
    
    def __init__(self, server_ip: str):
        
        self.requests = {'ping': send_echo_replay,
                        'Failed': None,
                        'execute_match': self.execute_match,
                        }
        self.data_nodes = []
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
    
    def _get_data_node_addr(self):
        if len(self.data_nodes) == 0:
            self.data_nodes = get_from_dns('DataBase')
        return random.choice(self.data_nodes)
    
    def _do_a_match(self, p1_id, p2_id):
        players = get_players_instances([p1_id, p2_id], self._get_data_node_addr())
        type, name = players[0]
        player1:Player =  player_types[type](name)
        type, name = players[1]
        player2:Player = player_types[type](name)
        winner = TicTacToe(player1, player2).Run()[2]
        print(winner)
        if winner == player1:
            match_winner_id = p1_id
        elif winner == player2:
            match_winner_id = p1_id
        else: raise Exception(f"The winner must be one of the players {p1_id}, {p2_id}")
        
        return match_winner_id

    def execute_match(self, arguments: tuple, connection, address):
        p1_id, p2_id = arguments
        match_winner = self._do_a_match(p1_id, p2_id)
        request = pickle.dumps(['match_result', match_winner])
        all_good = send_to(request, connection)
        return all_good
            
ip_address = input("Insert the node ip-address: ")
node = MinionNode(ip_address)  

# sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# listen_addr = ("172.18.0.30",8020)
# sock.bind(listen_addr)
# sock.listen()

# Stuart = Minion()
# server_addr = ('172.18.0.20', 8080)

# send_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# send_sock.connect(server_addr) #Going to have to change i guess, need to know who it needs to connect
# send_sock.sendto(pickle.dumps(("Banana?",listen_addr)),server_addr)
# send_sock.close()

# while True:
#     conn, addr = sock.accept()
#     data = conn.recv(1024)
#     match = pickle.loads(data)
    
#     Stuart.do_a_match(match)
#     send_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     send_sock.connect(server_addr) #Going to have to change i guess, need to know who it needs to connect
#     send_sock.sendto(pickle.dumps("Le Poofe Guacamole"),server_addr)
#     send_sock.close()