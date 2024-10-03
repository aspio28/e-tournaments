import socket
import time
import dill as pickle
import multiprocessing
from TicTacToe import *
from utils import DNS_ADDRESS, send_to, receive_from, send_and_wait_for_answer, get_from_dns, send_addr_to_dns, send_ping_to, send_echo_replay 
           
def get_players_instances(player_ids, address):
    sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    sock.connect(address) 
    request = pickle.dumps(['get_player', (player_ids,)])
    print(f"Requesting the players id to the database node in {address}")################
    all_good, data = send_and_wait_for_answer(request, sock, 4)
    sock.close()
    if len(data) == 0:
        print("Retrying with all the database nodes")##############                      
        data_nodes = get_from_dns('DataBase')
        for addr in data_nodes:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect(addr)
                print(f"Requesting the players id to the database node in {addr}")################
                all_good, data = send_and_wait_for_answer(request, sock, 10)
                sock.close()
                if len(data) != 0:
                    break
            except Exception as err:
                print(err, ". Failed retry after timeout") 
        
    records = pickle.loads(data) [1]
    return [(name, pickle.loads(player_code)) for id, name, player_code in records]

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
        self.run()
        
    def run(self):
        while True:
            try:                    
                result = send_addr_to_dns(self.str_rep, self.address)
                if result: 
                    break
            except Exception as err:
                print(err)    
                
        processes = []
        try:
            while True:                
                conn, address = self.serverSocket.accept()
                print('Received CONNECTION from: ', address)
                process = multiprocessing.Process(target=self.handle_connection, args=(conn, address))
                processes.append(process)
                process.start()
        except Exception as err:
            print(err)
        finally:
            self.serverSocket.close()
            for process in processes:
                if process.is_alive():
                    process.terminate()
                    process.join()
              
    def handle_connection(self, connection: socket, address):
        status = False
        received = receive_from(connection, 3)
        if len(received) == 0:
            print("Failed request, data not received") 
            connection.close()
            return status
        try:
            decoded = pickle.loads(received)
            if self.requests.get(decoded[0]):
                function_to_answer = self.requests.get(decoded[0])
                status = function_to_answer(decoded[1], connection, address)

        except Exception as err:
            print(err, ". Failed request ->",decoded[0]) 
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
        name, fun = players[0]
        player1 = Player(name, fun)
        name, fun = players[1]
        player2 = Player(name, fun)
        winner = TicTacToe(player1, player2).Run()[2]
        print(f"{player1} vs {player2}. winner {winner}")
        if winner == player1:
            match_winner_id = p1_id
        elif winner == player2:
            match_winner_id = p2_id
        else: raise Exception(f"The winner must be one of the players {p1_id}, {p2_id}")
        
        return match_winner_id

    def execute_match(self, arguments: tuple, connection, address):
        p1_id, p2_id = arguments
        match_winner = self._do_a_match(p1_id, p2_id)
        request = pickle.dumps(['match_result', match_winner])
        all_good = send_to(request, connection)
        return all_good
            
ip_address = input("Insert the node ip-address: ")#Range?
node = MinionNode(ip_address)  
