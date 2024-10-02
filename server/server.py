import pickle
import socket
import time
import multiprocessing
from TournamentsLogic import *
from utils import DNS_ADDRESS, send_to, receive_from, send_and_wait_for_answer, get_from_dns, send_addr_to_dns, send_ping_to, send_echo_replay 
import os

tournaments_type = {'Knockout': KnockoutTournament,
                   'FreeForAll': FreeForAllTournament}

class ServerNode:
    port = 8080
    str_rep = 'Server'
    
    def __init__(self):
        
        self.requests = {'ping': send_echo_replay,
                        'Failed': None,
                        'new_tournament': self.new_tournament,
                        'tournament_status': self.tournament_status,
                        'continue_tournament': self.continue_tournament,                        
                        }
        self.data_nodes = []
        self.minion_nodes = []
        self.address = (os.getenv('NODE_IP'), self.port)
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
            print(err, ". Failed request ->", decoded[0]) 
            answer = pickle.dumps(['Failed', (None,)])
            send_to(answer, connection)
                 
        finally:
            connection.close()
        return status
    
    def _get_minion_node_addr(self):
        minion_nodes = get_from_dns('Minion')
        return random.choice(minion_nodes)
    
    def _get_data_node_addr(self):
        data_nodes = get_from_dns('DataBase')
        return random.choice(data_nodes)
    
    def retry_after_timeout(self, request, addresses):
        for addr in addresses:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect(addr)
                all_good, data = send_and_wait_for_answer(request, sock, 10)
                sock.close()
                if len(data) != 0:
                    break
            except Exception as err:
                print(err, ". Failed retry after timeout") 
                
        if len(data) == 0:
            return False, None
        return True, data
    
    def _execute_tournament(self, tournament: Tournament):
        ended = False
            
        while not ended:
            ended, match = tournament.next_match()
            match:Match
            # print(ended, match)
            if ended:
                break
                            
            minion_addr = self._get_minion_node_addr()
            request = pickle.dumps(['execute_match', (match.player1, match.player2), self.address])
            
            send_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            send_sock.connect(minion_addr)
            
            all_good, data = send_and_wait_for_answer(request, send_sock, 10)
            send_sock.close()
            if len(data) == 0:              
                minion_nodes = get_from_dns('Minion')
                all_good, data = self.retry_after_timeout(request, minion_nodes)

            match_winner_id = pickle.loads(data) [1]
            match.ended = True # The database could be optimized removing ended and using the winner as a ended (if winner not None => ended is True)
            match.winner = match_winner_id
            match.save_to_db(self._get_data_node_addr()) 
            
        tournament.ended = True
        # print('tournament.ended = True')
        sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
        sock.connect(self._get_data_node_addr())

        request = pickle.dumps(['save_tournament', (tournament.id, tournament.tournament_type(), tournament.ended)])
        print("saving tournament", ['save_tournament', (tournament.id, tournament.tournament_type(), tournament.ended)])
        all_good, data = send_and_wait_for_answer(request, sock, 5)
        sock.close()
        if len(data) == 0:              
            data_nodes = get_from_dns('DataBase')
            all_good, data = self.retry_after_timeout(request, data_nodes)

        answer = pickle.loads(data)
        if answer[0] == 'saved_tournament':
            return True
        return all_good
    
    def new_tournament(self, arguments: tuple, connection, address):
        tournament_type, players = arguments 
        # time.sleep(20)
        # players is a list of tuples (player_type, player_name)
        tournament_instance = tournaments_type[tournament_type] (start=True, id=None, players=players)
        request = pickle.dumps(['running_tournament', (tournament_instance.id,)])
        all_good = send_to(request, connection)
        # time.sleep(20)
        
        return self._execute_tournament(tournament_instance)

    def continue_tournament(self, arguments: tuple, connection, address):
        tournament_type, tournament_id = arguments 
        tournament_instance = tournaments_type[tournament_type] (start=False, id=tournament_id, players=None)
        request = pickle.dumps(['running_tournament', (tournament_instance.id,)])
        all_good = send_to(request, connection)
        
        return self._execute_tournament(tournament_instance)
    
    def tournament_status(self, arguments: tuple, connection, address):
        tournament_id = arguments[0]
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(self._get_data_node_addr())
        print(['get_tournament_status', (tournament_id, ), self.address])
        request = pickle.dumps(['get_tournament_status', (tournament_id, ), self.address])
        all_good, data = send_and_wait_for_answer(request, sock, 4)
        sock.close()
        if len(data) == 0:              
            data_nodes = get_from_dns('DataBase')
            all_good, data = self.retry_after_timeout(request, data_nodes)
        tournament_id, tournament_type, ended, all_matches, all_players = pickle.loads(data)[1]
        answer = pickle.dumps(['tournament_status', (tournament_id, tournament_type, ended, all_matches, all_players), self.address])
        all_good = send_to(answer, connection)
        return all_good
        
node = ServerNode()  
