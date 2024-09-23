import pickle
import socket
import time
import random
from utils import DNS_ADDRESS, send_to, receive_from, send_and_wait_for_answer, get_from_dns, send_addr_to_dns, send_ping_to, send_echo_replay 


class ClientNode:
    port = 5000
    str_rep = 'Client'
    
    def __init__(self):
        
        self.requests = {'ping': send_echo_replay,
                        'Failed': None,                        
                        }
        self.run()
        
    def _get_server_node_addr(self):
        #TODO Improve the adquisition of a Server, to avoid overloading bussy servers while others stay free
        return random.choice(get_from_dns('Server'))
    
    def retry_after_timeout(self, request):
        servers = get_from_dns('Server')
        for addr in servers:
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
    
    def new_tournament(self, type_of_tournament, list_of_players):
        server_address = self._get_server_node_addr()
        # begin tournament
        request = pickle.dumps(['new_tournament', (type_of_tournament, list_of_players)])
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(server_address)
        all_good, data = send_and_wait_for_answer(request, sock, 10)
        sock.close()
        if len(data) == 0:
           all_good, data = self.retry_after_timeout(request)
            
        answer = pickle.loads(data)
        if answer[0] == 'running_tournament':
            return all_good, answer[1][0]
        return all_good, None
        
    def get_status(self, tournament_id):
        server_address = self._get_server_node_addr()

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(server_address)
        
        print(['tournament_status', (tournament_id,)])
        request = pickle.dumps(['tournament_status', (tournament_id,)])
        all_good, data = send_and_wait_for_answer(request, sock, 10)
        sock.close()
        if len(data) == 0:
           all_good, data = self.retry_after_timeout(request)
        
        answer = pickle.loads(data)
        print(answer)      

    def run(self):
        first_input = input("Do you want to create a new tournament? (Y/N)")
        if first_input == "Y":
                    
            type_of_tournament = input("What kind of tournament it will be? (1 = Knockout, 2 = FreeForAll)")
            
            if type_of_tournament == "1":
                type_of_tournament = "Knockout"
            elif type_of_tournament == "2":
                type_of_tournament = "FreeForAll"
            else:
                print("Invalid value")
                return
                
            amount_of_players = input("How many players there will be: ")
            
            try:
                amount_of_players = int(amount_of_players)
            except:
                print("The value of amout of players wasnt valid")
                return        
            
            print("Especify the type of player (random/greedy) and the name , separated by a space. (Each player will be a different input)")   
                
            list_of_players = []
            
            for i in range(amount_of_players):
                
                line = input()
                type_of_player , name_of_player = line.split()
                list_of_players.append((type_of_player, name_of_player))
        elif first_input == "1":
            type_of_tournament = "FreeForAll"
            list_of_players = [("random","A"),("random","B"),("greedy","C")]
        else:
            return    
        all_good, t_id = self.new_tournament(type_of_tournament, list_of_players)
        time.sleep(10)
        #Small test
        self.get_status(t_id)
        
ClientNode()