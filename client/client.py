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
        # TODO Improve the adquisition of a Server, to avoid overloading bussy servers while others stay free
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
        print(f"Requesting for a new {type_of_tournament} tournament to be created")
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
        
        print(f"Requesting the status of the tournament with id {tournament_id}")#['tournament_status', (tournament_id,)]
        request = pickle.dumps(['tournament_status', (tournament_id,)])
        all_good, data = send_and_wait_for_answer(request, sock, 10)
        sock.close()
        if len(data) == 0:
           all_good, data = self.retry_after_timeout(request)
        
        answer = pickle.loads(data)
        # print(answer)     
        self._pretty_print_tournament(*answer[1])

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
            
            print("Especify the type of player (random/greedy) and the name, separated by a space. (Each player will be a different input)")   
                
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
    
    def _pretty_print_tournament(self, tournament_id, tournament_type, ended, all_matches, all_players):
        player_dict = {}
        for player in all_players:
            player_dict[player[0]] = {'id': player[0], 'name': player[1], 'player_type':player[2], 't_id': player[3]}
        
        if tournament_type == "Knockout":
            def print_tree(parent_matches, match_dict, match_id, level=0):
                match = match_dict[match_id]
                print('  ' * level, end='')

                # if the match is ended show the winner
                if match['ended']: 
                    print(f"Match {match_id}: Winner {player_dict[match['winner']]}")
                else:
                    all_played = True
                    players = []
                    for required_id in match['required']:
                        if match_dict[required_id]['ended']:
                            players.append(match_dict[required_id]['winner'])
                        else:
                            all_played = False
                    # if all the required matches are ended but this match is not
                    if all_played:
                        print(f"Match {match_id}: Player {player_dict[players[0]]} vs. Player {player_dict[players[1]]}")
                    # if any of the required matches is not ended 
                    else:
                        print(f"Match {match_id}: Waiting")

                # Recursively print child matches
                for child_id in parent_matches[match_id]:
                    print_tree(parent_matches, match_dict, child_id, level + 1)

            match_dict = {}
            for match in all_matches:
                match_dict[match[0]] = {'id': match[0], 't_id': match[1], 'required': match[2], 
                                        'ended': bool(match[3]), 'p1': match[4], 'p2': match[5], 'winner': match[6]}
            # Create a dictionary to store the parent match for each match
            parent_matches = {id:[] for id in match_dict.keys()}
            for id, match in match_dict:
                for required_id in match['required']:
                    parent_matches[required_id].append(id)

            print_tree(parent_matches, match_dict, all_matches[-1][0])
        elif tournament_type == 'FreeForAll':
            # Create a dictionary to store player wins
            player_wins = {p_id:0 for p_id in player_dict.keys()}
            match_dict = {}
            for match in all_matches:
                new_match = {'id': match[0], 't_id': match[1], 'ended': bool(match[2]), 
                                'p1': match[3], 'p2': match[4], 'winner': match[5]}
                match_dict[match[0]] = new_match
            
                if new_match['ended']:
                    player_wins[new_match['winner']] = player_wins[new_match['winner']] + 1
            print(match_dict)
            # Create a list of tuples for sorting the table
            sorted_scores = sorted(player_wins.items(), key=lambda item: item[1], reverse=True)

            # Print the score table
            print("Score Table:")
            print("    Player     |   Matches Won")
            print("-------------- | ---------------")
            for player_id, wins in sorted_scores:
                print(f" {player_dict[player_id]['name']}/{player_dict[player_id]['player_type']} |   {wins}")

            # Print match statistics
            ended_matches = sum(1 for match_id in match_dict if match_dict[match_id]['ended'])
            total_matches = len(all_matches)
            print(f"\nEnded matches: {ended_matches} of a total of {total_matches} matches.")
        else: raise Exception(f'Unknown tournament type {tournament_type}')
        
        if ended:
            print(f"Tournament {tournament_id} ended.")
ClientNode()