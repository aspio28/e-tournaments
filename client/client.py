import dill as pickle
import socket
import time
import random
import string
from utils import send_to, receive_from, send_and_wait_for_answer, get_dns_address, get_from_dns, send_addr_to_dns, send_ping_to, send_echo_replay 

def random_player_move(board, move):
    import random
    while True:
        i = random.randint(0,2)
        j = random.randint(0,2)
        if board[i][j] == -1:
            return i,j,move
   
def greedy_player_move(board, move):
    def check_win(board, move):
        # Check rows and columns
        for i in range(3):
            if board[i][0] == board[i][1] == move and board[i][2] == -1:
                return (i, 2)
            if board[i][0] == board[i][2] == move and board[i][1] == -1:
                return (i, 1)
            if board[i][1] == board[i][2] == move and board[i][0] == -1:
                return (i, 0)

            if board[0][i] == board[1][i] == move and board[2][i] == -1:
                return (2, i)
            if board[0][i] == board[2][i] == move and board[1][i] == -1:
                return (1, i)
            if board[1][i] == board[2][i] == move and board[0][i] == -1:
                return (0, i)

        # Check diagonals
        if board[0][0] == board[1][1] == move and board[2][2] == -1:
            return (2, 2)
        if board[0][0] == board[2][2] == move and board[1][1] == -1:
            return (1, 1)
        if board[1][1] == board[2][2] == move and board[0][0] == -1:
            return (0, 0)

        if board[0][2] == board[1][1] == move and board[2][0] == -1:
            return (2, 0)
        if board[0][2] == board[2][0] == move and board[1][1] == -1:
            return (1, 1)
        if board[1][1] == board[2][0] == move and board[0][2] == -1:
            return (0, 2)
    
    def random_move(board, move):
        import random
        while True:
            i = random.randint(0,2)
            j = random.randint(0,2)
            if board[i][j] == -1:
                return i, j, move
            
    #check if the player can wintest
    winning_move = check_win(board, move)
    if winning_move:
        return *winning_move, move
    
    #check if the player can prevent the oponent from winning
    opponent = 0 if move == 1 else 1
    blocking_move = check_win(board, opponent)
    if blocking_move:
        return *blocking_move, move
    return random_move(board,move)

 
player_types = {'random': random_player_move, 
                'greedy': greedy_player_move }
           
class ClientNode:
    port = 5000
    str_rep = 'Client'
    
    def __init__(self):
        
        self.requests = {'ping': send_echo_replay,
                        'Failed': None, }
        self.tourn = {}
        self.run()
        
    def _get_server_node_addr(self):
        # TODO Improve the adquisition of a Server, to avoid overloading bussy servers while others stay free
        return random.choice(get_from_dns('Server'))
    
    def retry_after_timeout(self, request):
        servers = get_from_dns('Server')
        for addr in servers:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(4)
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
    
    def new_tournament(self, type_of_tournament, list_of_players, tournament_name):
        server_address = self._get_server_node_addr()
        # begin tournament
        print(f"Requesting for a new {type_of_tournament} tournament to be created")
        request = pickle.dumps(['new_tournament', (type_of_tournament, list_of_players, tournament_name)])
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(4)
        sock.connect(server_address)
        all_good, data = send_and_wait_for_answer(request, sock, 10)
        sock.close()
        if len(data) == 0:
           all_good, data = self.retry_after_timeout(request)
            
        answer = pickle.loads(data)
        if answer[0] == 'running_tournament':
            return all_good, answer[1][0]
        if answer[0] == 'Failed':
            return False, None
        return all_good, None
        
    def get_status(self, tournament_id, tournament_name):
        server_address = self._get_server_node_addr()

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(4)
        sock.connect(server_address)
        
        print(f"Requesting the status of the tournament {tournament_name}")#['tournament_status', (tournament_id,)]
        request = pickle.dumps(['tournament_status', (tournament_id,)])
        all_good, data = send_and_wait_for_answer(request, sock, 10)
        sock.close()
        if len(data) == 0:
           all_good, data = self.retry_after_timeout(request)
        
        answer = pickle.loads(data)
        
        if answer[0] == 'Failed':
            print("Failed get status request")
            return False
        # print(answer)     
        self._pretty_print_tournament(*answer[1])

    def run(self):
        run = True
        while run:
            choice_str = """
What do you want to do?
        1. Create new Knockout Tournament
        2. Create new Free For All Tournament
        3. Check Tournament status
        4. Create new Knockout Tournament (preset)
        5. Create new Free For All Tournament (preset)
        6. Quit
        Choose a number: """
            players_n_str = """How many players there will be: """
            players_str = """Especify the type of player (random/greedy) and the name, separated by a space. Each player must be in a different line. 
    Example: 
        random Juan
        greedy Ana
    """
            choose_tournament = f"""Insert the name of the Tournament to check. The current Tournaments are: {list(self.tourn.keys())}
    """
            choice = input(choice_str)
            if choice in ["1", "2"]:    
                if choice == "1":
                    type_of_tournament = "Knockout"
                elif choice == "2":
                    type_of_tournament = "FreeForAll"
                    
                tournament_name = input("Enter tournament name: ")
                if tournament_name in self.tourn:
                    # Add a random alphanumeric string if name exists
                    random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=5))
                    tournament_name = tournament_name + "_" + random_string
                    print(f"New {type_of_tournament} Tournament name already exists. Using {tournament_name} instead.")
                    
                amount_of_players = input(players_n_str)
                try:
                    amount_of_players = int(amount_of_players)
                except:
                    print("The value of amout of players is not valid")        
                 
                list_of_players = []      
                print(players_str)          
                for i in range(amount_of_players): 
                    line = input()
                    type_of_player , name_of_player = line.split()
                    code = pickle.dumps(player_types[type_of_player])
                    list_of_players.append((code, name_of_player))
            elif choice == "5":
                type_of_tournament = "FreeForAll"
                random_code = pickle.dumps(player_types["random"])
                greedy_code = pickle.dumps(player_types["greedy"])
                list_of_players = [(random_code, "A"), (random_code, "B"), (greedy_code, "C"),(random_code, "D"),(random_code, "E"),(greedy_code, "H")]
                tournament_name = "preset_FreeForAll"
                if tournament_name in self.tourn:
                    # Add a random alphanumeric string if name exists
                    random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=5))
                    tournament_name = tournament_name + "_" + random_string
                    print(f"New FreeForAll Tournament name: {tournament_name}")
            elif choice == "4":
                type_of_tournament = "Knockout"
                random_code = pickle.dumps(player_types["random"])
                greedy_code = pickle.dumps(player_types["greedy"])
                list_of_players = [(random_code, "A"), (random_code, "B"), (greedy_code, "C"), (greedy_code, "D"),(random_code, "E"), (random_code, "F"), (greedy_code, "G"), (greedy_code, "H")]
                tournament_name = "preset_Knockout"
                if tournament_name in self.tourn:
                    # Add a random alphanumeric string if name exists
                    random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=5))
                    tournament_name = tournament_name + "_" + random_string
                    print(f"New Knockout Tournament name: {tournament_name}")   
            elif choice == "3":
                if len(self.tourn.items()) == 0:
                    print("Error, no tournaments created yet.")
                    continue
                t_name = input(choose_tournament)
                if not self.tourn.get(t_name, False):
                    print(f"Invalid Tournament {t_name}")
                    continue
                self.get_status(self.tourn[t_name], t_name)
                continue
            elif choice == "6":
                return
            else:
                print("Invalid value")
                continue
            
            try:
                all_good, t_id = self.new_tournament(type_of_tournament, list_of_players, tournament_name)
            except Exception as err:
                print(err, ". Unexpected error ocurred")
                continue                
            
            if all_good:
                print(f"New {type_of_tournament} Tournament created: {tournament_name}")#######
                self.tourn[tournament_name] = t_id
                time.sleep(2)
            else:
                print("Failed create tournament request")
                
                
    
    def _pretty_print_tournament(self, tournament_id, tournament_type, ended, all_matches, all_players):
        player_dict = {}
        for player in all_players:
            player_dict[player[0]] = {'id': player[0], 'name': player[1], 't_id': player[2]}
        
        if tournament_type == "Knockout":
            def print_tree(parent_matches, match_dict, match_id, level=0):
                match = match_dict[match_id]
                print('   ' * level, end='')

                # if the match is ended show the winner
                if match['ended']: 
                    print(f"Match {match_id}: {player_dict[match['p1']]['name']} vs. {player_dict[match['p2']]['name']}. Winner {player_dict[match['winner']]['name']}")
                else:
                    all_played = True
                    players = []
                    for required_id in match['required']:
                        if match_dict[required_id]['ended']:
                            players.append(match_dict[required_id]['winner'])
                        else:
                            all_played = False
                    # if all the required matches are ended but this match is not
                    if all_played and len(match['required']) > 0:
                        print(f"Match {match_id}: Player {player_dict[players[0]]['name']} vs. Player {player_dict[players[1]]['name']}")
                    # if any of the required matches is not ended 
                    else:
                        print(f"Match {match_id}: Waiting")

                # Recursively print child matches
                for child_id in parent_matches[match_id]:
                    print_tree(parent_matches, match_dict, child_id, level + 1)

            match_dict = {}
            for match in all_matches:
                required =  [int(id) for id in match[2].split(',')]  if match[2] != '' else []                
                match_dict[match[0]] = {'id': match[0], 't_id': match[1], 'required': required, 
                                        'ended': bool(match[3]), 'p1': match[4], 'p2': match[5], 'winner': match[6]}
            # Create a dictionary to store the parent match for each match
            parent_matches = {id:[] for id in match_dict}
            for match_id in match_dict:
                parent_matches[match_id] = match_dict[match_id]['required']
            print_tree(parent_matches, match_dict, all_matches[-1][0])
        elif tournament_type == 'FreeForAll':
            # Create a dictionary to store player wins
            player_wins = {p_id:0 for p_id in player_dict}
            match_dict = {}
            for match in all_matches:
                new_match = {'id': match[0], 't_id': match[1], 'ended': bool(match[2]), 
                             'p1': match[3], 'p2': match[4], 'winner': match[5]}
                match_dict[match[0]] = new_match
            
                if new_match['ended']:
                    player_wins[new_match['winner']] = player_wins[new_match['winner']] + 1
           
            # Create a list of tuples for sorting the table
            sorted_scores = sorted(player_wins.items(), key=lambda item: item[1], reverse=True)

            # Print the score table
            print("Score Table:")
            print("    Player     |   Matches Won")
            print("-------------- | ---------------")
            for player_id, wins in sorted_scores:
                print(f" {player_dict[player_id]['name']} |   {wins}")

            # Print match statistics
            ended_matches = sum(1 for match_id in match_dict if match_dict[match_id]['ended'])
            total_matches = len(all_matches)
            print(f"\nEnded matches: {ended_matches} of a total of {total_matches} matches.")
        else: raise Exception(f'Unknown tournament type {tournament_type}')
        
        if ended:
            print(f"Tournament ended.")

ClientNode()