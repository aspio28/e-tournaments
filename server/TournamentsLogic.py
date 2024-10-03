from abc import ABC, abstractmethod
from collections import deque
import random, time
import pickle
import socket
import copy
from utils import DNS_ADDRESS, send_to, receive_from, send_and_wait_for_answer, get_from_dns, send_addr_to_dns, send_ping_to, send_echo_replay 

def retry_after_timeout(request, addresses):
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
            
class Match(ABC):
    @abstractmethod
    def __init__(self, tournament_id:int, required:list, ended:bool=False, player1:int=None, player2:int=None, winner:int=None, id:int=None):
        pass
   
    @abstractmethod
    def match_from_db(t_id, m_id, address):
        pass
   
    @abstractmethod
    def save_to_db(self, address):
        pass

    def __str__(self):
        return f"Match {self.id} of the {self.tournament} tournament. {self.player1} vs {self.player2}.  Winner: {self.winner}"
    def __repr__(self):
        return self.__str__()
    
class KnockoutMatch(Match):
    def __init__(self, tournament_id:int, required:list, ended:bool=False, player1:int=None, player2:int=None, winner:int=None, id:int=None):
        if len(required) == 0:
            # there are no requirements for this match
            self.is_leaf = True
        else: 
            self.is_leaf = False
        self.tournament = tournament_id
        self.ended = ended
        if self.is_leaf and (player1 == None or player2 == None): # TODO test
            raise Exception("A match with no requirements must have players")
        self.player1 = player1
        self.player2 = player2
        self.winner = winner
        self.required = required
        self.id = id

    def __str__(self):
        return f"Match {self.id} of the {self.tournament} Knockout tournament. {self.player1} vs {self.player2}.  Winner: {self.winner}"
    def __repr__(self):
        return self.__str__()
    
    def match_from_db(t_id, m_id, address):
        sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
        sock.connect(address) # TODO
        
        request = pickle.dumps(['get_match', ('KnockoutMatches', t_id, m_id)])
        all_good, data = send_and_wait_for_answer(request, sock, 4) # [(7, 6, '', 0, 24, 21)]
        sock.close()
        if len(data) == 0:              
            data_nodes = get_from_dns('DataBase')
            all_good, data = retry_after_timeout(request, data_nodes)
        
        record = pickle.loads(data) [1]
        id, tournament_id, required, ended, player1, player2, winner = record
        required = required.split(',') if required != '' else []
        ended = bool(ended)
        return KnockoutMatch(tournament_id, required, ended, player1, player2, winner, id)
    
    def save_to_db(self, address):
        sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
        sock.connect(address) # TODO
        
        # Convert the list of required match IDs to a comma-separated string
        required_str = ','.join(map(str, self.required)) if self.required else ''
        request = pickle.dumps(['save_match', ('KnockoutMatches', self.id, (self.tournament, required_str, self.ended, self.player1, self.player2, self.winner))])
        all_good, data = send_and_wait_for_answer(request, sock, 4)
        sock.close()
        if len(data) == 0:              
            data_nodes = get_from_dns('DataBase')
            all_good, data = retry_after_timeout(request, data_nodes)
            
        answer = pickle.loads(data)
        if answer[0] == 'saved_match':
            self.id = answer[1]

class FreeForAllMatch(Match):
    def __init__(self, tournament_id:int, ended:bool, player1:int, player2:int, winner:int=None, id:int=None):
        self.tournament = tournament_id
        self.ended = ended
        if player1 == None or player2 == None:
            raise Exception("A match with no requirements must have players")
        self.player1 = player1
        self.player2 = player2
        self.winner = winner
        self.id = id

    def __str__(self):
        return f"Match {self.id} of the {self.tournament} Free for all tournament. {self.player1} vs {self.player2}.  Winner: {self.winner}"
    def __repr__(self):
        return self.__str__()
    
    def match_from_db(t_id, m_id, address):
        print('match_from_db ',t_id, m_id)
        sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
        sock.connect(address) # TODO
        
        request = pickle.dumps(['get_match', ('FreeForAllMatches', t_id, m_id)])
        all_good, data = send_and_wait_for_answer(request, sock, 4)
        sock.close()
        if len(data) == 0:              
            data_nodes = get_from_dns('DataBase')
            all_good, data = retry_after_timeout(request, data_nodes)
            
        record = pickle.loads(data) [1]  # [(7, 6, '', 0, 24, 21)]
        id, tournament_id, ended, player1, player2, winner = record
        ended = bool(ended)
        return FreeForAllMatch(tournament_id, ended, player1, player2, winner, id)
    
    def save_to_db(self, address):
        sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
        sock.connect(address) # TODO
        
        request = pickle.dumps(['save_match', ('FreeForAllMatches', self.id, (self.tournament, self.ended, self.player1, self.player2, self.winner))])
        all_good, data = send_and_wait_for_answer(request, sock, 4)
        sock.close()
        if len(data) == 0:              
            data_nodes = get_from_dns('DataBase')
            all_good, data = retry_after_timeout(request, data_nodes)
            
        answer = pickle.loads(data)
        if answer[0] == 'saved_match':
            self.id = answer[1]
    
class Tournament(ABC):
    @abstractmethod
    def __init__(self, start:bool, id:int=None, players:list=None):
        pass
   
    @abstractmethod
    def tournament_type(self):
        pass
   
    @abstractmethod
    def insert_tournament_to_db(self, players:list):
        pass

# class BridgeTournament(Tournament):
#     def __init__(self, start:bool, id:int=None, players:list=None):
#         if start:
#             self.insert_tournament_to_db(players)
#             self.players_list = copy.deepcopy(self.players_ids)
#             random.shuffle(self.players_list)
#         self.current_bracket = copy.deepcopy(self.players_list)
#         self.next_bracket = []
#         self.pending = []
#
#     def insert_tournament_to_db(self, players:list):
#         if not os.path.exists(db_path):
#             create_db(db_path)
#         tournaments_columns = ['id INTEGER PRIMARY KEY AUTOINCREMENT', 'tournament_type TEXT NOT NULL', 'ended BOOLEAN NOT NULL']
#         create_table(db_path, 'tournaments', tournaments_columns)
#         self.id = insert_rows(db_path, 'tournaments', 'tournament_type, ended', (("bridge",False),)) [0]
#         participants_columns = ['id INTEGER PRIMARY KEY AUTOINCREMENT', 'name TEXT NOT NULL', 'player_type TEXT NOT NULL', 'tournament_id INTEGER', 'FOREIGN KEY (tournament_id) REFERENCES tournaments (id) ON DELETE CASCADE']
#         create_table(db_path, 'participants', participants_columns)
#         players_tuple = tuple((player.name, player.my_type(), self.id) for player in players)
#         self.players_ids = insert_rows(db_path, 'participants', 'name, player_type, tournament_id', players_tuple)       
#         # print(self.players_ids)
#
#     def initial_check(self):
#         if len(self.players_list)%2 == 0:
#             return True
#         return False
#
#     def next_match(self):
#         first = random.randint(0,len(self.current_bracket)-1)
#         second = random.randint(0,len(self.current_bracket)-1)
#         while not first !=second:
#             second = random.randint(0,len(self.current_bracket)-1)
#         self.pending.append((self.current_bracket[first],self.current_bracket[second]))
#         r_second = max(first,second)
#         r_first = min (first,second)
#         return self.current_bracket.pop(r_second) , self.current_bracket.pop(r_first)
#
#     def is_over(self):
#         if len(self.current_bracket)== 0:
#             if len(self.next_bracket)==1:
#                 return True
#             self.current_bracket = self.next_bracket
#             self.next_bracket = []
#             return False
#         return False
#
#     def Run(self):
#         ended = False
#         if not self.Initial_check():
#             raise Exception("Invalid")
#         while not ended:
#             next = self.Next_match()
#             TTT = TicTacToe(next[0],next[1]).Run()
#             for i in range(len(self.pending)):
#                 if self.pending[i] == (TTT[0],TTT[1]) or self.pending[i] == (TTT[1],TTT[0]):
#                     self.pending.pop(i)
#                     self.next_bracket.append(TTT[2])
#                     break
#             ended = self.is_over()
#         return self.next_bracket[0]

class KnockoutTournament(Tournament):
    
    def __init__(self, start:bool, id:int=None, players:list=None):
        if start:
            if players == None:
                raise Exception("A tournament to be created needs a players list")
            self.insert_tournament_to_db(players)
            self.players_list = copy.deepcopy(self.players_ids)
            # random.shuffle(self.players_list)
            self.create_tournament_tree()
            self.ended = False
        else:
            if id == None:
                raise Exception("A tournament that is not beginning now must recieve id")
            self.id = id
            self.load_tournament_from_id()
            self.players_list = copy.deepcopy(self.players_ids)
            self.load_matches_from_db()
            self.ended = self.last_match.ended

    def tournament_type(self):
        return "Knockout"
    
    def get_data_node_addr(self):
        data_nodes = get_from_dns('DataBase')
        return random.choice(data_nodes)
    
    def insert_tournament_to_db(self, players: list):
        sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
        sock.connect(self.get_data_node_addr()) # TODO

        request = pickle.dumps(['insert_tournament', (self.tournament_type(), players)])
        all_good, data = send_and_wait_for_answer(request, sock, 5)
        sock.close()
        if len(data) == 0:              
            data_nodes = get_from_dns('DataBase')
            all_good, data = retry_after_timeout(request, data_nodes)
            
        answer = pickle.loads(data) 
        print(answer)
        self.id = answer[1][0]
        self.players_ids = answer[1][1]
        return all_good
        
    def create_tournament_tree(self):
        match_id_insert = 1
        # Check if the number of players is a power of two
        num_players = len(self.players_list)
        if num_players < 2 or (num_players & (num_players - 1)) != 0:
            #& is the bitwise AND operator. It compares the bits of n and n-1 individually. 
            #A power of two in binary representation has only one '1' bit, followed by all zeros.
            # When you subtract 1 from a power of two, all the trailing zeros turn to '1's, and the '1' bit becomes a '0'.
            #Therefore, if you perform a bitwise AND of a power of two and its predecessor, the result will always be zero.
            return None

        # Create the initial matches (leaves of the tree)
        matches = [KnockoutMatch(self.id, [], False, self.players_list[i], self.players_list[i + 1]) for i in range(0, num_players, 2)]
        for m in matches:
            m.id = match_id_insert
            match_id_insert += 1
            m.save_to_db(self.get_data_node_addr())
        self.all_matches = copy.deepcopy(matches)
        # Build the tournament tree
        while len(matches) > 1:
            new_matches = []
            for i in range(0, len(matches), 2):
                required = [matches[i].id, matches[i + 1].id]
                parent_match = KnockoutMatch(self.id, required)
                new_matches.append(parent_match)
            matches = new_matches
            for m in matches:
                m.id = match_id_insert
                match_id_insert += 1
                m.save_to_db(self.get_data_node_addr())
            self.all_matches += copy.deepcopy(matches)
                
        self.last_match = matches[0]
        return matches[0]  # Return the root of the tournament tree
    
    def load_tournament_from_id(self):
        sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
        sock.connect(self.get_data_node_addr()) # TODO

        request = pickle.dumps(['get_tournament', (self.id, self.tournament_type())])
        all_good, data = send_and_wait_for_answer(request, sock, 5)
        sock.close()
        if len(data) == 0:              
            data_nodes = get_from_dns('DataBase')
            all_good, data = retry_after_timeout(request, data_nodes)
            
        decoded = pickle.loads(data) 
        self.id = decoded[0]
        self.ended = decoded[1]
        self.players_ids = decoded[2]
        return all_good
        
    def load_matches_from_db(self):
        sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
        sock.connect(self.get_data_node_addr()) # TODO
        
        request = pickle.dumps(['get_tournament_matches', (self.id, self.tournament_type())])
        all_good, data = send_and_wait_for_answer(request, sock, 5)
        sock.close()
        if len(data) == 0:              
            data_nodes = get_from_dns('DataBase')
            all_good, data = retry_after_timeout(request, data_nodes)
            
        matches_info = pickle.loads(data) 
        all_matches = matches_info[1][1]
        
        matches = []
        matches_dict = {}
        max_id = -1
        for record in all_matches:
            id, tournament_id, required, ended, player1, player2, winner = record
            required = required.split(',') if required != '' else []
            ended = bool(ended)
            match = KnockoutMatch(tournament_id, required, ended, player1, player2, winner, id)
            matches_dict[id] = match
            matches.append(match)
            max_id = max(id, max_id)
        self.all_matches = copy.deepcopy(matches)

        # Build the tournament tree
        for match in matches_dict.values():
            if len(match.required) != 0:
                if max_id < 0 or max_id in match.required:
                    raise Exception(f"The max index {max_id} is not the last match")
        
        self.last_match = matches_dict[max_id]
        return self.last_match  # Return the root of the tournament tree
    
    def find_not_ended(self):
        return self._find_not_ended(self.last_match)
    
    def _find_not_ended(self, root:KnockoutMatch):
            
        queue = deque([(root, 0)])  # Queue for BFS, storing (match, level)
        levels = {}  # To track matches at each level

        while queue:
            current_match, level = queue.popleft()

            # Store matches by level
            if level not in levels:
                levels[level] = []
            levels[level].append(current_match)

            # Add child matches to the queue if they exist
            if len(current_match.required) == 2:
                queue.append((KnockoutMatch.match_from_db(self.id, current_match.required[0], self.get_data_node_addr()), level + 1))
                queue.append((KnockoutMatch.match_from_db(self.id, current_match.required[1], self.get_data_node_addr()), level + 1))

        # Now check each level from the highest to the lowest
        last_level = []
        sorted_levels = sorted(levels.keys(), reverse=False) # [0, 1, 2, ...]
        for level in sorted_levels:
            matches_at_level = levels[level]
            ended_matches = [match for match in matches_at_level if match.ended]
            non_ended_matches = [match for match in matches_at_level if not match.ended]
            
            # if leaves level, then, give a match from here, unless they are all ended. Then give a match from last_level
            if level == sorted_levels[-1]:
                if len(non_ended_matches) == 0: 
                    return last_level[0]
                return non_ended_matches[0]
            
            if len(ended_matches) == 0:
                last_level = non_ended_matches
                continue
            if len(non_ended_matches) == 0: 
                return last_level[0]
                
            if ended_matches and non_ended_matches:
                return non_ended_matches[0]

        return None
    
    def next_match(self):
        ended = self.is_over()
        if ended:
            return True , None
        next_match:KnockoutMatch = self.find_not_ended()
        if next_match.player1 == None or next_match.player2 == None:
            next_match.player1 = KnockoutMatch.match_from_db(self.id, next_match.required[0], self.get_data_node_addr()).winner
            next_match.player2 = KnockoutMatch.match_from_db(self.id, next_match.required[1], self.get_data_node_addr()).winner
            next_match.save_to_db(self.get_data_node_addr())
        return ended, next_match
        
    def update_all_matches(self):
        updated = []
        for match in self.all_matches:
            match:KnockoutMatch
            updated.append(KnockoutMatch.match_from_db(self.id, match.id, self.get_data_node_addr()))
        self.all_matches = updated
        self.last_match = KnockoutMatch.match_from_db(self.id, self.last_match.id, self.get_data_node_addr())
        
    def is_over(self):
        self.update_all_matches()
        self.ended = self.last_match.ended
        sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
        sock.connect(self.get_data_node_addr())
    
        if self.ended:
            request = pickle.dumps(['save_tournament', (self.id, self.tournament_type(), self.ended)])
            all_good, data = send_and_wait_for_answer(request, sock, 4)
            sock.close()
            if len(data) == 0:              
                data_nodes = get_from_dns('DataBase')
                all_good, data = retry_after_timeout(request, data_nodes)
                
            answer = pickle.loads(data)
            if answer[0] == 'saved_tournament':
                self.id = answer[1]
        return self.ended    

class FreeForAllTournament(Tournament):
    
    def __init__(self, start:bool, id:int=None, players:list=None):
        self.data_nodes = get_from_dns('DataBase')
        if start:
            if players == None:
                raise Exception("A tournament to be created needs a players list")
            self.insert_tournament_to_db(players)
            self.players_list = copy.deepcopy(self.players_ids)
            # random.shuffle(self.players_list)
            self.create_tournament_tree()
            self.ended = False 
        else:
            if id == None:
                raise Exception("A tournament that is not beginning now must recieve id")
            self.id = id
            self.load_tournament_from_id()
            self.players_list = copy.deepcopy(self.players_ids)
            self.load_matches_from_db()
            self.ended = self.is_over()

    
    def tournament_type(self):
        return "FreeForAll"
    
    def get_data_node_addr(self):
        if len(self.data_nodes) == 0:
            self.data_nodes = get_from_dns('DataBase')
        return random.choice(self.data_nodes)
    
    def insert_tournament_to_db(self, players:list):
        sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
        sock.connect(self.get_data_node_addr()) # TODO

        request = pickle.dumps(['insert_tournament', (self.tournament_type(), players)])
        all_good, data = send_and_wait_for_answer(request, sock, 5)
        sock.close()
        if len(data) == 0:              
            data_nodes = get_from_dns('DataBase')
            all_good, data = retry_after_timeout(request, data_nodes)
            
        answer = pickle.loads(data) 
        print(answer)
        self.id = answer[1][0]
        self.players_ids = answer[1][1]
        return all_good
        
    def create_tournament_tree(self):
        match_id_insert = 1
        num_players = len(self.players_list)
        if num_players < 2:
            return None
        matches = []
        for i in range(num_players):
            for j in range(i):
                matches.append(FreeForAllMatch(self.id, False, self.players_list[i], self.players_list[j]))
        for m in matches:
            m.id = match_id_insert
            match_id_insert += 1
            m.save_to_db(self.get_data_node_addr())
        self.all_matches = matches

    def load_tournament_from_id(self):
        sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
        sock.connect(self.get_data_node_addr()) # TODO

        request = pickle.dumps(['get_tournament', (self.id, self.tournament_type())])
        all_good, data = send_and_wait_for_answer(request, sock, 5)
        sock.close()
        if len(data) == 0:              
            data_nodes = get_from_dns('DataBase')
            all_good, data = retry_after_timeout(request, data_nodes)
            
        decoded = pickle.loads(data) 
        self.id = decoded[0]
        self.ended = decoded[1]
        self.players_ids = decoded[2]
        return all_good
    
    def load_matches_from_db(self):
        sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
        sock.connect(self.get_data_node_addr()) # TODO
        
        request = pickle.dumps(['get_tournament_matches', (self.id, self.tournament_type())])
        all_good, data = send_and_wait_for_answer(request, sock, 5)
        sock.close()
        if len(data) == 0:              
            data_nodes = get_from_dns('DataBase')
            all_good, data = retry_after_timeout(request, data_nodes)
            
        matches_info = pickle.loads(data) 
        all_matches = matches_info[1][1]
        matches = []
        for record in all_matches:
            id, tournament_id, ended, player1, player2, winner = record
            ended = bool(ended)
            match = FreeForAllMatch(tournament_id, ended, player1, player2, winner, id)
            matches.append(match)
        self.all_matches = copy.deepcopy(matches)
    
    def find_not_ended(self):
        self.update_all_matches()
        non_ended_matches = [match for match in self.all_matches if not match.ended]
        if len(non_ended_matches) == 0:
            return None
        return non_ended_matches[0]
    
    def next_match(self):
        ended = self.is_over()
        if ended:
            return True , None
        print('find_not_ended')
        next_match:FreeForAllMatch = self.find_not_ended()
        return ended, next_match
    
    def update_all_matches(self):
        updated = []
        for match in self.all_matches:
            match:FreeForAllMatch
            updated.append(FreeForAllMatch.match_from_db(self.id, match.id, self.get_data_node_addr()))
        self.all_matches = updated
    
    def is_over(self):# TODO
        
        self.update_all_matches()
        for match in self.all_matches:
            match:FreeForAllMatch
            if not match.ended:
                self.ended = False  
                return self.ended
        self.ended = True
        print("Ended tournament")
        sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
        sock.connect(self.get_data_node_addr()) 
        request = pickle.dumps(['save_tournament', (self.id, self.tournament_type(), self.ended)])
        all_good, data = send_and_wait_for_answer(request, sock, 4)
        sock.close()
        if len(data) == 0:              
            data_nodes = get_from_dns('DataBase')
            all_good, data = retry_after_timeout(request, data_nodes)
            
        answer = pickle.loads(data)
        if answer[0] == 'saved_tournament':
            self.id = answer[1]
        return self.ended
    

tournament_type = {'Knockout': KnockoutTournament,
                   'FreeForAll': FreeForAllTournament}
match_type = {'Knockout': KnockoutMatch,
            'FreeForAll': FreeForAllTournament}
 