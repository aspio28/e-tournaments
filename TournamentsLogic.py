from abc import ABC, abstractmethod
from collections import deque
import random, time
import copy
from sqlite_access import *

db_name = "A_db_to_rule_them_all.db"
db_path = os.path.join(os.path.dirname(__file__), "data", db_name)

class Board:
    def __init__ (self):
        self.board = [[-1,-1,-1],[-1,-1,-1],[-1,-1,-1]]

    def __getitem__(self,key):
        return self.board[key]

class Player(ABC):
    @abstractmethod
    def Move(self):
        pass

    def __str__(self):
        pass
    def my_type(self):
        return ''

class RandomPlayer(Player):
    def __init__(self,name):
        self.name = name
    
    def __str__(self):
        return f"{self.name}"
    def __repr__(self):
        return f"{self.name}"
    def my_type(self):
        return 'random'
    
    def Move(self,board,move):
        while True:
            i = random.randint(0,2)
            j = random.randint(0,2)
            if board[i][j] == -1:
                return i,j,move

class GreedyPlayer(Player):
    def __init__(self,name):
        self.name = name        
    
    def __str__(self):
        return f"{self.name}"
    
    def __repr__(self): 
        return f"{self.name}"
    
    def my_type(self):
        return 'greedy'
    
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

        return None

    def RandomMove(self,board,move):
        while True:
            i = random.randint(0,2)
            j = random.randint(0,2)
            if board[i][j] == -1:
                return i, j, move
        
    def Move(self,board,move):
        #check if the player can win
        winning_move = GreedyPlayer.check_win(board, move)
        if winning_move:
            return *winning_move, move
        
        #check if the player can prevent the oponent from winning
        opponent = 0 if move == 1 else 1
        blocking_move = GreedyPlayer.check_win(board, opponent)
        if blocking_move:
            return *blocking_move, move
        return self.RandomMove(board,move)
    
class MinMaxPlayer(Player):
    def __init__(self,name):
        self.name = name        
    
    def __str__(self):
        return f"{self.name}"
    
    def __repr__(self): 
        return f"{self.name}"
    
    def my_type(self):
        return 'minmax'    
        
class TicTacToe:
    def __init__ (self, player1:Player, player2:Player):
        self.board = Board()
        self.players = [player1,player2]
        self.turn = 0
        self.play = []
        self.winner = -1
    
    def Ended(self):
        def i_winner(self,i):
            if (self.board[0][0]== i and self.board[1][1]== i and self.board[2][2]== i) or (self.board[0][2]== i and self.board[1][1]== i and self.board[2][0]== i):
                return True
            for j in range(3):
                if (self.board[j][0]== i and self.board[j][1]== i and self.board[j][2]== i) or ((self.board[0][j]== i and self.board[1][j]== i and self.board[2][j]== i)):
                    return True
            
        # for row in self.board:
        #     if not (any(-1 in row)):
        #         return True
            
        if not (any(-1 in row for row in self.board)):
            return True
        
        for i in range(2):
            if i_winner(self,i):
                self.winner = self.players[i]
                return True
            
        return False
    
    def Run(self):
        ended= False
        while not ended:
            move  = self.players[self.turn].Move(self.board,self.turn)
            #TODO Check if the player is cheating
            self.play.append(move)  
            self.board[move[0]][move[1]]= move [2]
            self.turn = (self.turn+1)%2
            # if self.turn == 2:
            #     self.turn = 0
            
            ended = TicTacToe.Ended(self)
        
        if self.winner == -1: #THIS IS NOT GOOD NEED WORK
            random_winner = random.randint(0,1)
            return (self.players[0],self.players[1],self.players[random_winner])

        return (self.players[0],self.players[1],self.winner)
            
class Match(ABC):
    @abstractmethod
    def __init__(self, tournament_id:int, required:list, ended:bool=False, player1:int=None, player2:int=None, winner:int=None, id:int=None):
        pass
   
    @abstractmethod
    def match_from_db(t_id, m_id):
        pass
   
    @abstractmethod
    def save_to_db(self):
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
    
    def match_from_db(t_id, m_id):
        query = f'''SELECT id, tournament_id, required, ended, player1, player2, winner
        FROM KnockoutMatches
        WHERE tournament_id = {t_id} AND id = {m_id}'''
        record = read_data(db_path, query) [0] # [(7, 6, '', 0, 24, 21)]
        id, tournament_id, required, ended, player1, player2, winner = record
        required = required.split(',') if required != '' else []
        ended = bool(ended)
        return KnockoutMatch(tournament_id, required, ended, player1, player2, winner, id)
    
    def save_to_db(self):
        # Convert the list of required match IDs to a comma-separated string
        required_str = ','.join(map(str, self.required)) if self.required else ''
        if self.id == None:
            matches_columns = [
                'id INTEGER NOT NULL',
                'tournament_id INTEGER NOT NULL',
                'required TEXT NOT NULL',
                'ended BOOLEAN NOT NULL',
                'player1 INTEGER',
                'player2 INTEGER',
                'winner INTEGER',
                'PRIMARY KEY (id, tournament_id)',
                'FOREIGN KEY (tournament_id) REFERENCES tournaments (tournament_id)'
            ]       
            create_table(db_path, 'KnockoutMatches', matches_columns)            
            self.id = insert_rows(db_path, 'KnockoutMatches', 'tournament_id, required, ended, player1, player2, winner', ((self.tournament, required_str, self.ended, self.player1, self.player2, self.winner),)) [0]
        else:
            insert_rows(db_path, 'KnockoutMatches', 'id, tournament_id, required, ended, player1, player2, winner', ((self.id, self.tournament, required_str, self.ended, self.player1, self.player2, self.winner),)) [0]

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
    
    def match_from_db(t_id, m_id):
        query = f'''SELECT id, tournament_id, ended, player1, player2, winner
FROM FreeForAllMatches
WHERE tournament_id = {t_id} AND id = {m_id}'''
        record = read_data(db_path, query) [0] # [(7, 6, '', 0, 24, 21)]
        id, tournament_id, ended, player1, player2, winner = record
        ended = bool(ended)
        return FreeForAllMatch(tournament_id, ended, player1, player2, winner, id)
    
    def save_to_db(self):
        # Convert the list of required match IDs to a comma-separated string
        if self.id == None:
            matches_columns = [
                'id INTEGER NOT NULL',
                'tournament_id INTEGER NOT NULL',
                'ended BOOLEAN NOT NULL',
                'player1 INTEGER',
                'player2 INTEGER',
                'winner INTEGER',
                'PRIMARY KEY (id, tournament_id)',
                'FOREIGN KEY (tournament_id) REFERENCES tournaments (tournament_id)'
            ]       
            create_table(db_path, 'FreeForAllMatches', matches_columns)            
            self.id = insert_rows(db_path, 'FreeForAllMatches', 'tournament_id, ended, player1, player2, winner', ((self.tournament, self.ended, self.player1, self.player2, self.winner),)) [0]
        else:
            insert_rows(db_path, 'FreeForAllMatches', 'id, tournament_id, ended, player1, player2, winner', ((self.id, self.tournament, self.ended, self.player1, self.player2, self.winner),)) [0]

def get_player_instance(player_id):
    query = f'''SELECT id, name, player_type
FROM participants
WHERE id = {player_id}'''
    record = read_data(db_path, query) [0] 
    id, name, player_type = record
    return player_types[player_type](name)
            
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

class BridgeTournament(Tournament):
    def __init__(self, start:bool, id:int=None, players:list=None):
        if start:
            self.insert_tournament_to_db(players)
            self.players_list = copy.deepcopy(self.players_ids)
            random.shuffle(self.players_list)
        self.current_bracket = copy.deepcopy(self.players_list)
        self.next_bracket = []
        self.pending = []

    def insert_tournament_to_db(self, players:list):
        if not os.path.exists(db_path):
            create_db(db_path)
        tournaments_columns = ['id INTEGER PRIMARY KEY AUTOINCREMENT', 'tournament_type TEXT NOT NULL', 'ended BOOLEAN NOT NULL']
        create_table(db_path, 'tournaments', tournaments_columns)
        self.id = insert_rows(db_path, 'tournaments', 'tournament_type, ended', (("bridge",False),)) [0]
        participants_columns = ['id INTEGER PRIMARY KEY AUTOINCREMENT', 'name TEXT NOT NULL', 'player_type TEXT NOT NULL', 'tournament_id INTEGER', 'FOREIGN KEY (tournament_id) REFERENCES tournaments (id) ON DELETE CASCADE']
        create_table(db_path, 'participants', participants_columns)
        players_tuple = tuple((player.name, player.my_type(), self.id) for player in players)
        self.players_ids = insert_rows(db_path, 'participants', 'name, player_type, tournament_id', players_tuple)       
        # print(self.players_ids)
    
    def initial_check(self):
        if len(self.players_list)%2 == 0:
            return True
        return False
    
    def next_match(self):
        first = random.randint(0,len(self.current_bracket)-1)
        second = random.randint(0,len(self.current_bracket)-1)
        while not first !=second:
            second = random.randint(0,len(self.current_bracket)-1)
        self.pending.append((self.current_bracket[first],self.current_bracket[second]))
        r_second = max(first,second)
        r_first = min (first,second)
        return self.current_bracket.pop(r_second) , self.current_bracket.pop(r_first)

    def is_over(self):
        if len(self.current_bracket)== 0:
            if len(self.next_bracket)==1:
                return True
            self.current_bracket = self.next_bracket
            self.next_bracket = []
            return False
        return False

    def Run(self):
        ended = False
        if not self.Initial_check():
            raise Exception("Invalid")
        while not ended:
            next = self.Next_match()
            TTT = TicTacToe(next[0],next[1]).Run()
            for i in range(len(self.pending)):
                if self.pending[i] == (TTT[0],TTT[1]) or self.pending[i] == (TTT[1],TTT[0]):
                    self.pending.pop(i)
                    self.next_bracket.append(TTT[2])
                    break
            ended = self.is_over()
        return self.next_bracket[0]

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
    
    def insert_tournament_to_db(self, players:list):
        if not os.path.exists(db_path):
            create_db(db_path)
        # Insert to tournaments table
        tournaments_columns = ['id INTEGER PRIMARY KEY AUTOINCREMENT', 'tournament_type TEXT NOT NULL', 'ended BOOLEAN NOT NULL']
        create_table(db_path, 'tournaments', tournaments_columns)
        self.id = insert_rows(db_path, 'tournaments', 'tournament_type, ended', ((self.tournament_type(), False),)) [0]
        
        # Insert the players to participants table
        participants_columns = ['id INTEGER PRIMARY KEY AUTOINCREMENT', 'name TEXT NOT NULL', 'player_type TEXT NOT NULL', 'tournament_id INTEGER', 'FOREIGN KEY (tournament_id) REFERENCES tournaments (id) ON DELETE CASCADE']
        create_table(db_path, 'participants', participants_columns)
        players_tuple = tuple((player.name, player.my_type(), self.id) for player in players)
        self.players_ids = insert_rows(db_path, 'participants', 'name, player_type, tournament_id', players_tuple)       
        
        # Create the matches table
        matches_columns = [
            'id INTEGER NOT NULL',
            'tournament_id INTEGER NOT NULL',
            'required TEXT NOT NULL',
            'ended BOOLEAN NOT NULL',
            'player1 INTEGER',
            'player2 INTEGER',
            'winner INTEGER',
            'PRIMARY KEY (id, tournament_id)',
            'FOREIGN KEY (tournament_id) REFERENCES tournaments (tournament_id)'
        ]       
        create_table(db_path, 'KnockoutMatches', matches_columns)
        
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
            m.save_to_db()
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
                m.save_to_db()
            self.all_matches += copy.deepcopy(matches)
                
        self.last_match = matches[0]
        return matches[0]  # Return the root of the tournament tree
    
    def load_tournament_from_id(self):
        # Load from tournaments table
        query = f'''SELECT id, tournament_type, ended
FROM tournaments
WHERE id = {self.id}'''
        record = read_data(db_path, query) [0] 
        id, tournament_type, ended = record
        if tournament_type != self.tournament_type():
            raise Exception("Cannot load a tournament that is not knockout.")
        self.ended = bool(ended)
        
        # Load players from the participants table
        query = f'''SELECT id, tournament_id
FROM participants
WHERE tournament_id = {self.id}'''
        players = read_data(db_path, query)
        self.players_ids = [t[0] for t in players]
        
    def load_matches_from_db(self):
        query = f'''SELECT id, tournament_id, required, ended, player1, player2, winner
FROM KnockoutMatches
WHERE tournament_id = {self.id}'''
        all_matches = read_data(db_path, query)
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
                queue.append((KnockoutMatch.match_from_db(self.id, current_match.required[0]), level + 1))
                queue.append((KnockoutMatch.match_from_db(self.id, current_match.required[1]), level + 1))

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
            next_match.player1 = KnockoutMatch.match_from_db(self.id, next_match.required[0]).winner
            next_match.player2 = KnockoutMatch.match_from_db(self.id, next_match.required[1]).winner
            next_match.save_to_db()
        return ended, next_match
        
    def update_all_matches(self):
        updated = []
        for match in self.all_matches:
            match:KnockoutMatch
            updated.append(KnockoutMatch.match_from_db(self.id, match.id))
        self.all_matches = updated
        self.last_match = KnockoutMatch.match_from_db(self.id, self.last_match.id)
        
    def is_over(self):
        self.update_all_matches()
        self.ended = self.last_match.ended
        return self.ended    

class FreeForAllTournament(Tournament):
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
            self.ended = self.is_over()
    
    def tournament_type(self):
        return "FreeForAll"
    
    def insert_tournament_to_db(self, players:list):
        if not os.path.exists(db_path):
            create_db(db_path)
        # Insert to tournaments table
        tournaments_columns = ['id INTEGER PRIMARY KEY AUTOINCREMENT', 'tournament_type TEXT NOT NULL', 'ended BOOLEAN NOT NULL']
        create_table(db_path, 'tournaments', tournaments_columns)
        self.id = insert_rows(db_path, 'tournaments', 'tournament_type, ended', ((self.tournament_type(), False),)) [0]
        
        # Insert the players to participants table
        participants_columns = ['id INTEGER PRIMARY KEY AUTOINCREMENT', 'name TEXT NOT NULL', 'player_type TEXT NOT NULL', 'tournament_id INTEGER', 'FOREIGN KEY (tournament_id) REFERENCES tournaments (id) ON DELETE CASCADE']
        create_table(db_path, 'participants', participants_columns)
        players_tuple = tuple((player.name, player.my_type(), self.id) for player in players)
        self.players_ids = insert_rows(db_path, 'participants', 'name, player_type, tournament_id', players_tuple)       
        
        # Create the matches table
        matches_columns = [
            'id INTEGER NOT NULL',
            'tournament_id INTEGER NOT NULL',
            'ended BOOLEAN NOT NULL',
            'player1 INTEGER',
            'player2 INTEGER',
            'winner INTEGER',
            'PRIMARY KEY (id, tournament_id)',
            'FOREIGN KEY (tournament_id) REFERENCES tournaments (tournament_id)'
        ]     
        create_table(db_path, 'FreeForAllMatches', matches_columns)
        
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
            m.save_to_db()
        self.all_matches = matches

    def load_tournament_from_id(self):
        # Load from tournaments table
        query = f'''SELECT id, tournament_type, ended
FROM tournaments
WHERE id = {self.id}'''
        record = read_data(db_path, query) [0] 
        id, tournament_type, ended = record
        if tournament_type != self.tournament_type():
            raise Exception("Cannot load a tournament that is not Free for all.")
        self.ended = bool(ended)
        
        # Load players from the participants table
        query = f'''SELECT id, tournament_id
FROM participants
WHERE tournament_id = {self.id}'''
        players = read_data(db_path, query)
        self.players_ids = [t[0] for t in players]
    
    def load_matches_from_db(self):
        query = f'''SELECT id, tournament_id, ended, player1, player2, winner
FROM FreeForAllMatches
WHERE tournament_id = {self.id}'''
        all_matches = read_data(db_path, query)
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
        next_match:FreeForAllMatch = self.find_not_ended()
        return ended, next_match
    
    def update_all_matches(self):
        updated = []
        for match in self.all_matches:
            match:FreeForAllMatch
            updated.append(FreeForAllMatch.match_from_db(self.id, match.id))
        self.all_matches = updated
    
    def is_over(self):
        self.update_all_matches()
        for match in self.all_matches:
            match:FreeForAllMatch
            if not match.ended:
                self.ended = False
                return self.ended
        self.ended = True
        return self.ended
    
    
player_types = {'random': RandomPlayer, 
                'greedy': GreedyPlayer
                }
tournament_type = {'Knockout': KnockoutTournament,
                   'FreeForAll': FreeForAllTournament}
match_type = {'Knockout': KnockoutMatch,
            'FreeForAll': FreeForAllTournament}

class Minion():
    def do_a_match(self,match:Match):
        player1:Player = get_player_instance(match.player1)
        player2:Player = get_player_instance(match.player2)
        winner = TicTacToe(player1, player2).Run()[2]
        print(winner)
        if winner == player1:
            match.winner = match.player1
        elif winner == player2:
            match.winner = match.player2
        else: raise Exception("Un texto si quieres")
        match.ended = True
        match.save_to_db()