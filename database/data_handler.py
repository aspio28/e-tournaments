import os
import pickle
import socket
from sqlite_access import *
from utils import DNS_ADDRESS, send_to, receive_from, send_and_wait_for_answer, get_from_dns, send_addr_to_dns, send_ping_to, send_echo_replay 

class DataBaseNode:
    port = 8040
    str_rep = 'DataBase'
    db_name = "A_db_to_rule_them_all.db" # TODO name unique for each node
    current_dir = os.path.abspath(os.path.dirname(__file__))
    db_path = os.path.join(current_dir, 'data', db_name)
    
    def __init__(self, server_ip: str):
        
        if not os.path.exists(self.db_path):
            create_db(self.db_path)
        self.requests = {'ping': send_echo_replay,
                        'Failed': None,
                        'save_match': self.save_match, 
                        'get_match' : self.get_match,
                        'add_players': self.add_players,
                        'get_player': self.get_player,
                        'insert_tournament': self.insert_tournament,
                        'get_tournament': self.get_tournament,
                        'save_tournament': self.save_tournament,
                        'get_tournament_matches': self.get_tournament_matches,
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
        
    def save_match(self, arguments: tuple, connection, address):
        match_type, match_id, args = arguments  
        if match_id == None:            
            if match_type == 'KnockoutMatches':
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
                create_table(self.db_path, match_type, matches_columns)   
                match_id = insert_rows(self.db_path, match_type, 'tournament_id, required, ended, player1, player2, winner', (args,)) [0]
            
            elif match_type == 'FreeForAllMatches':
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
                create_table(self.db_path, match_type, matches_columns)   
                match_id = insert_rows(self.db_path, match_type, 'tournament_id, ended, player1, player2, winner', (args,)) [0]
            
            else: raise Exception(f'Unknown match type {match_type}')
        else:
            if match_type == 'KnockoutMatches':
                insert_rows(self.db_path, match_type, 'id, tournament_id, required, ended, player1, player2, winner', ((match_id, *args),)) [0]
            elif match_type == 'FreeForAllMatches':
                insert_rows(self.db_path, match_type, 'id, tournament_id, ended, player1, player2, winner', ((match_id, *args),)) [0]  
            else: raise Exception(f'Unknown match type {match_type}')
            
        answer = pickle.dumps(['saved_match', match_id, self.address])
        all_good = send_to(answer, connection)
        return all_good
    
    def get_match(self, arguments: tuple, connection, address):
        match_type, tournament_id, match_id = arguments  
                
        if match_type == 'KnockoutMatches':
            query = f'''SELECT id, tournament_id, required, ended, player1, player2, winner
            FROM KnockoutMatches
            WHERE tournament_id = {tournament_id} AND id = {match_id}'''
        elif match_type == 'FreeForAllMatches':
            query = f'''SELECT id, tournament_id, ended, player1, player2, winner
            FROM FreeForAllMatches
            WHERE tournament_id = {tournament_id} AND id = {match_id}'''
        else: raise Exception(f'Unknown match type {match_type}')
        
        record = read_data(self.db_path, query) [0]
            
        answer = pickle.dumps(['sending_match', record, self.address])
        all_good = send_to(answer, connection)
        return all_good
    
    def add_players(self, arguments: tuple, connection, address):
        tournament_id, players_list  = arguments
        # Insert the players to participants table
        participants_columns = ['id INTEGER PRIMARY KEY AUTOINCREMENT', 'name TEXT NOT NULL', 'player_type TEXT NOT NULL', 'tournament_id INTEGER', 'FOREIGN KEY (tournament_id) REFERENCES tournaments (id) ON DELETE CASCADE']
        create_table(self.db_path, 'participants', participants_columns)
        players_tuple = tuple((player[1], player[0], tournament_id) for player in players_list)
        players_ids = insert_rows(self.db_path, 'participants', 'name, player_type, tournament_id', players_tuple)       
        
        answer = pickle.dumps(['added_players', players_ids, self.address])
        all_good = send_to(answer, connection)
        return all_good
    
    def get_player(self, arguments: tuple, connection, address):
        player_ids = arguments[0]
        records = []
        for id in player_ids:
            query = f'''SELECT id, name, player_type
            FROM participants
            WHERE id = {id}'''
            record = read_data(self.db_path, query) [0] 
            records.append(record)
            
        answer = pickle.dumps(['sending_player', records, self.address])
        all_good = send_to(answer, connection)
        return all_good
    
    def insert_tournament(self, arguments: tuple, connection, address):
        tournament_type, players_list  = arguments
        
        # Insert to tournaments table
        tournaments_columns = ['id INTEGER PRIMARY KEY AUTOINCREMENT', 'tournament_type TEXT NOT NULL', 'ended BOOLEAN NOT NULL']
        create_table(self.db_path, 'tournaments', tournaments_columns)
        tournament_id = insert_rows(self.db_path, 'tournaments', 'tournament_type, ended', ((tournament_type, False),)) [0]
        
        # Insert the players to participants table
        participants_columns = ['id INTEGER PRIMARY KEY AUTOINCREMENT', 'name TEXT NOT NULL', 'player_type TEXT NOT NULL', 'tournament_id INTEGER', 'FOREIGN KEY (tournament_id) REFERENCES tournaments (id) ON DELETE CASCADE']
        create_table(self.db_path, 'participants', participants_columns)
        players_tuple = tuple((player[1], player[0], tournament_id) for player in players_list)
        players_ids = insert_rows(self.db_path, 'participants', 'name, player_type, tournament_id', players_tuple)       
        
        # Create the matches table
        if tournament_type == 'Knockout':
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
            create_table(self.db_path, 'KnockoutMatches', matches_columns)  
        elif tournament_type == 'FreeForAll':
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
            create_table(self.db_path, 'FreeForAllMatches', matches_columns)   
        else: raise Exception(f'Unknown tournament type {tournament_type}')
        
        answer = pickle.dumps(['created_tournament', (tournament_id, players_ids), self.address])
        all_good = send_to(answer, connection)
        return all_good
    
    def get_tournament(self, arguments: tuple, connection, address):
        tournament_id_req, tournament_type_req = arguments[0]
        
        # Load from tournaments table
        query = f'''SELECT id, tournament_type, ended
        FROM tournaments
        WHERE id = {tournament_id_req}'''
        record = read_data(self.db_path, query) [0] 
        tournament_id, tournament_type, ended = record
        if tournament_type != tournament_type_req:
            raise Exception(f"Cannot load a {tournament_type} tournament when requesting a {tournament_type_req}.")
        ended = bool(ended)
        
        # Load players from the participants table
        query = f'''SELECT id, tournament_id
        FROM participants
        WHERE tournament_id = {tournament_id_req}'''
        players = read_data(self.db_path, query)
        players_ids = [t[0] for t in players]
        
        answer = pickle.dumps(['loading_tournament', (tournament_id, ended, players_ids), self.address])
        all_good = send_to(answer, connection)
        return all_good
    
    def save_tournament(self, arguments: tuple, connection, address):
        tournament_id, tournament_type, ended = arguments
        insert_rows(self.db_path, 'tournaments', 'id, tournament_type, ended', ((tournament_id, tournament_type, ended),)) [0]
        answer = pickle.dumps(['saved_tournament', tournament_id, self.address])
        all_good = send_to(answer, connection)
        return all_good
        
    def get_tournament_matches(self, arguments: tuple, connection, address):
        tournament_id, tournament_type  = arguments
        if tournament_type == 'Knockout':
            query = f'''SELECT id, tournament_id, required, ended, player1, player2, winner
            FROM KnockoutMatches
            WHERE tournament_id = {tournament_id}'''      
            all_matches = read_data(self.db_path, query)
        elif tournament_type == 'FreeForAll':
            query = f'''SELECT id, tournament_id, ended, player1, player2, winner
            FROM FreeForAllMatches
            WHERE tournament_id = {tournament_id}''' 
            all_matches = read_data(self.db_path, query)
        else: raise Exception(f'Unknown tournament type {tournament_type}')
        
        answer = pickle.dumps(['sending_tournament_matches', (tournament_id, all_matches), self.address])
        all_good = send_to(answer, connection)
        return all_good
    
ip_address = input("Insert the node ip-address: ")
node = DataBaseNode(ip_address)