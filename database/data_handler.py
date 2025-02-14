import os
import pickle
import socket
import time
import threading
import datetime
import multiprocessing

from sqlite_access import *
from utils import DNS_ADDRESS, getShaRepr, send_to, receive_from, send_and_wait_for_answer, get_dns_address, get_from_dns, send_addr_to_dns, send_ping_to, send_echo_replay, in_between
from chordReference import ChordNodeReference
from fingerTable import FingerTable
from succ_list import SuccList

class DataBaseNode:
    port = 8040
    str_rep = 'DataBase'
    db_name = "A_db_to_rule_them_all.db" # TODO name unique for each node
    current_dir = os.path.abspath(os.path.dirname(__file__))
    db_path = os.path.join(current_dir, 'data', db_name)
    ip = os.getenv('NODE_IP')

    def __init__(self):
        
        self.id = getShaRepr(self.ip)
        self.ref = ChordNodeReference(self.ip, self.port)
        self.succ = self.ref
        self.pred = None
        self.finger = FingerTable(self)
        self.successors = SuccList(3, self)

        if os.path.exists(self.db_path):
            os.remove(self.db_path)

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
                        'get_tournament_status': self.get_tournament_status,
                        'find_predecessor': self.finger.find_pred,
                        'find_successor': self.finger.find_succ,
                        'get_successor': self.get_succ,
                        'get_predecessor': self.get_pred,
                        'closest_preceding_finger': self.finger.closest_preceding_finger,
                        'check_predecessor': self.check_predecessor,
                        'notify': self.notify,
                        'ping_ring': self.ping,
                        'get_data': self.get_data,
                        'delete_data': self.delete_data,
                        }
        
        self.address = (self.ip, self.port)
        self.serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.serverSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.serverSocket.bind(self.address)

        threading.Thread(target=self.stabilize, daemon=True).start()
        threading.Thread(target=self.check_predecessor, daemon=True).start()  
        threading.Thread(target=self.run, daemon=True).start()
        threading.Thread(target=self.successors.fix_succ, daemon=True).start()
        
    def run(self):
        print(f"Listening at {self.address}")
        self.serverSocket.listen(5)

        while True:
            while True:
                try:                    
                    result = send_addr_to_dns(self.str_rep, self.address)
                    if result: 
                        break
                except Exception as err:
                    print(err)    
                    
            threads = []
            check_abandoned_tournaments_process = threading.Thread(target=self.check_abandoned_tournaments, daemon=True)
            threads.append(check_abandoned_tournaments_process)
            check_abandoned_tournaments_process.start()
            try:
                while True:            
                    conn, address = self.serverSocket.accept()
                    print('Received CONNECTION from: ', address)
                    thread = threading.Thread(target=self.handle_connection, args=(conn, address), daemon=True)
                    threads.append(thread)
                    thread.start()
            except Exception as err:
                print(err)
            finally:
                self.serverSocket.close()
                for th in threads:
                    if th.is_alive():
                        th.join()

    def handle_connection(self, connection: socket, address):

        status = False
        received = receive_from(connection, 3)
        if len(received) == 0:
            print("Failed request, data not received") 
            dns_address = get_dns_address()
            im_conn = send_ping_to(dns_address) 
            if not im_conn:
                connection.close()
                raise ConnectionError("I'm falling down")
        try:
            decoded = pickle.loads(received)
            if decoded[0] == "DNS":
                connection.close()
                return status
                
            if self.requests.get(decoded[0]):
                function_to_answer = self.requests.get(decoded[0])
                status = function_to_answer(decoded[1], connection, address)
                if not status:
                    dns_address = get_dns_address()
                    im_conn = send_ping_to(dns_address) 
                    if not im_conn:
                        raise ConnectionError("I'm falling down")

        except Exception as err:
            if str(err) =="I'm falling down":
                connection.close()
                raise err
            print(err, ". Failed request ->",decoded[0]) 
            answer = pickle.dumps(['Failed', (None,)])
            send_to(answer, connection)
                 
        finally:
            connection.close()
        return status
    
    def check_abandoned_tournaments(self):
        while True:
            try:
                # Load from tournaments table the not ended tournaments
                query = f'''SELECT id, tournament_name, tournament_type, last_update
                FROM tournaments
                WHERE ended = 0'''
                records = read_data(self.db_path, query) 
                
                inactive_tournaments = []
                for tournament_id, tournament_name, tournament_type, last_update_time in records:
                    last_update = datetime.datetime.strptime(last_update_time, '%Y-%m-%d %H:%M:%S.%f')
                    time_elapsed = datetime.datetime.now() - last_update
                    if time_elapsed.total_seconds() >= 15: # Check if 15 seconds have passed
                        inactive_tournaments.append((tournament_type, tournament_id, tournament_name))
                
                for t_type, t_id, t_name in inactive_tournaments:
                    servers = get_from_dns('Server')
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(4)
                    request = pickle.dumps(['continue_tournament', (t_type, t_id, t_name), self.address])
                    all_good, data = self.retry_after_timeout(request, servers)
                    
            except Exception as e:
                print(f"Error in abandonned tournaments checker: {e}")

            time.sleep(15)  # Check every 15 seconds        
        
    def retry_after_timeout(self, request, addresses, wait_answer=True):
        for addr in addresses:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(4)
                sock.connect(addr)
                if wait_answer:
                    all_good, data = send_and_wait_for_answer(request, sock, 6)
                    if len(data) != 0:
                        break
                else:
                    all_good = send_to(request, sock)
                    if all_good:
                        break
                sock.close()
            except Exception as err:
                print(err, ". Failed retry after timeout") 
                
        if wait_answer:
            if len(data) == 0:
                return False, None
            return all_good, data
        return all_good
    
    def save_match(self, arguments: tuple, connection, address):
        match_type, match_id, args = arguments  
        tournament_id = args[0] 

        tournament_hash = int(tournament_id) 
        node = self.finger.find_succ(tournament_hash)

        if(node.id != self.id):
            match_id = node.save_match(match_type, match_id, args)
        else:
            if match_id == None:        
                if match_type == 'KnockoutMatches':
                    matches_columns = [
                        'id INTEGER NOT NULL',
                        'tournament_id TEXT NOT NULL',
                        'required TEXT NOT NULL',
                        'ended BOOLEAN NOT NULL',
                        'player1 INTEGER',
                        'player2 INTEGER',
                        'winner INTEGER',
                        'PRIMARY KEY (id, tournament_id)',
                        'FOREIGN KEY (tournament_id) REFERENCES tournaments (tournament_id)'
                    ] 
                    create_table(self.db_path, match_type, matches_columns)   
                    match_id = insert_rows(self.db_path, match_type, 'tournament_id, required, ended, player1, player2, winner', (args,), with_autoincrement=True) [0]
                
                elif match_type == 'FreeForAllMatches':
                    matches_columns = [
                        'id INTEGER NOT NULL',
                        'tournament_id TEXT NOT NULL',
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
            
            # Update last_update in tournaments table directly
            query = f'''SELECT tournament_name, tournament_type, ended
            FROM tournaments
            WHERE id = '{tournament_id}' '''
            record = read_data(self.db_path, query) [0] 
            tournament_name, tournament_type, ended = record
            insert_rows(self.db_path, 'tournaments', 'id, tournament_name, tournament_type, ended, last_update', 
            ((tournament_id, tournament_name, tournament_type, ended, datetime.datetime.now()),))
            
        answer = pickle.dumps(['saved_match', match_id, self.address])
        all_good = send_to(answer, connection)
        return all_good
    
    def get_match(self, arguments: tuple, connection, address):
        match_type, tournament_id, match_id = arguments

        tournament_hash = int(tournament_id) 
        node = self.finger.find_succ(tournament_hash)

        if(node.id != self.id):
            record = node.get_match(match_type, tournament_id, match_id)

        else:       
            if match_type == 'KnockoutMatches':
                query = f'''SELECT id, tournament_id, required, ended, player1, player2, winner
                FROM KnockoutMatches
                WHERE tournament_id = '{tournament_id}' AND id = {match_id}'''
            elif match_type == 'FreeForAllMatches':
                query = f'''SELECT id, tournament_id, ended, player1, player2, winner
                FROM FreeForAllMatches
                WHERE tournament_id = '{tournament_id}' AND id = {match_id}'''
            else: raise Exception(f'Unknown match type {match_type}')
            
            record = read_data(self.db_path, query) [0]
        # time.sleep(8)

        answer = pickle.dumps(['sending_match', record, self.address])
        all_good = send_to(answer, connection)
        return all_good
    
    def add_players(self, arguments: tuple, connection, address):
        tournament_id, players_list  = arguments
        # Insert the players to participants table
        participants_columns = ['id INTEGER PRIMARY KEY AUTOINCREMENT', 'name TEXT NOT NULL', 'player_code TEXT NOT NULL', 'tournament_id INTEGER', 'FOREIGN KEY (tournament_id) REFERENCES tournaments (id) ON DELETE CASCADE']
        create_table(self.db_path, 'participants', participants_columns)
        players_tuple = tuple((player[1], player[0], tournament_id) for player in players_list)
        players_ids = insert_rows(self.db_path, 'participants', 'name, player_code, tournament_id', players_tuple, with_autoincrement=True)       
        
        answer = pickle.dumps(['added_players', players_ids, self.address])
        all_good = send_to(answer, connection)
        return all_good
    
    def get_player(self, arguments: tuple, connection, address):
        player_ids, tournament_id = arguments

        tournament_hash = int(tournament_id) 
        node = self.finger.find_succ(tournament_hash)

        if(node.id != self.id):
            records = node.get_player(player_ids, tournament_id)

        else:
            records = []
            for id in player_ids:
                query = f'''SELECT id, name, player_code
                FROM participants
                WHERE id = {id}'''
                record = read_data(self.db_path, query) [0] 
                records.append(record)
        
        answer = pickle.dumps(['sending_player', records, self.address])
        all_good = send_to(answer, connection)
        return all_good
    
    def insert_tournament(self, arguments: tuple, connection, address):
        tournament_type, players_list, tournament_name  = arguments

        key_hash = getShaRepr(tournament_name)
        key_hash_text = str(key_hash)
        print('TOURNAMENT HASH TO INSERT:',key_hash)
        node = self.finger.find_succ(key_hash)

        if(node.id != self.id):
            tournament_id, players_ids = node.insert_tournament(tournament_type, players_list, tournament_name)

        else:
            # Insert to tournaments table
            tournaments_columns = ['id TEXT PRIMARY KEY', 'tournament_name TEXT NOT NULL', 'tournament_type TEXT NOT NULL', 'ended BOOLEAN NOT NULL', 'last_update DATETIME'] 
            create_table(self.db_path, 'tournaments', tournaments_columns)
            tournament_id = insert_rows(self.db_path, 'tournaments', 'id, tournament_name, tournament_type, ended, last_update', ((key_hash_text, tournament_name, tournament_type, False, datetime.datetime.now()),)) [0]
            
            # Insert the players to participants table
            participants_columns = ['id INTEGER PRIMARY KEY AUTOINCREMENT', 'name TEXT NOT NULL', 'player_code BLOB NOT NULL', 'tournament_id TEXT', 'FOREIGN KEY (tournament_id) REFERENCES tournaments (id) ON DELETE CASCADE']
            create_table(self.db_path, 'participants', participants_columns)
            players_tuple = tuple((player[1], player[0], tournament_id) for player in players_list)
            players_ids = insert_rows(self.db_path, 'participants', 'name, player_code, tournament_id', players_tuple, with_autoincrement=True) 
            
            # Create the matches table
            if tournament_type == 'Knockout':
                matches_columns = [
                    'id INTEGER NOT NULL',
                    'tournament_id TEXT NOT NULL',
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
                    'tournament_id TEXT NOT NULL',
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
        
        tournament_id_req, tournament_type_req = arguments
        
        tournament_hash = int(tournament_id_req) 
        node = self.finger.find_succ(tournament_hash)

        if(node.id != self.id):
            tournament_id, ended, players_ids = node.get_tournament(tournament_id_req, tournament_type_req)

        else:
            # Load from tournaments table
            query = f'''SELECT id, tournament_type, ended
            FROM tournaments
            WHERE id = '{tournament_id_req}' '''

            record = read_data(self.db_path, query) [0] 
            tournament_id, tournament_type, ended = record
            if tournament_type != tournament_type_req:
                raise Exception(f"Cannot load a {tournament_type} tournament when requesting a {tournament_type_req}.")
            ended = bool(ended)
            
            # Load players from the participants table
            query = f'''SELECT id, tournament_id
            FROM participants
            WHERE tournament_id = '{tournament_id_req}' '''
            players = read_data(self.db_path, query)
            players_ids = [t[0] for t in players]
        
        answer = pickle.dumps(['loading_tournament', (tournament_id, ended, players_ids), self.address])
        all_good = send_to(answer, connection)
        return all_good
    
    def save_tournament(self, arguments: tuple, connection, address):
        tournament_id, tournament_name, tournament_type, ended = arguments

        tournament_hash = int(tournament_id) 
        node = self.finger.find_succ(tournament_hash)

        if(node.id != self.id):
            tournament_id = node.save_tournament(tournament_id, tournament_name, tournament_type, ended)

        else:
            insert_rows(self.db_path, 'tournaments', 'id, tournament_name, tournament_type, ended, last_update', ((tournament_id, tournament_name, tournament_type, ended, datetime.datetime.now()),)) [0]
        
        answer = pickle.dumps(['saved_tournament', tournament_id, self.address])
        all_good = send_to(answer, connection)
        return all_good
        
    def get_tournament_matches(self, arguments: tuple, connection, address):
        tournament_id, tournament_type  = arguments

        tournament_hash = int(tournament_id) 
        node = self.finger.find_succ(tournament_hash)

        if(node.id != self.id):
            tournament_id, all_matches = node.get_tournament_matches(tournament_id, tournament_type)
        
        else:
            if tournament_type == 'Knockout':
                query = f'''SELECT id, tournament_id, required, ended, player1, player2, winner
                FROM KnockoutMatches
                WHERE tournament_id = '{tournament_id}' '''      
                all_matches = read_data(self.db_path, query)
            elif tournament_type == 'FreeForAll':
                query = f'''SELECT id, tournament_id, ended, player1, player2, winner
                FROM FreeForAllMatches
                WHERE tournament_id = '{tournament_id}' ''' 
                all_matches = read_data(self.db_path, query)
            else: raise Exception(f'Unknown tournament type {tournament_type}')
        
        answer = pickle.dumps(['sending_tournament_matches', (tournament_id, all_matches), self.address])
        all_good = send_to(answer, connection)
        return all_good
    
    def get_tournament_status(self, arguments: tuple, connection, address):
        tournament_id = arguments[0]

        tournament_hash = int(tournament_id) 
        node = self.finger.find_succ(tournament_hash)

        if(node.id != self.id):
            tournament_id, tournament_type, ended, all_matches, all_players = node.get_tournament_status(tournament_id)

        else:
            # Load from tournaments table
            query = f'''SELECT id, tournament_type, ended
            FROM tournaments
            WHERE id = '{tournament_id}' '''
            record = read_data(self.db_path, query) [0] 
            id, tournament_type, ended = record
            ended = bool(ended)

            if tournament_type == 'Knockout':
                query = f'''SELECT id, tournament_id, required, ended, player1, player2, winner
                FROM KnockoutMatches
                WHERE tournament_id = '{tournament_id}' '''      
                all_matches = read_data(self.db_path, query)
            elif tournament_type == 'FreeForAll':
                query = f'''SELECT id, tournament_id, ended, player1, player2, winner
                FROM FreeForAllMatches
                WHERE tournament_id = '{tournament_id}' ''' 
                all_matches = read_data(self.db_path, query)
            else: raise Exception(f'Unknown tournament type {tournament_type}')
            
            query = f'''SELECT id, name, tournament_id
                FROM participants
                WHERE tournament_id = '{tournament_id}' '''
            all_players = read_data(self.db_path, query)

        answer = pickle.dumps(['tournament_status', (tournament_id, tournament_type, ended, all_matches, all_players), self.address])
        print((tournament_id, tournament_type, ended, all_matches, all_players))
        all_good = send_to(answer, connection)
        return all_good

    def join(self, node: 'ChordNodeReference'):
        """Join a Chord network using 'node' as an entry point."""
        if node:
            self.pred = None
            self.succ = node.find_successor(self.id)
            self.pred = self.succ.pred
            self.succ.notify(self.ref)
            tournaments, all_KnockoutMatches, all_FreeForAllMatches, all_players = self.succ.get_data(self.id, self.pred.id)

            if tournaments:
                self.insert_data(tournaments, all_KnockoutMatches, all_FreeForAllMatches, all_players)
                self.succ.delete_data(self.id, self.pred.id)
        else:
            self.succ = self.ref
            self.pred = None

    def stabilize(self):
        """Regular check for correct Chord structure."""
        while True:
            try:
                try:
                    if self.succ.id != self.id:
                        self.succ = self.successors.check_succ()
                except Exception as e:
                    print(f"Error selecting new successor")

                if len(self.successors.list) == 0:
                    self.succ = self.ref
                    
                if self.succ.id != self.id:
                    print('stabilize')
                    print(self.successors.list)
                    x = self.succ.pred
                    if x.id != self.id:
                        print(x)
                        if x and in_between(x.id, self.id, self.succ.id):
                            self.succ = x
                        self.succ.notify(self.ref)
            except Exception as e:
                print(f"Error in stabilize: {e}")

            print(f"successor : {self.succ} predecessor {self.pred}")
            time.sleep(10)

    def notify(self, node: ChordNodeReference, connection=None, address=None):
        if node.id == self.id:
            pass
        if not self.pred or in_between(node.id, self.pred.id, self.id):
            self.pred = node
        if self.succ.id == self.id:
            self.succ = node
        answer = pickle.dumps(['notified',(None,)])
        all_good = send_to(answer, connection)
        return all_good

    def check_predecessor(self, arguments=None, connection=None, address=None):
        pred_data = None
        pred_pred = None
        pred_pred_data = None

        while True:
            try:
                if self.pred:
                    socket.setdefaulttimeout(10) 
                    ok = self.pred.ping()

                    if ok:
                        pred_data = self.pred.get_data(999999999999999999999999999999999999999999999999, 0)
                        print('===============================================Predecessor data recived===============================================')
                        pred_pred = self.pred.pred

                        if pred_pred.id != self.id:
                            try:
                                socket.setdefaulttimeout(10)
                                ok_pred = pred_pred.ping()
                                if ok:
                                    pred_pred_data = pred_pred.get_data(999999999999999999999999999999999999999999999999, 0)
                                    print('==============================Predecessor to my predecessor data recived========================')
                            except Exception as e:
                                pass
            except Exception as e:
                if pred_data:
                    tournaments, all_KnockoutMatches, all_FreeForAllMatches, all_players = pred_data
                    if tournaments:
                        self.insert_data(tournaments, all_KnockoutMatches, all_FreeForAllMatches, all_players)
                    pred_data = None
                try:
                    socket.setdefaulttimeout(10) 
                    ok = pred_pred.ping()
                except Exception as e:
                    pred_pred = None
                    if pred_pred_data:
                        tournaments_pred, all_KnockoutMatches_pred, all_FreeForAllMatches_pred, all_players_pred = pred_pred_data
                        if tournaments_pred:
                            self.insert_data(tournaments_pred, all_KnockoutMatches_pred, all_FreeForAllMatches_pred, all_players_pred)
                        pred_pred_data = None
                self.pred = None
                pred_pred = None

            finally:
                socket.setdefaulttimeout(None)  
            time.sleep(10)

    def get_succ(self, arguments=None, connection=None, address=None):
        succ = self.succ if self.succ else self.ref
        if connection:
            answer = pickle.dumps(['get_successor', (succ.id, succ.ip)])
            all_good = send_to(answer, connection)
            return all_good
        else:
            return succ 
    
    def get_pred(self, arguments=None, connection=None, address=None):
        pred = self.pred if self.pred else self.ref
        if connection:
            answer = pickle.dumps(['get_predecessor', (pred.id, pred.ip)])
            all_good = send_to(answer, connection)
            return all_good
        else:
            return pred
    
    def ping(self, arguments=None, connection= None, address=None):
        answer = pickle.dumps(['ping_success', ('OK',)])
        all_good = send_to(answer, connection)

        return all_good
    
    def get_data(self, arguments, connection=None, address=None):

        node_id, pred_id = arguments

        tournaments = None
        all_FreeForAllMatches = None
        all_KnockoutMatches = None
        all_players = None

        if str(pred_id) > str(node_id):
            if exist_table(self.db_path, 'tournaments'):
                # Load from tournaments table
                query = f'''SELECT *
                FROM tournaments
                WHERE id > '{pred_id}' OR  id < '{node_id}' '''

                tournaments = read_data(self.db_path, query)

            if exist_table(self.db_path, 'KnockoutMatches'):
                query = f'''SELECT *
                FROM KnockoutMatches
                WHERE tournament_id > '{pred_id}' OR  tournament_id < '{node_id}' '''      
                all_KnockoutMatches = read_data(self.db_path, query)

            if exist_table(self.db_path, 'FreeForAllMatches'):
                query = f'''SELECT *
                FROM FreeForAllMatches
                WHERE tournament_id > '{pred_id}' OR  tournament_id < '{node_id}' ''' 
                all_FreeForAllMatches = read_data(self.db_path, query)
            
            if exist_table(self.db_path, 'participants'):
                query = f'''SELECT *
                FROM participants
                WHERE tournament_id > '{pred_id}' OR  tournament_id < '{node_id}' ''' 
                all_players = read_data(self.db_path, query)
        else:
            if exist_table(self.db_path, 'tournaments'):
                # Load from tournaments table
                query = f'''SELECT *
                FROM tournaments
                WHERE id BETWEEN '{pred_id}' AND '{node_id}' '''

                tournaments = read_data(self.db_path, query)

            if exist_table(self.db_path, 'KnockoutMatches'):
                query = f'''SELECT *
                FROM KnockoutMatches
                WHERE tournament_id BETWEEN '{pred_id}' AND '{node_id}' '''   
                all_KnockoutMatches = read_data(self.db_path, query)

            if exist_table(self.db_path, 'FreeForAllMatches'):
                query = f'''SELECT *
                FROM FreeForAllMatches
                WHERE tournament_id BETWEEN '{pred_id}' AND '{node_id}' '''
                all_FreeForAllMatches = read_data(self.db_path, query)
            
            if exist_table(self.db_path, 'participants'):
                query = f'''SELECT *
                FROM participants
                WHERE tournament_id BETWEEN '{pred_id}' AND '{node_id}' '''
                all_players = read_data(self.db_path, query)

        answer = pickle.dumps(['get_data', (tournaments, all_KnockoutMatches, all_FreeForAllMatches, all_players), self.address])
        print(tournaments, all_KnockoutMatches, all_FreeForAllMatches, all_players)
        all_good = send_to(answer, connection)
        return all_good

    def insert_data(self, tournaments, all_KnockoutMatches, all_FreeForAllMatches, all_players):

        tournaments_columns = ['id TEXT PRIMARY KEY', 'tournament_name TEXT NOT NULL', 'tournament_type TEXT NOT NULL', 'ended BOOLEAN NOT NULL', 'last_update DATETIME'] 
        create_table(self.db_path, 'tournaments', tournaments_columns)
        
        for tournament in tournaments:
            insert_rows(self.db_path, 'tournaments', 'id, tournament_name, tournament_type, ended, last_update', (tournament,))
            
        participants_columns = ['id INTEGER PRIMARY KEY AUTOINCREMENT', 'name TEXT NOT NULL', 'player_code BLOB NOT NULL', 'tournament_id TEXT', 'FOREIGN KEY (tournament_id) REFERENCES tournaments (id) ON DELETE CASCADE']
        create_table(self.db_path, 'participants', participants_columns)
        
        for players in all_players: 
            insert_rows(self.db_path, 'participants', 'id, name, player_code, tournament_id', (players,)) 
        
        if all_KnockoutMatches:
            matches_columns = [
                'id INTEGER NOT NULL',
                'tournament_id TEXT NOT NULL',
                'required TEXT NOT NULL',
                'ended BOOLEAN NOT NULL',
                'player1 INTEGER',
                'player2 INTEGER',
                'winner INTEGER',
                'PRIMARY KEY (id, tournament_id)',
                'FOREIGN KEY (tournament_id) REFERENCES tournaments (tournament_id)'
            ] 
            create_table(self.db_path, 'KnockoutMatches', matches_columns)

            for match in all_KnockoutMatches:   
                insert_rows(self.db_path, 'KnockoutMatches', 'id, tournament_id, required, ended, player1, player2, winner', (match,)) 
        
        if all_FreeForAllMatches:
            matches_columns = [
                'id INTEGER NOT NULL',
                'tournament_id TEXT NOT NULL',
                'ended BOOLEAN NOT NULL',
                'player1 INTEGER',
                'player2 INTEGER',
                'winner INTEGER',
                'PRIMARY KEY (id, tournament_id)',
                'FOREIGN KEY (tournament_id) REFERENCES tournaments (tournament_id)'
            ]
            create_table(self.db_path, 'FreeForAllMatches', matches_columns)  

            for match in all_FreeForAllMatches: 
                insert_rows(self.db_path, 'FreeForAllMatches', 'id, tournament_id, ended, player1, player2, winner', (match,))
        
        print('Data has been obtained satisfactorily')

    def delete_data(self, arguments, connection=None, address=None):
        
        node_id, pred_id = arguments

        if str(pred_id) > str(node_id):
            if exist_table(self.db_path, 'tournaments'):
                # Load from tournaments table
                query = f'''DELETE
                FROM tournaments
                WHERE id > '{pred_id}' OR  id < '{node_id}' '''

                delete_row(self.db_path, query)

            if exist_table(self.db_path, 'KnockoutMatches'):
                query = f'''DELETE
                FROM KnockoutMatches
                WHERE tournament_id > '{pred_id}' OR  tournament_id < '{node_id}' '''       
                
                delete_row(self.db_path, query)

            if exist_table(self.db_path, 'FreeForAllMatches'):
                query = f'''DELETE
                FROM FreeForAllMatches
                WHERE tournament_id > '{pred_id}' OR  tournament_id < '{node_id}' ''' 
                
                delete_row(self.db_path, query)
            
            if exist_table(self.db_path, 'participants'):
                query = f'''DELETE
                    FROM participants
                    WHERE tournament_id > '{pred_id}' OR  tournament_id < '{node_id}' ''' 
                
                delete_row(self.db_path, query)
        else:
            if exist_table(self.db_path, 'tournaments'):
                query = f'''DELETE
                FROM tournaments
                WHERE id < '{node_id}' '''

                delete_row(self.db_path, query)

            if exist_table(self.db_path, 'KnockoutMatches'):
                query = f'''DELETE
                FROM KnockoutMatches
                WHERE tournament_id < '{node_id}' '''      
                
                delete_row(self.db_path, query)

            if exist_table(self.db_path, 'FreeForAllMatches'):
                query = f'''DELETE
                FROM FreeForAllMatches
                WHERE tournament_id < '{node_id}' ''' 
                
                delete_row(self.db_path, query)
            
            if exist_table(self.db_path, 'participants'):
                query = f'''DELETE
                    FROM participants
                    WHERE tournament_id < '{node_id}' '''
                
                delete_row(self.db_path, query)
        

node = DataBaseNode()
ip_node_in_chord = os.getenv('NODE_IN_RED')

if ip_node_in_chord:
    node.join(ChordNodeReference(ip_node_in_chord, node.port))
while True: 
    pass