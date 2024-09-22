import os
import time
import pickle
import socket
from utils import DNS_ADDRESS, send_to, receive_from, send_and_wait_for_answer, get_from_dns, send_addr_to_dns, send_ping_to, send_echo_replay 

class DNSNode:
    str_rep = 'DNS'
    def __init__(self):
        self.requests = {'ping': send_echo_replay,
                        'Failed': None,
                        'GET': self.get_domain, 
                        'POST': self.add_domain,}
        current_dir = os.path.abspath(os.path.dirname(__file__))
        self.address_log = os.path.join(current_dir, 'address_log.bin')
        
        # Restart or create adresses log
        logs = {'DataBase':[], 'Minion':[], 'Server':[]}
        with open(self.address_log, 'wb') as f:
            pickle.dump(logs, f)
        
        self.address = DNS_ADDRESS
        self.serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.serverSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.serverSocket.bind(self.address)
        print(f"Listening at {self.address}")
        self.serverSocket.listen(5)
        try:
            while True:                
                conn, address = self.serverSocket.accept()
                print('CONNECTED: ', address)
                self.attend_connection(conn, address)
        except Exception as er:
            raise er
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
        
    def get_domain(self, arguments: tuple, connection, address):
        with open(self.address_log, 'rb') as f:
            logs = pickle.load(f)
        domain = arguments[0]
        records = logs[domain]
        addresses = [ r['data'] for r in records ]
        addresses = list(set(addresses)) # randomize 
        
        answer = pickle.dumps(['sent_addr', addresses, self.address])
        all_good = send_to(answer, connection)
        return all_good
    
    def add_domain(self, arguments: tuple, connection, address):
        domain, new_address, ttl = arguments
        new_record = {'domain': domain, 'ttl': ttl, 'data': new_address, 'added_at': time.time()}
        
        try:
            with open(self.address_log, 'rb') as f:
                logs = pickle.load(f)
            logs[domain].append(new_record)
            with open(self.address_log, 'wb') as f:
                pickle.dump(logs, f)
            return True
        except FileNotFoundError:
            print("DNS error. Logs not found")
            return False

DNSNode()