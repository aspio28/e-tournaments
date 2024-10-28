import os
import time
import pickle
import socket
import multiprocessing
from utils import send_to, receive_from, send_and_wait_for_answer, get_dns_address, get_from_dns, send_addr_to_dns, send_ping_to, send_echo_replay 

class DNSNode:
    str_rep = 'DNS'
    port = 5353
    ip = os.getenv('NODE_IP')
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
        
        self.address = (self.ip, self.port)
        self.serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.serverSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.serverSocket.bind(self.address)
        print(f"Listening at {self.address}")
        self.serverSocket.listen(5)
            
        self.ttl_checker = multiprocessing.Process(target=self.check_ttl)
        self.ttl_checker.start()
        
        self.broadcast_handler = multiprocessing.Process(target=self.receive_broadcast)
        self.broadcast_handler.start()
        
        processes = []
        try:
            while True:                
                conn, address = self.serverSocket.accept()
                print('Received CONNECTION from: ', address)
                process = multiprocessing.Process(target=self.handle_connection, args=(conn, address))
                processes.append(process)
                process.start()
                # self.handle_connection(conn, address)
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
            if decoded[0] == "DNS":
                connection.close()
                return status
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
    
    def receive_broadcast(self):
        while True:
            try:
                broadcast_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) 
                broadcast_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                broadcast_sock.bind(('', 6000))
                print("DNS server listening for broadcast messages...")
                while True:
                    data, client_address = broadcast_sock.recvfrom(1024)
                    request = pickle.loads(data)
                    if request[0] == "DNS":
                        print(f"Received discovery message from {client_address[0]}")
                        
                        # Respond with this server's IP address
                        response = b"DNS_SERVER_IP"
                        response = pickle.dumps(["DNS_ADDR", self.address])
                        broadcast_sock.sendto(response, client_address)
            except Exception as e:
                print(f"Error in Receive Broadcast: {e}")
                broadcast_sock.close()

            time.sleep(3)
        
    def check_ttl(self):
        while True:
            try:
                with open(self.address_log, 'rb') as f:
                    logs = pickle.load(f)
                
                for domain, records in logs.items():
                    for record in records:
                        if time.time() >= record['added_at'] + record['ttl']:
                            # Check TTL
                            if send_ping_to(record['data']):
                                #TODO Maybe add a print here
                                # Update added_at if ping is successful
                                record['added_at'] = time.time()
                            else:
                                # Remove record if ping fails
                                logs[domain].remove(record)
                                print(f"Removed record for {record['data']} from domain {domain} due to failed ping.")

                # Save updated logs
                with open(self.address_log, 'wb') as f:
                    pickle.dump(logs, f)

            except Exception as e:
                print(f"Error in TTL checker: {e}")

            time.sleep(5)  # Check every 5 seconds
            
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
                
            # Check if a record with the same IP already exists
            for record in logs[domain]:
                if record['data'] == new_address:
                    record['ttl'] = ttl
                    record['added_at'] = time.time()
                    print(f"Updated TTL and added_at for existing record with IP {new_address} in domain {domain}")
                    with open(self.address_log, 'wb') as f:
                        pickle.dump(logs, f)
                    return True
            
            logs[domain].append(new_record)
            print(f"Added new record with IP {new_address} in domain {domain}")
            with open(self.address_log, 'wb') as f:
                pickle.dump(logs, f)
            return True
        except FileNotFoundError:
            print("DNS error. Logs not found")
            return False

DNSNode()