import socket
import pickle

from utils import getShaRepr, send_to

class ChordNodeReference:
    def __init__(self, ip: str, port: int = 8040):
        self.id = getShaRepr(ip)
        self.ip = ip
        self.port = port
        self.address = (ip, port)

    def _send_data(self, data = None) -> bytes:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.ip, self.port))
                s.sendall(data)
                return s.recv(1024000)
        except Exception as e:
            print(f"Error sending data: {e}")
            return b''
    
    def find_successor(self, id: int) -> 'ChordNodeReference':
        request = pickle.dumps(['find_successor', (str(id))])
        data = self._send_data(request)
        response = pickle.loads(data)[1]
        return ChordNodeReference(response[1], self.port)

    def find_predecessor(self, id: int) -> 'ChordNodeReference':
        request = pickle.dumps(['find_predecessor', (str(id))])
        data = self._send_data(request)
        response = pickle.loads(data)[1]
        return ChordNodeReference(response[1], self.port)

    @property
    def succ(self) -> 'ChordNodeReference':
        request = pickle.dumps(['get_successor', (None,)])
        data = self._send_data(request)
        response = pickle.loads(data)[1]
        return ChordNodeReference(response[1], self.port)

    @property
    def pred(self) -> 'ChordNodeReference':
        request = pickle.dumps(['get_predecessor', (None,)])
        data = self._send_data(request)
        response = pickle.loads(data)[1]
        return ChordNodeReference(response[1], self.port)

    def notify(self, node: 'ChordNodeReference'):
        request = pickle.dumps(['notify', (node)])
        self._send_data(request)

    def check_predecessor(self):
        request = pickle.dumps(['check_predecessor', (None,)])
        self._send_data(request)

    def closest_preceding_finger(self, id: int) -> 'ChordNodeReference':
        request = pickle.dumps(['closest_preceding_finger', (str(id))])
        data = self._send_data(request)
        response = pickle.loads(data)[1]
        return response[0]
    
    def insert_tournament(self, tournament_type, players_list, tournament_name):
        request = pickle.dumps(['insert_tournament', (tournament_type, players_list, tournament_name)])
        data = self._send_data(request)
        response = pickle.loads(data)[1]
        return response
    
    def save_match(self, match_type, match_id, args):
        request = pickle.dumps(['save_match', (match_type, match_id, args)])
        data = self._send_data(request)
        response = pickle.loads(data)[1]
        return response
    
    def get_match(self, match_type, tournament_id, match_id):
        request = pickle.dumps(['get_match', (match_type, tournament_id, match_id)])
        data = self._send_data(request)
        response = pickle.loads(data)[1]
        return response
    
    def get_player(self, player_ids, tournament_id):
        request = pickle.dumps(['get_player', (player_ids, tournament_id)])
        data = self._send_data(request)
        response = pickle.loads(data)[1]
        return response
    
    def get_tournament(self, tournament_id_req, tournament_type_req):
        request = pickle.dumps(['get_tournament', (tournament_id_req, tournament_type_req)])
        data = self._send_data(request)
        response = pickle.loads(data)[1]
        return response

    def save_tournament(self, tournament_id, tournament_name, tournament_type, ended):
        request = pickle.dumps(['save_tournament', (tournament_id, tournament_name, tournament_type, ended)])
        data = self._send_data(request)
        response = pickle.loads(data)[1]
        return response

    def get_tournament_matches(self, tournament_id, tournament_type):
        request = pickle.dumps(['get_tournament_matches', (tournament_id, tournament_type)])
        data = self._send_data(request)
        response = pickle.loads(data)[1]
        return response

    def get_tournament_status(self, tournament_id):
        request = pickle.dumps(['get_tournament_status', (tournament_id, )])
        data = self._send_data(request)
        response = pickle.loads(data)[1]
        print(response)
        return response

    def get_data(self, node_id, pred_id):
        request = pickle.dumps(['get_data', (node_id, pred_id)])
        data = self._send_data(request)
        print(len(data))
        response = pickle.loads(data)[1]
        return response
    
    def delete_data(self, node_id, pred_id):
        request = pickle.dumps(['delete_data', (node_id, pred_id)])
        self._send_data(request)
        

    def ping(self):
        request = pickle.dumps(['ping_ring', (None,)])
        data = self._send_data(request)
        response = pickle.loads(data)[1]
        return response[0]
    
    # def ping(self):
    #     request = pickle.dumps(['ping', (None,)])
    #     try:
    #         socket.setdefaulttimeout(15) 
    #         data = self._send_data(request)
    #         response = pickle.loads(data)[1]
    #         return response[0]
    #     except Exception as e:
    #         raise e
    #     finally:
    #         socket.setdefaulttimeout(None) 

    def __str__(self) -> str:
        return f'{self.id},{self.ip},{self.port}'

    def __repr__(self) -> str:
        return str(self)
