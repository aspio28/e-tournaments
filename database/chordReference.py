import socket
import pickle

from utils import getShaRepr

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
                return s.recv(1024)
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
        request = pickle.dumps(['notify', (node.ip)])
        self._send_data(request)

    def check_predecessor(self):
        request = pickle.dumps(['check_predecessor', (None,)])
        self._send_data(request)

    def closest_preceding_finger(self, id: int) -> 'ChordNodeReference':
        request = pickle.dumps(['closest_preceding_finger', (str(id))])
        data = self._send_data(request)
        response = pickle.loads(data)[1]
        return ChordNodeReference(response[1], self.port)

    def __str__(self) -> str:
        return f'{self.id},{self.ip},{self.port}'

    def __repr__(self) -> str:
        return str(self)
