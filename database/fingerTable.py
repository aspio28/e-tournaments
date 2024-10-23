import time
import threading
import pickle

from chordReference import ChordNodeReference
from utils import in_between, send_to

class FingerTable:
    def __init__(self, node):
        self.node = node
        self.m = 160
        self.finger = [self.node.ref] * self.m
        self.next = 0

        
        threading.Thread(target=self.fix_fingers, daemon=True).start()  

    # Method to find the successor of a given id
    def find_succ(self, id, connection=None, address=None) -> 'ChordNodeReference':
        id = int(id)
        node = self.find_pred(id)  # Find predecessor of id
        if connection:
            answer = pickle.dumps(['successor', (node.succ.id, node.succ.ip)])
            all_good = send_to(answer, connection)
            return all_good
        else:
            return node.succ  # Return successor of that node

    # Method to find the predecessor of a given id
    def find_pred(self, id: int) -> 'ChordNodeReference':
        node = self.node.ref
        while not in_between(id, node.id, node.succ.id):
            node = node.closest_preceding_finger(id)
        return node

    # Method to find the closest preceding finger of a given id
    def closest_preceding_finger(self, id: int) -> 'ChordNodeReference':
        for i in range(self.m - 1, -1, -1):
            if self.finger[i] and in_between(self.finger[i].id, self.node.id, id):
                return self.finger[i]
        return self.node.ref
    
    def fix_fingers(self):
        while True:
            try:
                self.next += 1
                if self.next >= self.m:
                    self.next = 0
                self.finger[self.next] = self.find_succ((self.node.id + 2 ** self.next) % 2 ** self.m)
            except Exception as e:
                print(f"Error in fix_fingers: {e}")
            time.sleep(10)