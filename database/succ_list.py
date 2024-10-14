from chordReference import ChordNodeReference

class SuccList:
    def __init__(self, r, id):
        self.r = r
        self.list = []

    def set_succs(self, node: ChordNodeReference):
        self.list.append(node.succ)