import time

from chordReference import ChordNodeReference

class SuccList:
    def __init__(self, r, node):
        self.r = r
        self.list = []
        self.boss_node = node
        self.one_node = True

    def set_succ(self, node: ChordNodeReference):
        actual_node = node.succ
        for i in range(0, self.r):
            if self.list[0].id == actual_node.id:
                break
            self.list.append(actual_node)
            try:
                actual_node = actual_node.succ
            except Exception as e:
                print(f"Error in create list of successors: {e}")

    def fix_succ(self):
        while True:

            if self.one_node and self.boss_node.id != self.boss_node.succ.id:
                self.list = [self.boss_node.succ]
                self.one_node = False

            if not self.one_node and self.boss_node.succ != self.list[0].id:
                self.list = [self.boss_node.succ]

            if not self.one_node and len(self.list) < self.r and self.list[-1].succ.id != self.boss_node.id:
                self.list.append(self.list[-1].succ)

            else:
                for i in range(0, len(self.list) - 1):
                    if self.list[i].succ.id != self.list[i + 1].id:
                        self.list.pop()
                        self.list = self.list[:i] + self.list[i].succ + self.list[i:]
            time.sleep(10)

    def __str__(self) -> str:
        return f'{self.list}'