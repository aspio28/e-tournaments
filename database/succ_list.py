import time

from chordReference import ChordNodeReference

class SuccList:
    def __init__(self, r, node):
        self.r = r
        self.list = []
        self.boss_node = node
        self.one_node = True

    def fix_succ(self):
        while True:

            if self.one_node and self.boss_node.id != self.boss_node.succ.id:
                self.list = [self.boss_node.succ]
                self.one_node = False

            if not self.one_node:
                if self.boss_node.succ != self.list[0].id:
                    self.list = [self.boss_node.succ]

                elif len(self.list) < self.r and self.list[-1].succ.id != self.boss_node.id:
                    self.list.append(self.list[-1].succ)

                else:
                    for i in range(0, len(self.list) - 1):
                        if self.list[i].succ.id != self.list[i + 1].id:
                            self.list.pop()
                            self.list = self.list[:i] + self.list[i].succ + self.list[i:]
            time.sleep(10)

    def __str__(self) -> str:
        return f'{self.list}'