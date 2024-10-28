import time
import threading

from chordReference import ChordNodeReference

class SuccList:
    list_lock = threading.Lock()

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
                try:
                    if self.boss_node.succ.id != self.list[0].id:
                        self.list = [self.boss_node.succ]
                    
                    elif len(self.list) < self.r and self.list[-1].succ.id != self.boss_node.id:
                        self.list.append(self.list[-1].succ)

                    else:
                        for i in range(0, len(self.list) - 1):
                            if self.list[i].succ.id != self.list[i + 1].id:
                                self.list.pop()
                                self.list = self.list[:i] + self.list[i].succ + self.list[i:]
                except:
                    print('Failure in some successor...Fixing Succs List')
                    with self.list_lock:
                        for i in range(0, len(self.list)):
                            try:
                                ok = self.list[i].ping()
                            except:
                                try:
                                    self.list.pop(i)
                                    if len(self.list) == 0:
                                        self.one_node = True
                                except:
                                    self.one_node = True
            time.sleep(10)

    def check_succ(self):
        if len(self.list) == 0:
            time.sleep(3)
        with self.list_lock:
            try:
                ok = self.list[0].ping()
                if ok:
                    return self.list[0]
            except:
                print(f"Successor not find... Finding new successor")
                self.list.pop(0)
                if len(self.list) == 0:
                    self.boss_node.pred = None
                    self.one_node = True
                    return self.boss_node.ref
                else:
                    return self.list[0]
    
    def __str__(self) -> str:
        return f'{self.list}'