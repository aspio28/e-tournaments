from abc import ABC, abstractmethod
import random
import copy

class Board:
    def __init__ (self):
        self.board = [[-1,-1,-1],[-1,-1,-1],[-1,-1,-1]]

    def __getitem__(self,key):
        return self.board[key]

class Player(ABC):
    @abstractmethod
    def Move(self):
        pass

    def __str__(self):
        pass


class RandomPlayer(Player):
    def __init__(self,name):
        self.name = name
    
    def __str__(self):
        return f"{self.name}"
    def __repr__(self):
        return f"{self.name}"
    
    def Move(self,board,move):
        while True:
            i = random.randint(0,2)
            j = random.randint(0,2)
            if board[i][j] == -1:
                return i,j,move
            
class TicTacToe:
    def __init__ (self,player1, player2):
        self.board = Board()
        self.players = [player1,player2]
        self.turn = 0
        self.play = []
        self.winner = -1
    
    def Ended(self):
        def i_winner(self,i):
            if (self.board[0][0]== i and self.board[1][1]== i and self.board[2][2]== i) or (self.board[0][2]== i and self.board[1][1]== i and self.board[2][0]== i):
                return True
            for j in range(3):
                if (self.board[j][0]== i and self.board[j][1]== i and self.board[j][2]== i) or ((self.board[0][j]== i and self.board[1][j]== i and self.board[2][j]== i)):
                    return True
            
        # for row in self.board:
        #     if not (any(-1 in row)):
        #         return True
            
        if not (any(-1 in row for row in self.board)):
            return True
        
        for i in range(2):
            if i_winner(self,i):
                self.winner = self.players[i]
                return True
            
        return False
    
    def Run(self):
        ended= False
        while not ended:
            move  = self.players[self.turn].Move(self.board,self.turn)
            #TODO Check if the player is cheating
            self.play.append(move)  
            self.board[move[0]][move[1]]= move [2]
            self.turn+=1
            if self.turn == 2:
                self.turn = 0
            
            ended = TicTacToe.Ended(self)
        
        if self.winner == -1: #THIS IS NOT GOOD NEED WORK
            random_winner = random.randint(0,1)
            return (self.players[0],self.players[1],self.players[random_winner])

        return (self.players[0],self.players[1],self.winner)
            
class Tournament(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def Run(self):
        pass

class BridgeTournament(Tournament):
    def __init__(self,players):
        self.players_list = players
        random.shuffle(self.players_list)
        self.actual_bracket = copy.deepcopy(self.players_list)
        self.next_bracket = []
        self.pending = []

    def Initial_check(self):
        if len(self.players_list)%2 == 0:
            return True
        return False
    
    def Next_match(self):
        first = random.randint(0,len(self.actual_bracket)-1)
        second = random.randint(0,len(self.actual_bracket)-1)
        while not first !=second:
            second = random.randint(0,len(self.actual_bracket)-1)
        self.pending.append((self.actual_bracket[first],self.actual_bracket[second]))
        r_second = max(first,second)
        r_first = min (first,second)
        return self.actual_bracket.pop(r_second) , self.actual_bracket.pop(r_first)

    def Ended(self):
        if len(self.actual_bracket)== 0:
            if len(self.next_bracket)==1:
                return True
            self.actual_bracket = self.next_bracket
            self.next_bracket = []
            return False
        return False

    def Run(self):
        ended = False
        if not self.Initial_check():
            raise Exception("Invalid")
        while not ended:
            next = self.Next_match()
            TTT = TicTacToe(next[0],next[1]).Run()
            for i in range(len(self.pending)):
                if self.pending[i] == (TTT[0],TTT[1]) or self.pending[i] == (TTT[1],TTT[0]):
                    self.pending.pop(i)
                    self.next_bracket.append(TTT[2])
                    break
            ended = self.Ended()
        return self.next_bracket[0]
