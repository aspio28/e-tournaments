from abc import ABC, abstractmethod
import random
class Board:
    def __init__ (self):
        self.board = [[-1,-1,-1],[-1,-1,-1],[-1,-1,-1]]

class TicTacToe:
    def __init__ (self,player1, player2):
        self.board = Board()
        self.players = [player1,player2]
        self.turn = 0
        self.play   
    
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
            if i_winner(i):
                self.winner = self.players[i]
                return True
            
        return False
    
    def Run(self):
        ended= False
        while not ended:

            self.play.append(self.players[self.turn].Play(self.board,self.turn))  

            self.turn+=1
            if self.turn == 2:
                self.turn = 0
        
            ended = TicTacToe.Ended(self)
            
class Player(ABC):
    @abstractmethod
    def Move(self):
        pass

class RandomPlayer(Player):
    def __init__(self,name):
        self.name = name
    
    def Move(self,board,move):
        while True:
            i = random.randint(0,2)
            j = random.randint(0,2)
            if board[i][j] == -1:
                return (move,i,j)
