import random

class Player:
    def __init__(self, name, function):
        self.name = name
        self.move_function = function
    
    def Move(self, board, move):
        return self.move_function(board, move)

    def __str__(self):
        return f"{self.name}"
    def __repr__(self):
        return f"{self.name}"

class Board:
    def __init__ (self):
        self.board = [[-1,-1,-1],[-1,-1,-1],[-1,-1,-1]]

    def __getitem__(self,key):
        return self.board[key]

class TicTacToe:
    def __init__ (self, player1:Player, player2:Player):
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
        tries = 0
        while not ended:
            move  = self.players[self.turn].Move(self.board,self.turn)
            #TODO Check if the player is cheating
            self.play.append(move)  
            self.board[move[0]][move[1]]= move [2]
            self.turn = (self.turn+1)%2
            # if self.turn == 2:
            #     self.turn = 0
            
            ended = TicTacToe.Ended(self)
            
            tries+=1
            if self.winner == -1 and ended: #If the match ends in a tie, retry playing it
                if tries <= 3: #Unless already have been retried 3 times, in that case just pick a random winner cause yolo
                    random_winner = random.randint(0,1)
                    return (self.players[0],self.players[1],self.players[random_winner])
                else:
                    ended = False
                    self.board = Board()
                    self.turn = 0
                    self.play = []
                    
        return (self.players[0],self.players[1],self.winner)
