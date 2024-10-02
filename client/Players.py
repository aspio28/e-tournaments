
from abc import ABC, abstractmethod
import random

class Player(ABC):
    @abstractmethod
    def Move(self):
        pass

    def __str__(self):
        pass
    def my_type(self):
        return ''

class RandomPlayer(Player):
    def __init__(self,name):
        self.name = name
    
    def __str__(self):
        return f"{self.name}"
    def __repr__(self):
        return f"{self.name}"
    def my_type(self):
        return 'random'
    
    def Move(self,board,move):
        while True:
            i = random.randint(0,2)
            j = random.randint(0,2)
            if board[i][j] == -1:
                return i,j,move

class GreedyPlayer(Player):
    def __init__(self,name):
        self.name = name        
    
    def __str__(self):
        return f"{self.name}"
    
    def __repr__(self): 
        return f"{self.name}"
    
    def my_type(self):
        return 'greedy'
    
    def check_win(board, move):
    # Check rows and columns
        for i in range(3):
            if board[i][0] == board[i][1] == move and board[i][2] == -1:
                return (i, 2)
            if board[i][0] == board[i][2] == move and board[i][1] == -1:
                return (i, 1)
            if board[i][1] == board[i][2] == move and board[i][0] == -1:
                return (i, 0)

            if board[0][i] == board[1][i] == move and board[2][i] == -1:
                return (2, i)
            if board[0][i] == board[2][i] == move and board[1][i] == -1:
                return (1, i)
            if board[1][i] == board[2][i] == move and board[0][i] == -1:
                return (0, i)

        # Check diagonals
        if board[0][0] == board[1][1] == move and board[2][2] == -1:
            return (2, 2)
        if board[0][0] == board[2][2] == move and board[1][1] == -1:
            return (1, 1)
        if board[1][1] == board[2][2] == move and board[0][0] == -1:
            return (0, 0)

        if board[0][2] == board[1][1] == move and board[2][0] == -1:
            return (2, 0)
        if board[0][2] == board[2][0] == move and board[1][1] == -1:
            return (1, 1)
        if board[1][1] == board[2][0] == move and board[0][2] == -1:
            return (0, 2)

        return None

    def RandomMove(self,board,move):
        while True:
            i = random.randint(0,2)
            j = random.randint(0,2)
            if board[i][j] == -1:
                return i, j, move
        
    def Move(self,board,move):
        #check if the player can win
        winning_move = GreedyPlayer.check_win(board, move)
        if winning_move:
            return *winning_move, move
        
        #check if the player can prevent the oponent from winning
        opponent = 0 if move == 1 else 1
        blocking_move = GreedyPlayer.check_win(board, opponent)
        if blocking_move:
            return *blocking_move, move
        return self.RandomMove(board,move)

def random_player_move(board, move):
    import random
    while True:
        i = random.randint(0,2)
        j = random.randint(0,2)
        if board[i][j] == -1:
            return i,j,move
   
def greedy_player_move(board, move):
    def check_win(board, move):
        # Check rows and columns
        for i in range(3):
            if board[i][0] == board[i][1] == move and board[i][2] == -1:
                return (i, 2)
            if board[i][0] == board[i][2] == move and board[i][1] == -1:
                return (i, 1)
            if board[i][1] == board[i][2] == move and board[i][0] == -1:
                return (i, 0)

            if board[0][i] == board[1][i] == move and board[2][i] == -1:
                return (2, i)
            if board[0][i] == board[2][i] == move and board[1][i] == -1:
                return (1, i)
            if board[1][i] == board[2][i] == move and board[0][i] == -1:
                return (0, i)

        # Check diagonals
        if board[0][0] == board[1][1] == move and board[2][2] == -1:
            return (2, 2)
        if board[0][0] == board[2][2] == move and board[1][1] == -1:
            return (1, 1)
        if board[1][1] == board[2][2] == move and board[0][0] == -1:
            return (0, 0)

        if board[0][2] == board[1][1] == move and board[2][0] == -1:
            return (2, 0)
        if board[0][2] == board[2][0] == move and board[1][1] == -1:
            return (1, 1)
        if board[1][1] == board[2][0] == move and board[0][2] == -1:
            return (0, 2)
    
    def random_move(board, move):
        import random
        while True:
            i = random.randint(0,2)
            j = random.randint(0,2)
            if board[i][j] == -1:
                return i, j, move
            
    #check if the player can win
    winning_move = check_win(board, move)
    if winning_move:
        return *winning_move, move
    
    #check if the player can prevent the oponent from winning
    opponent = 0 if move == 1 else 1
    blocking_move = check_win(board, opponent)
    if blocking_move:
        return *blocking_move, move
    return random_move(board,move)
    
    
        
# class MinMaxPlayer(Player):
#     def __init__(self,name):
#         self.name = name        
    
#     def __str__(self):
#         return f"{self.name}"
    
#     def __repr__(self): 
#         return f"{self.name}"
    
#     def my_type(self):
#         return 'minmax'    
    
#     def check_win(board, player):
#     # Check rows and columns
#         for i in range(3):
#             if board[i][0] == board[i][1] == board[i][2] == player:
#                 return True
#             if board[0][i] == board[1][i] == board[2][i] == player:
#                 return True

#         # Check diagonals
#         if board[0][0] == board[1][1] == board[2][2] == player:
#             return True
#         if board[0][2] == board[1][1] == board[2][0] == player:
#             return True

#         return False
    
#     def evaluate_board(board):
#     # Check rows and columns
#         for i in range(3):
#             if board[i][0] == board[i][1] == board[i][2] == 1:
#                 return 1  # X wins
#             if board[i][0] == board[i][1] == board[i][2] == 0:
#                 return 0  # O wins

#             if board[0][i] == board[1][i] == board[2][i] == 1:
#                 return 1  # X wins
#             if board[0][i] == board[1][i] == board[2][i] == 0:
#                 return 0  # O wins

#         # Check diagonals
#         if board[0][0] == board[1][1] == board[2][2] == 1:
#             return 1  # X wins
#         if board[0][0] == board[1][1] == board[2][2] == 0:
#             return 0  # O wins

#         if board[0][2] == board[1][1] == board[2][0] == 1:
#             return 1  # X wins
#         if board[0][2] == board[1][1] == board[2][0] == 0:
#             return 0  # O wins

#         # If no one has won, it's a draw
#         if all(cell != -1 for row in board for cell in row):
#             return -1

#         return None  # Game is not over yet

#     def minimax(board, depth, is_maximizing):
#         result = MinMaxPlayer.evaluate_board(board)
#         if result is not None:
#             return result

#         if is_maximizing:
#             best_score = float('-inf')
#             for i in range(3):
#                 for j in range(3):
#                     if board[i][j] == -1:
#                         board[i][j] = "X"
#                         score = minimax(board, depth + 1, False)
#                         board[i][j] = " "
#                         best_score = max(score, best_score)
#             return best_score
#         else:
#             best_score = float('inf')
#             for i in range(3):
#                 for j in range(3):
#                     if board[i][j] == -1:
#                         board[i][j] = "O"
#                         score = minimax(board, depth + 1, True)
#                         board[i][j] = " "
#                         best_score = min(score, best_score)
#             return best_score

#     def ai_move(board):
#         best_score = float('-inf')
#         best_move = None
#         for i in range(3):
#             for j in range(3):
#                 if board[i][j] == " ":
#                     board[i][j] = "X"
#                     score = minimax(board, 0, False)
#                     board[i][j] = " "
#                     if score > best_score:
#                         best_score = score
#                         best_move = (i, j)
#         return best_move
        