from TournamentsLogic import *

player_list = [RandomPlayer("Niley"),RandomPlayer("EsMolesta"),RandomPlayer("MuyMolesta"),RandomPlayer("YHormonal")]

NileyTournament = KnockoutTournament(True,players=player_list)
# last_match = NileyTournament.Run()

# def print_tournament_tree(node:Match, t_id, level=0):
#     st = f"{node}"
#     if len(node.required) != 0:
#         st = print_tournament_tree(Match.match_from_db(t_id, node.required[0]), level + 1)
#         st += '\n' + ' ' * 4 * level + f'-> {node}'
#         st += '\n' + print_tournament_tree(Match.match_from_db(t_id, node.required[1]), level + 1)
#     if level == 0:
#         print(st)
#     return st   
# print_tournament_tree(last_match, NileyTournament.id)
ended = False
Stuart = Minion()
while not ended:
    ended, match = NileyTournament.next_match()
    if ended:
        break
    Stuart.do_a_match(match)
    
    # player1:Player = NileyTournament.get_player_instance(match.player1)
    # player2:Player = NileyTournament.get_player_instance(match.player2)
    # winner = TicTacToe(player1, player2).Run()[2]
    # print(winner)
    # if winner == player1:
    #     match.winner = match.player1
    # elif winner == player2:
    #     match.winner = match.player2
    # else: raise Exception("Un texto si quieres")
    # match.ended = True
    # match.save_to_db()
