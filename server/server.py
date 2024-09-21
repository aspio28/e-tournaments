import socket
import time
from TournamentsLogic import *
from Players import *
import pickle

            
            
player_types = {'random': RandomPlayer, 
                'greedy': GreedyPlayer
                }
tournament_type = {'Knockout': KnockoutTournament,
                   'FreeForAll': FreeForAllTournament}


sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

sock.bind(('172.18.0.20', 8080))
sock.listen(1)

minion_address_list = []
minion_busy_list = []
while True:
    
    conn, addr = sock.accept()
    
    data = conn.recv(1024)
    data = pickle.loads(data)
    print(data)
    if data[0] == "Banana?":
        minion_address_list.append(data[1])
        
        # To remove repeated instances of a minion address (may need a better fix)
        temporal_list = minion_address_list
        minion_address_list = []
        [minion_address_list.append(x) for x in temporal_list if x not in minion_address_list]
    
    else:            
        with conn:
            
            if data[0] == "Create new tournament":
                players = []
                for i in range(len(data[2])):
                    players.append(player_types[data[2][i][0]](data[2][i][1]))
                    
                tournament = tournament_type[data[1]](True ,players=players)
            
            ended = False
            
            while not ended:
                ended, match = tournament.next_match()
                if ended:
                    break
                
                while len(minion_address_list) == 0:
                    time.sleep(1)
                    print("Waiting for free minions")
                               
                minion = minion_address_list.pop()
                match = pickle.dumps(match)
                print(minion)
                send_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                send_sock.connect(minion) #Going to have to change i guess, need to know who it needs to connect
                send_sock.sendto(match,minion)
                send_sock.close()
                
                minion_busy_list.append(minion)
                
                conn, addr = sock.accept()
                data = conn.recv(1024)
                data = pickle.dumps(data)
                
                if data == "Le Poofe Guacamole":
                    minion_busy_list.pop(minion_busy_list.index(addr))
                    minion_address_list.append(addr)

            print(f'Recibido del cliente: {tournament}')
            
            #conn.sendto(b'Hola cliente!', addr)
            time.sleep(1)
            
