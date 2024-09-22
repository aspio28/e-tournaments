import socket
import pickle

def Run():
    server_address = ('172.18.0.20', 8080)
    first_input = input("Do you want to create a new tournament? (Y/N)")
    if first_input == "Y":
                
        type_of_tournament = input("What kind of tournament it will be? (1 = Knockout, 2 = FreeForAll)")
        
        if type_of_tournament == "1":
            type_of_tournament = "Knockout"
        elif type_of_tournament == "2":
            type_of_tournament = "FreeForAll"
        else:
            print("Invalid value")
            return
               
        amount_of_players = input("How many players there will be: ")
        
        try:
            amount_of_players = int(amount_of_players)
        except:
            print("The value of amout of players wasnt valid")
            return        
        
        print("Especify the type of player (random/greedy) and the name , separated by a space. (Each player will be a different input)")   
            
        list_of_players = []
        
        for i in range(amount_of_players):
            
            line = input()
            type_of_player , name_of_player = line.split()
            list_of_players.append((type_of_player, name_of_player))
        
        package = ("Create new tournament", type_of_tournament, list_of_players)
        # tournament = tournament_type[type_of_tournaments](True ,players=list_of_players)
        package = pickle.dumps(package)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(server_address) #Going to have to change i guess, need to know who it needs to connect
        
        sock.sendto(package,server_address)

        # data = sock.recv(1024)
        # print(data.decode('utf-8'))

        sock.close()
    elif first_input == "1":
        package = ("Create new tournament","FreeForAll",[("random","A"),("random","B"),("greedy","C")])
        
        package = pickle.dumps(package)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(server_address) #Going to have to change i guess, need to know who it needs to connect
        
        sock.sendto(package,server_address)

        # data = sock.recv(1024)
        # print(data.decode('utf-8'))

        sock.close()
        
Run()
