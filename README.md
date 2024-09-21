# E-tournaments Server

The server of a distributed system that allows
organizing tournaments for a specific game where computing power can be used
of various teams

nodes: database, client, organizator_server, operator_server.

### Network
```bash
docker network create --driver bridge --subnet=172.18.0.0/24 tournament-net
```

### Client container 
```bash
cd client
docker build -t client .
docker run -it --name client1 --net tournament-net --ip 172.18.0.2 -p 5000:5000 client
```
### Server container 
```bash
cd server
docker build -t server .
docker run -it --name server1 --net tournament-net --ip 172.18.0.20 -p 8080:8080 server
```
### Minion container 
```bash
cd minion
docker build -t minion .
docker run -it --name minion1 --net tournament-net --ip 172.18.0.30 -p 8020:8020 minion
```