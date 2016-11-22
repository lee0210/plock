import socket
import os
from plock_conf import socket_path
from plock import parse

os.unlink(socket_path)

server_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
server_socket.bind(socket_path)
server_socket.listen(10)

terminal = False
while not terminal:
    client_socket, addr = server_socket.accept()
    request = client_socket.recv(1024)
    result = parse(request)
    print result
    client_socket.send('OK')
    client_socket.close()


