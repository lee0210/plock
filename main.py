#!/usr/bin/python
import socket
import os
import sys
import plock
from plock_conf import socket_path
from plock_conf import log_path

if os.path.exists(socket_path): os.unlink(socket_path)
sys.stdout = sys.stderr = open(log_path, 'a+', 0)
os.chmod(log_path, 0x777)

server_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
server_socket.bind(socket_path)
server_socket.listen(10)

bulk_lock = []
waiting_queue = {}

def send_message_and_close(s, d):
    try: 
        s.send(d) 
        return True
    except:
        return False
    finally:
        s.close()

terminal = False

pid = os.fork()

while not terminal and pid == 0:
    client_socket, addr = server_socket.accept()
    option, data = plock.parse_request(client_socket)
    if option == plock.LOCK:
        if data in bulk_lock:
            if not waiting_queue.has_key(data): waiting_queue[data] = []
            waiting_queue[data].append(client_socket)
        else:
            bulk_lock.append(data)
            send_message_and_close(client_socket, plock.SUCCESS)
    if option == plock.UNLOCK:
        if data in bulk_lock:
            no_pending = True
            if waiting_queue.has_key(data):
                wq = waiting_queue[data]
                try:
                    cs = wq.pop(0)
                    while not send_message_and_close(cs, plock.SUCCESS):
                        cs = wq.pop(0)   
                    no_pending = False
                except IndexError, e:
                    waiting_queue.pop(data)
            if no_pending:
                bulk_lock.remove(data)
            send_message_and_close(client_socket, plock.SUCCESS)
        else:
            send_message_and_close(client_socket, plock.NOLOCK)
    if option == plock.EXIT:
        terminal = True
        send_message_and_close(client_socket, plock.SUCCESS)
    if option == plock.PRINT:
        print bulk_lock
        print waiting_queue
        send_message_and_close(client_socket, plock.SUCCESS)
    
os.chmod(socket_path, 0x777)
