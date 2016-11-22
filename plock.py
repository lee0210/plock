from struct import pack
from struct import unpack
from uuid import uuid1
from exceptions import RuntimeError
from plock_conf import socket_path
import socket

all_format = '>ci{0}s'
header_format = '>ci'
data_format = '>{0}s'
LOCK = 'L'
UNLOCK = 'U'
EXIT = 'E'
SUCCESS = 'OK'
NOLOCK = 'NOLOCK'

def _send_request(request, time_out=None):
    client_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client_socket.connect(socket_path)
    client_socket.send(request)
    client_socket.settimeout(time_out)
    try:
        result = client_socket.recv(1024)
    except Exception, e:
        result = 'FAILED'
    finally:
        client_socket.close()
    return result

def lock(*args, **dargs):
    if args is (): raise RuntimeError('key is required')
    time_out = dargs['time_out'] if dargs.has_key('time_out') else None
    key = str(args)
    request =  pack(all_format.format(len(key)), LOCK, len(key), key)
    return _send_request(request, time_out)

def bulk_lock(bulk_id, *args, **dargs):
    if args is not (): raise RuntimeError('key is required')
    bulk_id.append(args)
    return lock(*args, **dargs)

def unlock(*args, **dargs):
    if args is (): raise RuntimeError('key is required')
    time_out = dargs['time_out'] if dargs.has_key('time_out') else None
    key = str(args)
    request = pack(all_format.format(len(key)), UNLOCK, len(key), key)
    return _send_request(request, time_out)

def bulk_unlock(bulk_id, time_out):
    result = True
    for l in bulk_id:
        if unlock(*l, time_out=time_out): result = False
    bulk_id = []
    return result

def parse_request(client_socket):
    option, data_length = unpack(header_format, client_socket.recv(5))
    data = ''
    if data_length > 0:
        data, = unpack(data_format.format(data_length), client_socket.recv(data_length))
    return option, data

def exit_program():
    return _send_request(pack(header_format, EXIT, 0))

