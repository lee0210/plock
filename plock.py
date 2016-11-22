from struct import pack
from struct import unpack
from uuid import uuid1
from exceptions import RuntimeError
from plock_conf import socket_path
import socket

# Proxy format
all_format = '>ci{0}s'
header_format = '>ci'
data_format = '>{0}s'
# Option code
LOCK = 'L'
UNLOCK = 'U'
EXIT = 'E'
PRINT = 'P'
# Return code
SUCCESS = 'OK'
NOLOCK = 'NOLOCK'
RELOCK = 'RELOCK'
_LOCK_SET = []

def _send_request(request, time_out=None):
    try:
        client_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client_socket.connect(socket_path)
        client_socket.send(request)
        client_socket.settimeout(time_out)
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
    if key in _LOCK_SET: return RELOCK
    request =  pack(all_format.format(len(key)), LOCK, len(key), key)
    r = _send_request(request, time_out)
    if r == SUCCESS:
        _LOCK_SET.append(key)
    return r

def bulk_lock(bulk_id, *args, **dargs):
    if args is (): raise RuntimeError('key is required')
    r = lock(*args, **dargs)
    if r == SUCCESS:
        bulk_id.append(args)
    return r

def unlock(*args, **dargs):
    if args is (): raise RuntimeError('key is required')
    time_out = dargs['time_out'] if dargs.has_key('time_out') else None
    key = str(args)
    request = pack(all_format.format(len(key)), UNLOCK, len(key), key)
    r = _send_request(request, time_out)
    if r == SUCCESS:
        _LOCK_SET.remove(key)
    return r

def bulk_unlock(bulk_id, time_out=None):
    result = []
    for i in range(0, len(bulk_id)):
        l = bulk_id.pop(0)
        if not unlock(*l, time_out=time_out) == SUCCESS:
            result.append(l)
    return result

def parse_request(client_socket):
    option, data_length = unpack(header_format, client_socket.recv(5))
    data = ''
    if data_length > 0:
        data, = unpack(data_format.format(data_length), client_socket.recv(data_length))
    return option, data

def exit_program():
    return _send_request(pack(header_format, EXIT, 0))

def print_lock():
    return _send_request(pack(header_format, PRINT, 0))

