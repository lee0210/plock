from struct import pack
from struct import unpack
from uuid import uuid1
from exceptions import RuntimeError
from plock_conf import socket_path
import socket

all_format = '>c32si{0}s'
header_format = '>c32si'
data_format = '>{0}s'
LOCK = 'L'
UNLOCK = 'U'

def _send_request(request, time_out=None):
    client_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client_socket.connect(socket_path)
    client_socket.send(request)
    client_socket.settimeout(time_out)
    result = client_socket.recv(1024)
    if result == '': result = False
    client_socket.close()
    return result

def lock(*args, **dargs):
    time_out = None
    if dargs.has_key('time_out'): time_out = dargs['time_out']
    key = str(args)
    request =  pack(all_format.format(len(key)), LOCK, ' '*32, len(key), key)
    _send_request(request, time_out)

def bulk_lock(bulk_id=None, *args):
    if bulk_id is None and args is not ():
        raise RuntimeError('bulk_id required')
    if bulk_id is None:
        return uuid1()
    key = str(args)
    request = pack(all_format.format(len(key)), LOCK, bulk_id.get_hex(), len(key), key)

def unlock(*args):
    key = str(args)
    request = pack(all_format.format(len(key)), UNLOCK, ' '*32, len(key), key)

def bulk_lock(bulk_id=None, *args):
    if bulk_id is None and args is not ():
        raise RuntimeError('bulk_id required')
    if bulk_id is None:
        return uuid1()
    key = str(args)
    request = pack(all_format.format(len(key)), UNLOCK, bulk_id.get_hex(), len(key), key)


def parse(request):
    option, bulk_id, data_length = unpack(header_format, request[0:37])
    data, = unpack(data_format.format(data_length), request[37:])
    return {'option': option, 'bulk_id': bulk_id, 'data': data}

