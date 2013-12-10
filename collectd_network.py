#!/usr/bin/env python
#
# Collectd Network Module
# *Pack collectd values into a network datagram
# *Dispatch the datagram over multicast/unicast
# 
# More info about the packet structure here:
# https://collectd.org/wiki/index.php/Binary_protocol
#
# Kamil Czauz 
# 


import re
import time
import socket
import struct

TYPE_HOST            = 0x0000
TYPE_TIME            = 0x0001
TYPE_PLUGIN          = 0x0002
TYPE_PLUGIN_INSTANCE = 0x0003
TYPE_TYPE            = 0x0004
TYPE_TYPE_INSTANCE   = 0x0005
TYPE_VALUES          = 0x0006
TYPE_INTERVAL        = 0x0007
LONG_INT_CODES = [TYPE_TIME, TYPE_INTERVAL]
STRING_CODES = [TYPE_HOST, TYPE_PLUGIN, TYPE_PLUGIN_INSTANCE, TYPE_TYPE, TYPE_TYPE_INSTANCE]

VALUE_COUNTER  = 0
VALUE_GAUGE    = 1
VALUE_DERIVE   = 2
VALUE_ABSOLUTE = 3
VALUE_CODES = {
    VALUE_COUNTER:  "!Q",
    VALUE_GAUGE:    "<d",
    VALUE_DERIVE:   "!q",
    VALUE_ABSOLUTE: "!Q"
}


def pack_numeric(type_code, number):
    return struct.pack("!HHq", type_code, 12, number)

def pack_string(type_code, string):
    return struct.pack("!HH", type_code, 5 + len(string)) + string + "\0"

def pack_value(name, value):
    return "".join([
        pack(TYPE_TYPE_INSTANCE, name),
        struct.pack("!HHH", TYPE_VALUES, 15, 1),
        struct.pack("<Bd", VALUE_GAUGE, value) ])

def pack(id, value):
    if isinstance(id, basestring):
        return pack_value(id, value)
    elif id in LONG_INT_CODES:
        return pack_numeric(id, value)
    elif id in STRING_CODES:
        return pack_string(id, value)
    else:
        raise AssertionError("invalid type code " + str(id))

class Collectd(object):
    '''Collectd Class used to pack and send collectd datagrams over network.  
    collectd_server, collectd_port and interval must be defined or 
    defaults will be used.'''
    def __init__(self, collectd_host = "localhost", collectd_port = 25826,
                interval = 30):
        self._collectd_addr = (collectd_host, collectd_port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.interval = interval

    def packet(self, 
              plugin_type, 
              values,
              host=socket.gethostname(), 
              plugin="python", 
              type_instance="", 
              plugin_instance=None, 
              time=None):
        ''' describe a packet according to http://goo.gl/I4KKyz
            plugin_type (string) and value (int) are required params '''

        self.plugin_type = plugin_type
        self.type_instance = type_instance
        self.host = host
        self.plugin = plugin
        self.plugin_instance = plugin_instance
        self.time = time
        self.values = values


    def dispatch(self):
        ''' send data over the network'''
        header = "".join([
            pack(TYPE_HOST, self.host),
            pack(TYPE_TIME, self.time or time.time()),
            pack(TYPE_PLUGIN, self.plugin),
            pack(TYPE_PLUGIN_INSTANCE, self.plugin_instance),
            pack(TYPE_TYPE, self.plugin_type),
            pack(TYPE_INTERVAL, self.interval)
        ])

        length_of_value_parts = 2+2+2+len(self.values) * (1+8)

        #size of type code TYPE_VALUES (16 bits) + 
        #size of this length field (16 bits) +
        #size of number of values field (16 bits) + 
        #( num of values * (size of data type code (8 bits) +
        #                  size of vale (64 bits) ) )
                                   
        value_parts=[ 
            pack(TYPE_TYPE_INSTANCE, self.type_instance),
            struct.pack("!HHH", TYPE_VALUES, 
                        length_of_value_parts, len(self.values) )
            ]

        for val in self.values:
            value_parts.append( struct.pack("<B", VALUE_GAUGE) )
        for val in self.values:
            value_parts.append( struct.pack("<d", val) )


        body = "".join(value_parts)
        msg = "".join([header, body])
        self.sock.sendto(msg, self._collectd_addr)

#### bytes converter
#http://goo.gl/pQxmXW
SYMBOLS = {
    'customary'     : ('B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'),
    'customary_ext' : ('byte', 'kilo', 'mega', 'giga', 'tera', 'peta', 'exa',
                      'zetta', 'iotta'),
    'iec'           : ('Bi', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi', 'Yi'),
    'iec_ext'       : ('byte', 'kibi', 'mebi', 'gibi', 'tebi', 'pebi', 'exbi',
                                              'zebi', 'yobi'),
}

def bytes2human(n, format='%(value).1f %(symbol)s', symbols='customary'):
    """
    Convert n bytes into a human readable string based on format.
    symbols can be either "customary", "customary_ext", "iec" or "iec_ext",
    see: http://goo.gl/kTQMs

      >>> bytes2human(0)
      '0.0 B'
      >>> bytes2human(0.9)
      '0.0 B'
      >>> bytes2human(1)
      '1.0 B'
      >>> bytes2human(1.9)
      '1.0 B'
      >>> bytes2human(1024)
      '1.0 K'
      >>> bytes2human(1048576)
      '1.0 M'
      >>> bytes2human(1099511627776127398123789121)
      '909.5 Y'

      >>> bytes2human(9856, symbols="customary")
      '9.6 K'
      >>> bytes2human(9856, symbols="customary_ext")
      '9.6 kilo'
      >>> bytes2human(9856, symbols="iec")
      '9.6 Ki'
      >>> bytes2human(9856, symbols="iec_ext")
      '9.6 kibi'

      >>> bytes2human(10000, "%(value).1f %(symbol)s/sec")
      '9.8 K/sec'

      >>> # precision can be adjusted by playing with %f operator
      >>> bytes2human(10000, format="%(value).5f %(symbol)s")
      '9.76562 K'
    """
    n = int(n)
    if n < 0:
        raise ValueError("n < 0")
    symbols = SYMBOLS[symbols]
    prefix = {}
    for i, s in enumerate(symbols[1:]):
        prefix[s] = 1 << (i+1)*10
    for symbol in reversed(symbols[1:]):
        if n >= prefix[symbol]:
            value = float(n) / prefix[symbol]
            return format % locals()
    return format % dict(symbol=symbols[0], value=n)

def human2bytes(s):
    """
    Attempts to guess the string format based on default symbols
    set and return the corresponding bytes as an integer.
    When unable to recognize the format ValueError is raised.

      >>> human2bytes('0 B')
      0
      >>> human2bytes('1 K')
      1024
      >>> human2bytes('1 M')
      1048576
      >>> human2bytes('1 Gi')
      1073741824
      >>> human2bytes('1 tera')
      1099511627776

      >>> human2bytes('0.5kilo')
      512
      >>> human2bytes('0.1  byte')
      0
      >>> human2bytes('1 k')  # k is an alias for K
      1024
      >>> human2bytes('12 foo')
      Traceback (most recent call last):
          ...
      ValueError: can't interpret '12 foo'
    """
    init = s
    num = ""
    while s and s[0:1].isdigit() or s[0:1] == '.':
        num += s[0]
        s = s[1:]
    num = float(num)
    letter = s.strip()
    for name, sset in SYMBOLS.items():
        if letter in sset:
            break
    else:
        if letter == 'k':
            # treat 'k' as an alias for 'K' as per: http://goo.gl/kTQMs
            sset = SYMBOLS['customary']
            letter = letter.upper()
        else:
            raise ValueError("can't interpret %r" % init)
    prefix = {sset[0]:1}
    for i, s in enumerate(sset[1:]):
        prefix[s] = 1 << (i+1)*10
    return int(num * prefix[letter])

