#!/usr/bin/python
#coding: utf-8

#Copyright (c) 2013, Dexma Sensors S.L. (info@dexmatech.com)
#All rights reserved.
#
#Redistribution and use in source and binary forms, with or without
#modification, are permitted provided that the following conditions are met:
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in the
#      documentation and/or other materials provided with the distribution.
#    * Neither the name of the Dexma Sensors S.L. nor the
#      names of its contributors may be used to endorse or promote products
#      derived from this software without specific prior written permission.
#
#THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
#AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
#IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
#DISCLAIMED. IN NO EVENT SHALL Dexma Sensors S.L. BE LIABLE FOR ANY
#DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
#(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
#LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
#ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
#(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
#SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import time
import httplib
import json
import logging

try:
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass
    logging.NullHandler = NullHandler
    from logging import NullHandler


class DexcellLoggingHandler(logging.Handler):
    """
    A class which sends records to a DEXCell Energy manager server,
    """

    def __init__(self, gateway, token, host='www.dexcell.com', port=80,
                 url='/api/v2/gateway/log/set/'):
        """
        Initialize the instance with the host, the request URL and token
        """
        logging.Handler.__init__(self)
        self.gateway = gateway
        self.token = token
        self.host = host
        self.port = port
        self.url = url
        self.method = 'POST'

    def mapLogRecord(self, record):
        """
        Default implementation of mapping the log record into a dict
        that is sent as the CGI data. Overwrite in your class.
        Contributed by Franz  Glasner.
        """
        return record.__dict__

    def emit(self, record):
        """
        Emit a record.

        Send the record to the Web server as a percent-encoded dictionary
        """
        try:
            recorddict = record.__dict__
            name = recorddict['name']
            msg = recorddict['msg']
            level = recorddict['levelname']
            module = recorddict['module']
            ts = time.gmtime(recorddict['created'])
            dexcellmsgdict = {}
            dexcellmsgdict['level'] = level
            dexcellmsgdict['message'] = "%s - %s - %s" % (module, name, msg)
            dexcellmsgdict['tz'] = 'UTC'
            dexcellmsgdict['ts'] = time.strftime('%Y%m%d%H%M%S', ts)
            jsondexcellmsg = json.dumps(dexcellmsgdict)
            host = self.host
            port = self.port
            h = httplib.HTTPConnection(host, port, timeout=10.0)
            url = self.url + self.gateway
            # support multiple hosts on one IP address...
            # need to strip optional :port from host, if present
            i = host.find(":")
            if i >= 0:
                host = host[:i]
            headers = {
                "Host": host,
                "Content-type": "application/json",
                "Content-length": str(len(jsondexcellmsg)),
                'x-dexcell-token': str(self.token)
            }
            h.request(self.method, url, jsondexcellmsg, headers)
            h.getresponse()                                                     # can't do anything with the result
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)


class DexcellServiceMessage(object):

    # AMBIENT
    SERVICE_TEMPERATURE = 301                   # ºC
    SERVICE_HUMIDITY = 302                      # %
    SERVICE_LIGHT = 303                         # LUX
    SERVICE_AIR_QUALITY_CO = 306                # ppm
    SERVICE_AIR_QUALITY_CO2 = 307               # ppm
    SERVICE_SOUND_INTENSITY = 308               # dB
    SERVICE_SOIL_HUMIDITY = 309                 # %

    #ENERGY
    SERVICE_ACTIVE_POWER = 401                  # W
    SERVICE_ACTIVE_ENERGY = 402                 # kWh
    SERVICE_INDUCTIVE_REACTIVE_POWER = 403      # VAR
    SERVICE_INDUCTIVE_REACTIVE_ENERGY = 404     # kVARh
    SERVICE_VOLTAGE = 405                       # V
    SERVICE_CURRENT = 406                       # A
    SERVICE_CAPACITIVE_REACTIVE_POWER = 407     # VAR
    SERVICE_CAPACITIVE_REACTIVE_ENERGY = 408    # kVARh
    SERVICE_APPARENT_POWER = 409                # VA
    SERVICE_APPARENT_ENERGY = 410               # kVAh
    SERVICE_COS_PHI = 411                       # 0..1
    SERVICE_POWER_FACTOR = 412                  # -1..1
    SERVICE_NEUTRAL_CURRENT = 413               # A
    SERVICE_FREQUENCY = 414                     # Hz
    SERVICE_GAS_VOLUME = 419                    # m^3
    SERVICE_GAS_ENERGY = 420                    # kWh
    SERVICE_THD_VOLTAGE = 422                   # %
    SERVICE_THD_CURRENT = 423                   # %
    SERVICE_AVERAGE_CURRENT = 426               # A
    SERVICE_FUEL_VOLUME = 432                   # m^3
    SERVICE_FUEL_ENERGY = 433                   # kWh
    SERVICE_EXP_ACTIVE_ENERGY = 452             # kWh
    SERVICE_EXP_INDUCTIVE_R_ENERGY = 454        # kVARh
    SERVICE_EXP_CAPACITIVE_R_ENERGY = 458       # kVARh

    #Generic
    SERVICE_BINARY_INPUT = 501                  # None
    SERVICE_PULSE_COUNTER = 502                 # None
    SERVICE_GENERIC_VALUE = 503                 # None

    #Device
    SERVICE_DEVICE_TEMPERATURE = 701            # ºC

    #HVAC
    SERVICE_THERMAL_POWER = 801                 # kW
    SERVICE_THERMAL_ENERGY = 802                # kWh
    SERVICE_HOT_WATER_VOLUME = 803              # m^3
    SERVICE_MASS_FLOW = 804                     # (m^3)/h
    SERVICE_INLET_TEMPERATURE = 805             # ºC
    SERVICE_OUTLET_TEMPERATURE = 806            # ºC
    SERVICE_COP_EER = 807                       # No unit
    SERVICE_LOW_PRESSURE = 808                  # Bar
    SERVICE_HIGH_PRESSURE = 809                 # Bar

    #Water
    SERVICE_WATER_VOLUME = 901                  # m^3
    SERVICE_WATER_FLOW = 902                    # (m^3)/h

    def __init__(self, node, service, timestamp, value, seq):
        try:
            self.node = str(node)
            self.service = int(service)
            self.timestamp = timestamp
            self.value = float(value)
            self.seqnum = int(seq)
        except:
            raise Exception("Problem creating DexcellServiceMessage")

    def __repr__(self):
        timeformat = "%Y/%m/%d %H:%M"
        outPut = "DexcellServiceMessage(node=%s" % str(self.node)
        outPut += ", service=%s" % str(self.service)
        outPut += ", timestamp=%s" % time.strftime(timeformat, self.timestamp)
        outPut += ", value=%s" % str(self.value)
        outPut += ", seqnum=%s)" % str(self.seqnum)
        return outPut

    def __eq__(self, other):
        if not isinstance(other, DexcellServiceMessage):
            return False
        e1 = self.node == other.node
        e2 = self.service == other.service
        e3 = self.timestamp == other.timestamp
        e4 = self.value == other.value
        e5 = self.seqnum == other.seqnum
        return e1 and e2 and e3 and e4 and e5

    def __ne__(self, other):
        if not isinstance(other, DexcellServiceMessage):
            return True
        e1 = not self.node == other.node
        e2 = not self.service == other.service
        e3 = not self.timestamp == other.timestamp
        e4 = not self.value == other.value
        e5 = not self.seqnum == other.seqnum
        return e1 or e2 or e3 or e4 or e5


class DexcellSender(object):

    DEFAULT_SERVER = 'insert.dexcell.com'
    DEFAULT_URL = '/insert-json.htm'
    DEFAULT_LOGFILE = '/var/log/dexma/DexcellSender.log'
    DEFAULT_LOGLEVEL = logging.INFO
    DEFAULT_GATEWAY = 'None'
    DEFAULT_LOGGERNAME = 'DexcellSender'

    def __init__(self, gateway=DEFAULT_GATEWAY, loggerName=DEFAULT_LOGGERNAME,
                 logfile=DEFAULT_LOGFILE, loglevel=DEFAULT_LOGLEVEL,
                 server=DEFAULT_SERVER, url=DEFAULT_URL,
                 https=True, timeout=30.0):
        self.__https = https
        self.__server = server
        self.__url = url
        self.__timeout = timeout
        self.__gateway = gateway
        self.__logger = logging.getLogger(loggerName)
        if len(self.__logger.handlers) == 0:
            self.__logger.setLevel(loglevel)
            self.handler = NullHandler()
            h_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            self.handler.setFormatter(logging.Formatter(h_format))
            self.__logger.addHandler(self.handler)

    def setup(self, gateway=DEFAULT_GATEWAY, loggerName=DEFAULT_LOGGERNAME,
              logfile=DEFAULT_LOGFILE, loglevel=DEFAULT_LOGLEVEL,
              server=DEFAULT_SERVER, url=DEFAULT_URL):
        """Setup the Dexcell Sender Object
        """
        self.__server = server
        self.__url = url
        self.__gateway = gateway
        self.__logger = logging.getLogger(loggerName)
        if len(self.__logger.handlers) == 0:
            self.__logger.setLevel(loglevel)
            self.handler = NullHandler()
            h_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            self.handler.setFormatter(logging.Formatter(h_format))
            self.__logger.addHandler(self.handler)

    def changeGateway(self, gateway):
        """Change the gateway mac that will be sent
        """
        self.__gateway = gateway

    def __insertRawJSONData(self, data):
        """Insert the raw data string to the server
        """
        params = 'data=' + data
        headers = {"Content-type": "application/x-www-form-urlencoded",
                   "Accept": "text/plain"}
        if self.__https:
            conn = httplib.HTTPSConnection(self.__server,
                                           timeout=self.__timeout)
        else:
            conn = httplib.HTTPConnection(self.__server,
                                          timeout=self.__timeout)
        conn.request("POST", self.__url, params, headers)
        error = True
        maxerror = 0
        while error:
            try:
                response = conn.getresponse()
                error = False
            except:
                print self.__logger.exception("Error inserting data")
                maxerror = maxerror + 1
                time.sleep(1)
                if maxerror > 10:
                    return (-1, 'FAIL')
        logger_msg_wo_params = "Insert from %s with status %s and result %s"
        logger_params = (self.__gateway, str(response.status),
                         str(response.getheader('data')))
        logger_message = logger_msg_wo_params % logger_params
        self.__logger.debug(logger_message)
        return response.status, response.getheader('data')

    def insertDexcellServiceMessage(self, serviceMessage,
                                    timezone='UTC', extraparams={}):
        '''Insert a single DexcellServiceMessage
        '''
        reading = {
            'nodeNetworkId': str(serviceMessage.node),
            'serviceNetworkId': int(serviceMessage.service),
            'value': float(serviceMessage.value),
            'seqNum': int(serviceMessage.seqnum),
            'timeStamp': time.strftime("%Y-%m-%dT%H:%M:%S.000 " + timezone,
                                       serviceMessage.timestamp)
        }
        data = {
            'gatewayId': self.__gateway,
            'service': [reading]
        }
        for key in extraparams.keys():
            data[key] = extraparams[key]
        result = self.__insertRawJSONData(json.dumps(data))
        return result

    def insertDexcellServiceMessages(self, serviceMessageIterator,
                                     timezone='UTC', extraparams={}):
        """ Insert many DexcellServiceMessages at once
        """
        readings = []
        for serviceMessage in serviceMessageIterator:
            reading = {
                'nodeNetworkId': str(serviceMessage.node),
                'serviceNetworkId': int(serviceMessage.service),
                'value': float(serviceMessage.value),
                'seqNum': int(serviceMessage.seqnum),
                'timeStamp': time.strftime("%Y-%m-%dT%H:%M:%S.000 " + timezone,
                                           serviceMessage.timestamp)
            }
            readings.append(reading)
        data = {
            'gatewayId': self.__gateway,
            'service': readings
        }
        for key in extraparams.keys():
            data[key] = extraparams[key]
        result = self.__insertRawJSONData(json.dumps(data))
        return result

