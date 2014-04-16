#!/usr/bin/python
#coding: utf-8

#Copyright (c) 2014, Dexma Sensors S.L. (info@dexmatech.com)
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


import httplib
import json
import logging
import re
import time
import urllib2
from datetime import datetime


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

    def __init__(self, gateway, token, host='www.dexcell.com', port=0,
                 url='/api/v2/gateway/log/set/', https=True):
        """
        Initialize the instance with the host, the request URL and token
        """
        logging.Handler.__init__(self)
        self.gateway = gateway
        self.token = token
        self.host = host
        self.url = url
        self.method = 'POST'
        self.https = https
        if port == 0:
            if https:
                port = 443
            else:
                port = 80
        self.port = port

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
            if self.https:
                h = httplib.HTTPSConnection(host, port, timeout=10.0)
            else:
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
        except Exception as e:
            raise Exception("Problem creating DexcellServiceMessage " + e.message)

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


class DexcellRestApiError(Exception):
    def __init__(self, error_type, description, info):
        self.type = error_type
        self.description = description
        self.info = info

    def __str__(self):
        return repr("%s:%s(%s)" % (self.type, self.description, self.info))


class DexcellRestApiAuth(object):
    """
        Class for authentification in Dexcell
        software.
    """
    def __init__(self, endpoint, hash_dexma, secret, logger_name="dexcell_rest_api_auth"):
        self.endpoint = endpoint
        self.hash = hash_dexma
        self.secret = secret
        self.logger = logging.getLogger(logger_name)
        if len(self.logger.handlers) == 0:
            self.logger.setLevel(logging.INFO)
            self.handler = NullHandler()
            h_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            self.handler.setFormatter(logging.Formatter(h_format))
            self.logger.addHandler(self.handler)

    def _json_date_handler(self, obj):
        return obj.isoformat() if hasattr(obj, 'isoformat') else obj

    def _datetime_parser(self, dct):
        DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
        strp = datetime.strptime
        for k, v in dct.items():
            if isinstance(v, basestring) and re.search("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", v):
                try:
                    dct[k] = strp(v, DATE_FORMAT)
                except ValueError:
                    pass
        return dct

    def _call_rest(self, url):
        url = self.endpoint + url
        req = urllib2.Request(url)
        response = urllib2.urlopen(req)
        data = response.read()
        self.logger.info(data)
        return data

    def perm_token(self, temp_token):
        ''' obtain permanent token for oauth authentication'''
        url = "/oauth/accesstoken?temp_token=%s&secret=%s&idclient=%s" % (str(temp_token), self.secret, self.hash)
        response = self._call_rest(url)
        return response

    def set_key_value(self, key, value):
        "Set this key with this value in the key-value data store"
        url = self.endpoint + "/things/set/" + key
        req = urllib2.Request(url, json.dumps(value, default=self._json_date_handler),
                              headers={'x-dexcell-secret': self.secret})
        self.logger.info('storing key: %s with secret: %s' % (key, self.secret))
        response = urllib2.urlopen(req)
        data = response.read()
        return data

    def get_key(self, key):
        "Get this key from the key-value data store"
        url = "%s/things/get/%s" % (self.endpoint, key)
        req = urllib2.Request(url, headers={'x-dexcell-secret': self.secret})
        response = urllib2.urlopen(req)
        data = response.read()
        data = json.loads(data, object_hook=self._datetime_parser)
        result = json.loads(data['result'], object_hook=self._datetime_parser)
        return result


class DexcellRestApi(object):

    """
        class with all the utils API calls available group by
        from deployment calls, location calls and devide calls.
    """

    def __init__(self, endpoint, token, logger_name="dexcell_rest_api"):
        self.endpoint = endpoint
        self.token = token
        self.logger = logging.getLogger(logger_name)
        if len(self.logger.handlers) == 0:
            self.logger.setLevel(logging.INFO)
            self.handler = NullHandler()
            h_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            self.handler.setFormatter(logging.Formatter(h_format))
            self.logger.addHandler(self.handler)

    def _json_date_handler(self, obj):
        return obj.isoformat() if hasattr(obj, 'isoformat') else obj

    def _datetime_parser(self, dct):
        DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
        for k, v in dct.items():
            if isinstance(v, basestring) and re.search("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", v):
                try:
                    dct[k] = datetime.strptime(v, DATE_FORMAT)
                except:
                    pass
        return dct

    def dxdate(self, dt):
        """ convert datetime into default date string format used in dexcell api calls """
        return dt.strftime("%Y%m%d%H%M%S")

    def _call_rest(self, url, payload=None, parse_response=True):
        url = self.endpoint + url
        self.logger.info('url:%s token:%s' % (url, self.token))
        if payload is None:
            req = urllib2.Request(url, headers={'x-dexcell-token': self.token})
        else:
            req = urllib2.Request(url, payload, headers={'x-dexcell-token': self.token})
        try:
            response = urllib2.urlopen(req, timeout=600.0)
            data = response.read()
            if parse_response:
                return json.loads(data)
            else:
                return data
        except urllib2.HTTPError as httperror:

            info = json.loads(httperror.read())

            if httperror.code == 404:
                self.logger.error('error: not found')
                raise DexcellRestApiError('NOTFOUND', info['description'], info['moreInfo'])
            elif httperror.code == 401:
                self.logger.error('error: not authorized')
                raise DexcellRestApiError('INVALIDTOKEN', info['description'], info['moreInfo'])
            else:
                raise DexcellRestApiError('UNKNOWN', info['description'], info['moreInfo'])
                self.logger.error('error: %s' % (str(httperror.code)))

    def get_deployment(self, dep_id):
        """ return dict with basic information from deployment number dep_id"""
        url = "/deployments/%i.json" % dep_id
        deployment = self._call_rest(url)
        return deployment

    def get_deployment_locations(self, dep_id):
        """ return array with locations information from deployment number dep_id"""
        url = "/deployments/%i/locations.json" % dep_id
        location_list = self._call_rest(url)
        return location_list

    def get_deployment_devices(self, dep_id):
        """ return array with devices information from deployment number dep_id"""
        url = "/deployments/%i/devices.json" % dep_id
        device_list = self._call_rest(url)
        return device_list

    def get_deployment_parameters(self, dep_id):
        """ return array with parameters {freq, name, id, i18m, units}
            from deployment number dep_id
        """
        url = "/deployments/%i/parameters.json" % dep_id
        param_list = self._call_rest(url)
        return param_list

    def get_deployment_supplies(self, dep_id):
        """ return array with supplies {pod, name, id}
            from deployment number dep_id
        """
        url = "/deployments/%i/supplies.json" % dep_id
        supply_list = self._call_rest(url)
        return supply_list

    def get_deployment_notices(self, dep_id, start, end):
        """ return array with alerts information from deployment number dep_id
            from the interval selected
        """
        start = self.dxdate(start)
        end = self.dxdate(end)
        url = "/deployments/%i/notices.json?start=%s&end=%s" % (dep_id, start, end)
        notice_list = self._call_rest(url)
        return notice_list

    def get_deployment_parameter_devices(self, dep_id, param_nid):
        """ return array with parameters {id, name, networkid}
            from deployment number dep_id
        """
        url = "/deployments/%i/parameters/%s/devices.json" % (dep_id, str(param_nid))
        device_list = self._call_rest(url)
        return device_list

    def set_deployment_thing(self, dep_id, key, value):
        """ update dict of information saved by the user"""
        url = "/deployments/%i/things/set/%s.json" % (dep_id, key)
        payload = json.dumps(value, default=self._json_date_handler)
        data = self._call_rest(url, payload=payload, parse_response=False)
        return data

    def get_deployment_thing(self, dep_id, key):
        """ return dict of information saved by the user"""
        url = "/deployments/%i/things/get/%s.json" % (dep_id, key)
        data = self._call_rest(url, parse_response=False)
        self.logger.info('dep_thing:%s' % str(data))
        data = json.loads(data, object_hook=self._datetime_parser)
        return data

    def get_location(self, loc_id):
        """ return dict with basic information from locat"""
        url = "/locations/%i.json" % loc_id
        location = self._call_rest(url)
        return location

    def get_location_parameters(self, loc_id):
        """ return array with parameters {freq, name, id, i18m, units}
            from location number loc_id
        """
        url = "/locations/%i/parameters.json" % loc_id
        param_list = self._call_rest(url)
        return param_list

    def get_location_notices(self, loc_id, start, end):
        """ return array with alerts information for location number loc_id
            from the interval selected
        """
        start = self.dxdate(start)
        end = self.dxdate(end)
        url = "/locations/%i/notices.json?start=%s&end=%s" % (loc_id, start, end)
        notice_list = self._call_rest(url)
        return notice_list

    def get_location_comments(self, loc_id, start, end):
        """ return array with comments information for location number loc_id
            from the interval selected
        """
        start = self.dxdate(start)
        end = self.dxdate(end)
        url = "/locations/%i/comments.json?start=%s&end=%s" % (loc_id, start, end)
        comments = self._call_rest(url)
        return comments

    def get_location_parameter_devices(self, loc_id, param_nid):
        """ return array with parameters {id, name, networkid}
            from location number loc_id
        """
        url = "/locations/%i/parameters/%i/devices.json" % (loc_id, param_nid)
        device_list = self._call_rest(url)
        return device_list

    def get_location_supplies(self, loc_id):
        """ return array with supplies {pod, name, id}
            from location number loc_id
        """
        url = "/locations/%i/supplies.json" % loc_id
        supply_list = self._call_rest(url)
        return supply_list

    def get_location_devices(self, loc_id):
        """ return array with the devices from the location """
        url = "/locations/%i/devices.json" % loc_id
        device_list = self._call_rest(url)
        return device_list

    def get_device(self, dev_id):
        """ return dict with information for the device """
        url = "/devices/%i.json" % dev_id
        device = self._call_rest(url)
        return device

    def get_device_parameters(self, dev_id):
        """ return array with parameters {freq, name, id, i18m, units}
            from device number dev_id
        """
        param_list = self._call_rest("/devices/" + str(dev_id) + "/parameters.json")
        return param_list

    def get_simulated_bill(self, dev_id, start, end, type_param="ELECTRICAL", parameters="AAANNN", pod=None, time='HOUR'):
        ''' returns bill generated from data in dexcell
            Parameters
                A : RAW + SUMMARY
                R: RAW set of datas returned by time mesure, default hour
                S: SUMMARY resum of data: totals, periods...
                N: nothing
            type_param can be ELECTRICAL, WATER, GAS
        '''
        new_pod = ''
        if pod is not None:
            new_pod = "&pod=" + pod
        start = start.strftime("%Y%m%d%H%M%S")
        end = end.strftime("%Y%m%d%H%M%S")
        url = ["/cost/%i/%s.json?start=%s" % (dev_id, type_param, start)]
        url.append("&end=%s&applyPattern=%s&period=%s%s" % (end, parameters, time, new_pod))
        url = "".join(url)
        bill = self._call_rest(url)
        return bill

    def get_supply_bills(self, sup_id, start, end, type_param='ELECTRICAL', parameters="AAANNN", pod=None, time='HOUR'):
        ''' returns bills updated by the customer
            Parameters
                A : RAW + SUMMARY
                R: RAW set of datas returned by time mesure, default hour
                S: SUMMARY resum of data: totals, periods...
                N: nothing
            type_param can be ELECTRICAL, WATER, GAS
        '''
        new_pod = ''
        if pod is not None:
            new_pod = "&pod=" + pod
        start = start.strftime("%Y%m%d%H%M%S")
        end = end.strftime("%Y%m%d%H%M%S")
        url = ["/cost/%i/bills/%s.json?start=%s" % (sup_id, type_param, start)]
        url.append("&end=%s&applyPattern=%s&period=%s%s" % (end, parameters, time, new_pod))
        url = "".join(url)
        bills = self._call_rest(url)
        return bills

    def get_session(self, session_id):
        """ return the session for an app with a concret session_id"""
        url = "/session/%s.json" % session_id
        response = self._call_rest(url)
        self.logger.info('get session: ' + str(response))
        return response

    def get_readings(self, dev_id, s_nid, start, end):
        """ return array dict with {values, timestamp} """
        start = self.dxdate(start)
        end = self.dxdate(end)
        url = "/devices/%i/%i/readings.json?start=%s&end=%s" % (dev_id, s_nid, start, end)
        readings = self._call_rest(url)
        for i in range(0, len(readings)):
            try:
                readings[i]['ts'] = datetime.strptime(readings[i]['ts'], "%Y-%m-%d %H:%M:%S")
                readings[i]['tsutc'] = datetime.strptime(readings[i]['tsutc'], "%Y-%m-%d %H:%M:%S")
            except KeyError:
                pass
        return readings

    def get_readings_new(self, dev_id, param, frequency, operation, start, end):
        """ returns array of dict of values from the device dev_id with
            parameter param with a frequency in the interval start - end.
        """
        start = self.dxdate(start)
        end = self.dxdate(end)
        url = ["/devices/%i/%s/readings.json?" % (dev_id, str(param))]
        url.append("start=%s&end=%s&frequency=%s&operation=%s" % (start, end, str(frequency), str(operation)))
        url = "".join(url)
        readings = self._call_rest(url)
        for i in range(0, len(readings)):
            try:
                readings[i]['ts'] = datetime.strptime(readings[i]['ts'], "%Y-%m-%d %H:%M:%S")
                readings[i]['tsutc'] = datetime.strptime(readings[i]['tsutc'], "%Y-%m-%d %H:%M:%S")
            except KeyError:
                pass
        return readings

    def get_cost(self, nid, start, end, energy_type='ELECTRICAL', period='HOUR', grouped=False):
        """ return array from cost and consumption with timestamp"""
        str_grouped = 'TRUE'
        if not grouped:
            str_grouped = 'FALSE'
        start = self.dxdate(start)
        end = self.dxdate(end)
        url = ["/devices/%i/%s/cost.json?" % (nid, energy_type)]
        url.append("start=%s&end=%s&period=%s&grouped=%s" % (start, end, str(period), str_grouped))
        url = "".join(url)
        raw_response = self._call_rest(url)
        try:
            readings = raw_response['readings']
            for i in range(0, len(readings)):
                readings[i]['ts'] = datetime.strptime(readings[i]['ts'], "%Y/%m/%d %H:%M:%S")
            periods = raw_response['periods']
            return readings, periods
        except KeyError:
            return []

