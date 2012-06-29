#!/usr/bin/python
#coding: utf-8
#Copyright (c) 2012, Dexma Sensors S.L. (info@dexmatech.com)
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
#THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
#ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
#WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
#DISCLAIMED. IN NO EVENT SHALL Dexma Sensors S.L. BE LIABLE FOR ANY
#DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
#(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
#LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
#ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
#(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
#SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import time
import datetime
import httplib
import json
import logging
from logging.handlers import RotatingFileHandler 
    
class DexcellServiceMessage(object):

    def __init__(self,node,service,timestamp,value,seq):
        try: 
            self.node = str(node)
            self.service = int(service)
            self.timestamp = timestamp
            self.value = float(value)
            self.seqnum = int(seq)
        except Exception as ex:
            print ex;
            raise Exception("Problem creating DexcellServiceMessage")
        
    def __repr__(self):
        outPut = "DexcellServiceMessage(node="
        outPut += str(self.node)
        outPut += ", service="+  str(self.service)
        outPut += ", timestamp="+ str(self.timestamp)
        outPut += ", value=" + str(self.value)
        outPut += ", seqnum=" + str(self.seqnum)
        outPut += ")"
        return outPut



class DexcellSender(object):
    
    DEFAULT_SERVER = 'insert.dexcell.com'
    DEFAULT_URL = '/insert-json.htm'
    DEFAULT_LOGFILE = '/var/log/dexma/DexcellSender.log'
    DEFAULT_LOGLEVEL = logging.INFO
    DEFAULT_GATEWAY = 'None'
    DEFAULT_LOGGERNAME = 'DexcellSender'
    
    def __init__(self, gateway=DEFAULT_GATEWAY, loggerName=DEFAULT_LOGGERNAME, logfile=DEFAULT_LOGFILE, loglevel=DEFAULT_LOGLEVEL, server=DEFAULT_SERVER, url=DEFAULT_URL):
        self.__server = server
        self.__url = url
        self.__gateway = gateway
        self.__logger = logging.getLogger(loggerName)
        if len(self.__logger.handlers) == 0:
            self.__logger.setLevel(loglevel)
            self.handler = RotatingFileHandler(logfile, maxBytes=204800, backupCount=10)
            self.handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
            self.__logger.addHandler(self.handler)   
        

        
    def setup(self, gateway=DEFAULT_GATEWAY, loggerName=DEFAULT_LOGGERNAME, logfile=DEFAULT_LOGFILE, loglevel=DEFAULT_LOGLEVEL, server=DEFAULT_SERVER, url=DEFAULT_URL):
        """Setup the Dexcell Sender Object
        
        """
        self.__server = server
        self.__url = url
        self.__gateway = gateway
        self.__logger = logging.getLogger(loggerName)
        if len(self.__logger.handlers) == 0:
            self.__logger.setLevel(loglevel)
            self.handler = RotatingFileHandler(logfile, maxBytes=204800, backupCount=10)
            self.handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
            self.__logger.addHandler(self.handler)
            
    def changeGateway(self, gateway):
        """Change the gateway mac that will be sent
        
        """
        self.__gateway = gateway
        

    def __insertRawJSONData(self, data):
        """Insert the raw data string to the server
        
        """
        params = 'data='+data
        headers = {"Content-type": "application/x-www-form-urlencoded","Accept": "text/plain"}
        conn = httplib.HTTPConnection(self.__server)        
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
                    return ( -1, 'FAIL' )
                
        self.__logger.debug('[ LOG '+self.__gateway+' ]['+ str(datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))+'] status: '+ str(response.status)+ ' result'+ response.getheader('data'))
        return response.status, response.getheader('data')

    def insertDexcellServiceMessage(self,serviceMessage,timezone='UTC'):
        '''Insert a single DexcellServiceMessage
        
        '''
        reading = {                            
                'nodeNetworkId': str(serviceMessage.node),
                'serviceNetworkId': int(serviceMessage.service), 
                'value': float(serviceMessage.value),
                'seqNum': int(serviceMessage.seqnum),
                'timeStamp': time.strftime("%Y-%m-%dT%H:%M:%S.000 " + timezone, serviceMessage.timestamp)
            }
        data = {
                'gatewayId': self.__gateway,
                'service': [ reading ]
                }
        result = self.__insertRawJSONData(json.dumps(data))
        return result


    def insertDexcellServiceMessages(self, serviceMessageIterator, timezone='UTC'):
        """ Insert many DexcellServiceMessages at once
        
        """
        readings = []
        for serviceMessage in serviceMessageIterator:                
            reading = {                            
                            'nodeNetworkId': str(serviceMessage.node),
                            'serviceNetworkId': int(serviceMessage.service), 
                            'value': float(serviceMessage.value),
                            'seqNum': int(serviceMessage.seqnum),
                            'timeStamp': time.strftime("%Y-%m-%dT%H:%M:%S.000 " + timezone, serviceMessage.timestamp)
                        }
            readings.append(reading)
        data = {
                'gatewayId': self.__gateway,
                'service': readings
                }
        result = self.__insertRawJSONData(json.dumps(data))
        return result
