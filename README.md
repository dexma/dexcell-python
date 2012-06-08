dexcell-python
==============

DEXMA DEXCell Energy Manager python API

============================================================
Summary
============================================================

dexcell-python is a python implementation of the DEXCell Energy
Manager insertion API.

============================================================
Features
============================================================

* insert single message
* insert multiple messages

============================================================
Example Code
============================================================

	from dexma.dexcell import DexcellSender, DexcellServiceMessage
	
	message = DexcellServiceMessage(node=1,service=401,timestamp=time.localtime(),value=1001.23,seq=1)
	sender = DexcellSender(gateway='yourgateway')
	print server.insertDexcellServiceMessage(message)


============================================================
Installing
============================================================

	git clone git://github.com/jsoucheiron/dexcell-python.git
	cd dexcell-python
	python setup.py install

============================================================
License
============================================================

dexcell-python is built on top of code developed from:
	* Dexma Sensors S.L.

The software is released under the BSD License

============================================================
Contact
============================================================

You may address your questions at support@dexmatech.com 
