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
	import time
	from dexma.dexcell import DexcellSender, DexcellServiceMessage
	
	message = DexcellServiceMessage(node=1, service=401, timestamp=time.gmtime(), value=1001.23, seq=1)
	sender = DexcellSender(gateway='yourgateway')
	result =  sender.insertDexcellServiceMessage(message)
	if result == (200, 'OK'):
		print "Insert OK"
	else:
		print "Insert failed"


============================================================
Installing
============================================================

	git clone git://github.com/dexma/dexcell-python.git
	cd dexcell-python
	python setup.py install

============================================================
License
============================================================

dexcell-python has been developed by Dexma Sensors S.L.

The software is released under the BSD License

============================================================
Contact
============================================================

You may address your questions at support@dexmatech.com 
