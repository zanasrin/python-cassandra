---
services: cosmos-db
platforms: python
author: zanasrin
---

# Accessing Cassandra on Cosmos Db using Python
Azure Cosmos DB is Microsoft’s globally distributed multi-model database service. You can quickly create and query document, key/value, and graph databases, all of which benefit from the global distribution and horizontal scale capabilities at the core of Azure Cosmos DB.

This quickstart demonstrates how to write python samples and connect it to your Azure Cosmos DB database, which supports Cassandra client connections. In other words, your python program only knows that it's connecting to a database using Cassandra APIs. It is transparent to the application that the data is stored in Azure Cosmos DB.

## Running this sample
* Before you can run this sample, you must have the following perquisites:
	* An active Azure DocumentDB account - If you don't have an account, refer to the [Create a DocumentDB account](https://azure.microsoft.com/en-us/documentation/articles/documentdb-create-account/) article.
	* [Python 2.7].
	* [Git](http://git-scm.com/).


1. Clone this repository using `https://github.com/zanasrin/python-cassandra.git`

2. Go to PythonSamples folder.

3. Next, substitute the endpoint and authorization key in `config.py` with your Cosmos DB account's values.

	```
	username = "~your DocumentDB endpoint here~";
	password = "~your auth key here~";
	```

4. Run 
   ```
   pip install Cassandra-driver 
   pip install prettytable
   pip install requests
   pip install pyopenssl
   ```
   in a terminal to install required python packages
 
5. Run `python FileName.py` in a terminal to execute it.

6. If you have a self signed certificate with which you want to verify the server certificate while connecting to the cluster using SSL, add the path of the cert in the config file `config.py` with the keyname as `selfsigned_cert`

## About the code
The code included in this sample is intended to get you quickly started with a python program that connects to Azure Cosmos DB with the Cassandra API.

## More information

- [Azure Cosmos DB](https://docs.microsoft.com/azure/cosmos-db/introduction)
- [Azure DocumentDB Python SDK](https://docs.microsoft.com/en-us/azure/cosmos-db/documentdb-sdk-python)

