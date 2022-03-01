import traceback

import motor.motor_asyncio
from dynaconf import settings
from pymongo import WriteConcern

playIPChatMongoClient: motor.motor_asyncio.AsyncIOMotorClient = None
playIPChatHelperDB = None
wc_majority = None

if settings.USEDOCKER:
    print("Executando com a configuração do docker")
else:
    print("Executando com a configuração de depuração")



def getBotMongoDB():
    global playIPChatHelperDB
    if not playIPChatHelperDB:
        playIPChatHelperDB = getMongoClient().PlayIPChatHelper
    return playIPChatHelperDB

def closeBotMongoDb():
    if playIPChatMongoClient is not None:
        playIPChatMongoClient.close()

def getMongoClient():
    global playIPChatMongoClient, wc_majority

    if playIPChatMongoClient is None:
        try:
            playIPChatMongoClient = motor.motor_asyncio.AsyncIOMotorClient(settings.MONGO_DB_HOST, settings.MONGO_DB_PORT) #, username=settings.MONGO_DB_USER, password=settings.MONGO_DB_PASSWORD
            wc_majority = WriteConcern("majority", wtimeout=1000)

        except:
            traceback.print_exc()

    return playIPChatMongoClient