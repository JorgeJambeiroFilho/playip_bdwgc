import traceback

import motor.motor_asyncio
import pymongo
from dynaconf import settings
from pymongo import WriteConcern

playIPChatMongoClient: motor.motor_asyncio.AsyncIOMotorClient = None
playIPChatHelperDB = None
wc_majority = None

if settings.USEDOCKER:
    print("Executando com a configuração do docker")
else:
    print("Executando com a configuração de depuração")

def createIndex_ChatHistory_TimeId(mdb):
    # try:
    #     mdb.ChatHistory.drop_index("time_id")
    # except:
    #     pass
    mdb.ChatHistory.create_index(
        [
            ("appId", pymongo.ASCENDING),
            ("timeStamp", pymongo.ASCENDING),
            ("_id", pymongo.ASCENDING)
        ],
        background=False, name="time_id_2"
    )
    print("Created ChatHistory time_id if necessary")


def createIndex_Infra_Full_Address(mdb):
    mdb.infra.create_index(
        [
            ("addressFullNames", pymongo.ASCENDING)
        ],
        background=False, name="address_full_name_index"
    )
    print("Created infra address_full_name_index if necessary")


def createIndex_Infra_WordPair(mdb):
    mdb.infra.create_index(
        [
            ("filters.indexedWordPair", pymongo.ASCENDING)
        ],
        background=False, name="wordpair_index"
    )
    print("Created infra word_pair_index if necessary")

def createIndex_UserClients_TimeId(mdb):
    # try:
    #     mdb.UserClients.drop_index("time_id")
    # except:
    #     pass

    mdb.UserClients.create_index(
        [
            ("cpfCnpj", pymongo.ASCENDING),
            ("timeStamp", pymongo.DESCENDING),
            ("idContrato", pymongo.ASCENDING),
            ("appId", pymongo.ASCENDING),
        ],
        background=False, name="client_time_id"
    )
    mdb.UserClients.create_index(
        [
            ("appId", pymongo.ASCENDING),
            ("timeStamp", pymongo.DESCENDING),
        ],
        background=False, name="app_id"
    )
    print("Created UserClients indexes if necessary")

def createIndex_analytics(mdb):
    mdb.UserClients.create_index(
        [
            ("context", pymongo.ASCENDING),
            ("event_type", pymongo.DESCENDING),
            ("metric_name", pymongo.ASCENDING),
            ("period_group", pymongo.ASCENDING),
        ],
        background=False, name="context_eventtype_metricname_periodgroup"
    )
    print("Created Analytics indexes if necessary")


def getBotMongoDB():
    global playIPChatHelperDB
    if not playIPChatHelperDB:
        playIPChatHelperDB = getMongoClient().PlayIPChatHelper
        print("Create Indexes")
        createIndex_Infra_WordPair(playIPChatHelperDB)
        createIndex_Infra_Full_Address(playIPChatHelperDB)
        createIndex_ChatHistory_TimeId(playIPChatHelperDB)
        createIndex_UserClients_TimeId(playIPChatHelperDB)
        createIndex_analytics(playIPChatHelperDB)
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