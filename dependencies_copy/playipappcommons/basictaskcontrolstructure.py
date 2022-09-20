import time

import pydantic

from playipappcommons.playipchatmongo import getMongoClient


class BasicTaskControlStructure(pydantic.BaseModel):
    key: str
    fail: bool = False
    complete: bool = False
    started: bool = False
    justStarted: bool = False # nunca é salvo como True (veja save()). Só é True quando acaba de dar o start. Veja getControlStructure().
    message: str = "ok"
    aborted: bool = False
    num_processed: int = 0
    num_fails: int = 0
    last_action: float = 0
    max_inactive_time: float = 60000 # cada processo deve colocar um número adequado aqui
                                     # um processo nunca pode ficar maix que max_inactive_time milisegundos sem
                                     # atualizar last_action ou outras instâncias ou outras chamadas a mesma instância
                                     # concluirão que ele caiu

    def isGoingOn(self):
        return self.started and not self.complete and time.time() - self.last_action <= self.max_inactive_time

    def isSuspended(self):
        return self.started and not self.complete and time.time() - self.last_action > self.max_inactive_time

    def act(self):
        self.last_action = time.time()

    def done(self):
        self.complete = True
        self.started = False

    def start(self):
        self.complete = False
        self.started = True
        self.justStarted = True
        self.aborted = False
        self.act()

    def hasJustStarted(self) -> bool:
        return self.justStarted
    async def save(self, mdb):
        resDict = self.dict(by_alias=True)
        resDict["justStarted"] = False
        self.act()
        await mdb.control.replace_one({"key": self.key}, resDict, upsert=True)

    def isAborted(self):
        return self.aborted

    def abort(self):
        self.aborted = True

    def clear(self):
        if self.isGoingOn():
            res = self.copy()
            res.message = "CannotClearRunningProcess"
            return res
        else:
            self.fail: bool = False
            self.complete: bool = False
            self.started: bool = False
            self.message: str = "ok"
            self.aborted: bool = False
            self.num_processed: int = 0
            self.num_fails: int = 0
            self.last_action: float = 0


taskControlFactories = {}


def registerTaskControlStructureFactory(key: str, creatorFunction):
    global taskControlFactories
    taskControlFactories[key] = creatorFunction


def createTaskControlStructure(json) -> BasicTaskControlStructure:
    global taskControlFactories
    return taskControlFactories[json["key"]](json)


async def getControlStructureCallback(session, key:str, begin:bool) -> BasicTaskControlStructure:

    mdb = session.client.PlayIPChatHelper

    resDict = await mdb.control.find_one({"key":key})
    if resDict is None:
        res = createTaskControlStructure({"key":key})
    else:
        res: BasicTaskControlStructure = createTaskControlStructure(resDict)
    if not res.started and begin:
        res.start()
        await res.save(mdb)

    return res

async def getControlStructure(mdb, key:str, begin:bool) -> BasicTaskControlStructure:

    mdbcli = getMongoClient()

    async with await mdbcli.start_session() as session:
        return await session.with_transaction(lambda s: getControlStructureCallback(s, key, begin))



