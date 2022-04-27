from playipappcommons.infra.endereco import Endereco
from playipappcommons.infra.infraimportmethods import findAddress
from playipappcommons.infra.inframethods import isAddressInFailIntern, getInfraElementFailState
from playipappcommons.infra.inframodels import AddressInFail, InfraElement, AddressQuery
from playipappcommons.playipchatmongo import getBotMongoDB


async def isAddressInFail(addressQuery: AddressQuery) -> AddressInFail:
    if not addressQuery.endereco:
        return await isAddressInFailIntern(addressQuery)
    else:
        endereco = addressQuery.endereco.copy(deep=True)
        endereco.prefix = "Infraestrutura-"+addressQuery.medianetwork
        print("isAddressInFail "+str(endereco))
        return await isInFail(endereco)

async def isInFail(endereco: Endereco) -> AddressInFail:
    mdb = getBotMongoDB()
    infraElement: InfraElement = await findAddress(mdb, endereco)
    if infraElement is None:
        print("isAddressInFail not found " + str(endereco))
        return AddressInFail(located=False, inFail= False)
    else:
        print("isAddressInFail found " + str(endereco))
        res: AddressInFail = await getInfraElementFailState(infraElement.id)
        return res


