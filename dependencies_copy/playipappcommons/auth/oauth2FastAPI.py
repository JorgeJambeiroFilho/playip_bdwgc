import aiohttp
from aiohttp import ClientSession, ClientResponse
from dynaconf import settings
from fastapi import APIRouter, HTTPException, Security
from fastapi.security import APIKeyQuery, APIKeyHeader, APIKeyCookie
from starlette.datastructures import Headers
from starlette.requests import Request
from starlette.responses import RedirectResponse, Response, PlainTextResponse
import uuid
#oauthRouter = APIRouter(prefix=getApiRoot()+"/oauth")
from playipappcommons.auth.oauth2 import get_oauth2_login_url_common, checkSessionCommon

#secret = str(uuid.uuid1())


def permission(required_permission: str = None):
    def Inner(func):
        def wrapper(*args, **kwargs):
            #print("wrapper ",required_permission)
            request = kwargs["request"]
            if not checkSession(request, required_permission):
                res = PlainTextResponse("faltou permissão: "+required_permission, status_code=403)
                return res
            else:
                return func(*args, **kwargs)
        return wrapper
    return Inner


async def defaultpermissiondep(request: Request):
    return await permissiondep(request, required_permission=None)

async def backuppermissiondep(request: Request):
    return await permissiondep(request, required_permission="playipappbackup")

async def infrapermissiondep(request: Request):
    return await permissiondep(request, required_permission="playipappinfra")

async def analyticspermissiondep(request: Request):
    return await permissiondep(request, required_permission="playipappanalytics")




async def permissiondep(request: Request, required_permission: str = None):
    if not await checkSession(request, required_permission):
        raise HTTPException(403, "Missing permission: "+str(required_permission))


async def getSecret():
    openClientSession: ClientSession = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=(settings.TEST == 0)))
    resp: ClientResponse = await openClientSession.get(settings.PLAYIPAPP_URL + "/playipappinternal/getSecret")
    secret = await resp.text()
    await openClientSession.close()
    return secret


def get_oauth2_login_url_intern(request: Request):
    host = request.headers["host"]
    headers = request.headers
    scheme = request.url.scheme
    #print(request.headers)
    url, state = get_oauth2_login_url_common(host, scheme, headers)
    response = RedirectResponse(url=url)
    response.set_cookie(key="playipapp_oauth_state", value=state, path="/")
    #print("state " + state)
    return response


# api_key_query = APIKeyQuery(name="APISecret", auto_error=False)
# api_key_header = APIKeyHeader(name="APISecret", auto_error=False)
# api_key_cookie = APIKeyCookie(name="APISecret", auto_error=False)

async def checkSession(request: Request, permission=None
    # api_key_query: str = Security(api_key_query),
    # api_key_header: str = Security(api_key_header),
    # api_key_cookie: str = Security(api_key_cookie),
):
    secret = await getSecret()
    # if api_key_query == secret or api_key_header == secret or api_key_cookie == secret:
    #     return True
    return checkSessionCommon(request.cookies, secret, permission)
