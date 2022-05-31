from dynaconf import settings
from fastapi import Security
from fastapi.security import APIKeyQuery, APIKeyHeader, APIKeyCookie
from starlette.requests import Request

from playipappcommons.auth.oauth2FastAPI import checkSession

API_KEY = settings.URA_INTEGRATION_KEY
API_KEY_NAME = "access-token"
#COOKIE_DOMAIN = "localtest.me"

api_key_query = APIKeyQuery(name=API_KEY_NAME, auto_error=False)
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)
api_key_cookie = APIKeyCookie(name=API_KEY_NAME, auto_error=False)


async def get_api_key(
    request: Request,
    api_key_query: str = Security(api_key_query),
    api_key_header: str = Security(api_key_header),
    api_key_cookie: str = Security(api_key_cookie),

):
    print (api_key_header, "    -----     ", API_KEY)
    #return "xxx"
    if api_key_query == API_KEY:
        return "api_key_query"
    elif api_key_header == API_KEY:
        return "api_key_header"
    elif api_key_cookie == API_KEY:
        return "api_key_cookie"
    else:
        if await checkSession(request):
            return "oauth2"
        else:
            return ""
        # raise HTTPException(
        #     status_code=HTTP_403_FORBIDDEN, detail="Could not validate credentials"
        # )
