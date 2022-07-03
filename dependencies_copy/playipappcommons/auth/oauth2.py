import traceback
import uuid
from typing import Union

from dynaconf import settings
import jwt
from datetime import datetime



def get_oauth2_login_url_common(host: str, scheme:str, headers):
    if "X-Forwarded-Ssl" in headers and headers["X-Forwarded-Ssl"] == 'on':
        protocol = "https"
    else:
        protocol = scheme

    state = str(uuid.uuid1())
    nonce = 456
    cid = "playipappserver"

    callback = protocol + "://" + host + "/playipapp/oauth2Callback"

    url = "{burl}?client_id={cid}&response_type=code&state={state}&nonce={nonce}&redirect_uri={callback}&scope=openid" \
        .format(burl=settings.OAUTH2_AUTHORIZATION_URL, nonce=nonce, state=state, cid=cid, callback=callback)

    return url, state


def checkSessionCommon(cookies, secret, permission=None):
    if settings.NO_OAUTH:
        return True
    print("secret", "<<" + secret + ">>")
    print("cookies",cookies)
    if "APIInternalSecret" in cookies and cookies["APIInternalSecret"] == secret:
        return True
    if "playipappsession" not in cookies:
        return False
    session_str = cookies["playipappsession"]

    #print("SESSION STRING")
    #print(session_str)
    try:
        session_data = jwt.decode(session_str, secret, algorithms='HS256')
        #print("required permission ", permission)
        #print("session_data", session_data)
        if "timeini" not in session_data:
            return False
        if permission is not None:
            if permission not in session_data or not session_data[permission]:
                return False
        timen = datetime.now().timestamp()
        timeini = float(session_data["timeini"])
        if timen - timeini > 12 * 60 * 60:
            return False
        return True
    except jwt.exceptions.InvalidTokenError as ex:
        traceback.print_exc()
        return False
    except Exception as ex:
        traceback.print_exc()
        raise ex


# async def checkSession(request: Request):
#     if settings.NO_OAUTH:
#         return True
#     if "playipchatsession" not in request.cookies:
#         return False
#     session_str = request.cookies["playipchatsession"]
#     session_bytes = session_str.encode("utf-8")
#     try:
#         session_data = jwt.decode(session_bytes, secret, algorithms='HS256')
#         if "timeini" not in session_data:
#             return False
#         timen = datetime.now().timestamp()
#         timeini = float(session_data["timeini"])
#         if timen - timeini > 12 * 60 * 60:
#             return False
#         return True
#     except jwt.exceptions.InvalidTokenError as ex:
#         return False

# @oauthRouter.get("/oauth2Callback")
# async def oauth2_callback(request: Request):
#     host = request.headers["host"]
#     if "X-Forwarded-Ssl" in request.headers and request.headers["X-Forwarded-Ssl"] == 'on':
#         protocol = "https"
#     else:
#         protocol = request.scheme
#     cid = "playipchat"
#
#     callback = protocol + "://" + host + "/playipchathelper/oauth/oauth2Callback"
#
#     state = request.query_params['state']
#     # session_state = request.rel_url.query['session_state']
#     saved_state = request.cookies["playipchat_oauth_state"]
#     code = request.query_params['code']
#
#     form = aiohttp.FormData(
#         {"grant_type": "authorization_code", "code": code, "client_id": cid, "redirect_uri": callback})
#
#     resp: ClientResponse = None
#     try:
#         if state != saved_state:
#             raise Exception("State " + state + " != " + saved_state)
#         openClientSession: ClientSession = aiohttp.ClientSession(
#             connector=aiohttp.TCPConnector(ssl=(settings.TEST == 0)))
#         resp: ClientResponse = await openClientSession.post(settings.OAUTH2_TOKEN_URL, data=form())
#         body = await resp.read()
#         print("Corpo da resposta HTTP para obter token")
#         print(body)
#         jsonToken = json.loads(body)
#         key = "Bearer " + jsonToken["access_token"]
#         aheaders = CIMultiDict()
#         aheaders.add("Authorization", key)
#         userInfo: ClientResponse = await openClientSession.get(settings.OAUTH2_USERINFO_URL, headers=aheaders)
#         body = await userInfo.read()
#         jsonUser = json.loads(body)
#
#     except:
#         traceback.print_exc()
#         res = PlainTextResponse("autorizaçao falhou", status_code=403)
#         return res
#     finally:
#         if resp:
#             resp.close()
#
#     sessionId = str(uuid.uuid1())
#     encoded_jwt = jwt.encode(
#         {'session': sessionId, 'user': jsonUser["preferred_username"], "timeini": str(datetime.now().timestamp())},
#         secret, algorithm='HS256')
#
#     if isinstance(encoded_jwt, str):
#         encoded_jwt_str = encoded_jwt
#     else:
#         encoded_jwt_str = encoded_jwt.decode("utf-8")
#
#     response = RedirectResponse("/playipchathelper")
#     response.set_cookie("playipchatsession", encoded_jwt_str, path="/")
#
#     return response



