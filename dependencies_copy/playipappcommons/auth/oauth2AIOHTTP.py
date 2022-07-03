import json
import traceback
import uuid
from datetime import datetime

import aiohttp
import jwt
from aiohttp import ClientResponse, ClientSession, web
from aiohttp.web_request import BaseRequest
from dynaconf import settings
from multidict import CIMultiDict

from playipappcommons.auth.oauth2 import get_oauth2_login_url_common


async def oauth2_callback_aiohttp(secret, request: BaseRequest):
    host = request.host
    if "X-Forwarded-Ssl" in request.headers and request.headers["X-Forwarded-Ssl"] == 'on':
        protocol = "https"
    else:
        protocol = request.scheme
    cid = "playipappserver"

    callback = protocol + "://" + host + "/playipapp/oauth2Callback"

    state = request.rel_url.query['state']
    # session_state = request.rel_url.query['session_state']
    saved_state = request.cookies["playipapp_oauth_state"]
    code = request.rel_url.query['code']

    # OAUTH2_TOKEN_URL = "https://127.0.0.1/auth/realms/playipintern/protocol/openid-connect/token"
    # OAUTH2_USERINFO_URL = "https://127.0.0.1/auth/realms/playipintern/protocol/openid-connect/userinfo"

    form = aiohttp.FormData(
        {"grant_type": "authorization_code", "code": code, "client_id": cid, "redirect_uri": callback,
         "scope": "openid"})

    resp: ClientResponse = None
    try:
        # if state != saved_state:
        #    raise Exception("State "+state+" != "+saved_state)
        openClientSession: ClientSession = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=(settings.TEST == 0)))
        resp: ClientResponse = await openClientSession.post(settings.OAUTH2_TOKEN_URL, data=form())
        body = await resp.read()
        jsonToken = json.loads(body)
        # print("json token ____________________________________")
        # print(jsonToken)
        # print("json token end ____________________________________")
        key = "Bearer " + jsonToken["access_token"]
        aheaders = CIMultiDict()
        aheaders.add("Authorization", key)
        userInfo: ClientResponse = await openClientSession.get(settings.OAUTH2_USERINFO_URL, headers=aheaders)
        body = await userInfo.read()
        # para obter os Roles do usuário
        # Criar o Role
        #   No momento uso o Role playip_backup
        # Em Client Scopes/ Roles / Mappers / Real Roles / Token claim Name
        #    mudar o valor de "realm_access.roles" para "roles"
        #    clicar em "Add to userinfo"

        jsonUser = json.loads(body)
        # print("json user ____________________________________")
        # print(jsonUser)
        # print("json user end ____________________________________")

        await openClientSession.close()
    except:
        traceback.print_exc()
        res = web.Response(content_type="text/plain", status=403, text="autorizaçao falhou")
        return res
    finally:
        if resp:
            resp.close()

    sessionId = str(uuid.uuid1())
    do_backup = "roles" in jsonUser and "playip_backup" in jsonUser["roles"]
    encoded_jwt = jwt.encode(
        {'session': sessionId, 'user': jsonUser["preferred_username"], 'playipappbackup': do_backup, 'playipappinfra': do_backup, 'playipappanalytics': do_backup,
         "timeini": str(datetime.now().timestamp()), "id_token": jsonToken["id_token"]}, secret, algorithm='HS256')
    if isinstance(encoded_jwt, str):
        encoded_jwt_str = encoded_jwt
    else:
        encoded_jwt_str = encoded_jwt.decode(
            "utf-8")  # em algumas versoes, em vez de string, jwt.encode retorna bytes. nesse caso, trandforma em string
    # print("##  ", encoded_jwt_str)

    response = aiohttp.web.HTTPFound("/playipapp/list")  # http://127.0.0.1:8008/playipapp/list
    # response.cookies['playipappsession'] = encoded_jwt_str
    response.set_cookie("playipappsession", encoded_jwt_str, path="/")
    response.set_cookie('playipappbackup', do_backup, path="/")

    raise response


async def logout_aiohttp(secret:str, request: BaseRequest):
    try:
        host = request.host
        if "X-Forwarded-Ssl" in request.headers and request.headers["X-Forwarded-Ssl"] == 'on':
            protocol = "https"
        else:
            protocol = request.scheme

        callback = protocol + "://" + host + "/playipapp/"

        try:
            session_str = request.cookies["playipappsession"]
            # print("**  ", session_str)
            session_bytes = session_str.encode("utf-8")
            # print("b  ", session_bytes)
            session_data = jwt.decode(session_str, secret, algorithms='HS256')
        except:
            res = aiohttp.web.HTTPFound(callback)  # http://127.0.0.1:8008/playipapp/list
            # res = web.Response(content_type="text/plain", status=200, text="Logout realizado")
            res.del_cookie("playipapp_oauth_state", path="/")
            res.del_cookie("playipappsession", path="/")
            await res.prepare(request)
            return res

        id_token = session_data["id_token"]
        # print("id_token")
        # print(id_token)

        url = "{burl}?id_token_hint={token}&post_logout_redirect_uri={callback}" \
             .format(burl=settings.OAUTH2_LOGOUT_URL, token=id_token, callback=callback)

        res = aiohttp.web.HTTPFound(url)  # http://127.0.0.1:8008/playipapp/list
        #res = web.Response(content_type="text/plain", status=200, text="Logout realizado")
        res.del_cookie("playipapp_oauth_state", path="/")
        res.del_cookie("playipappsession", path="/")
        await res.prepare(request)

        return res
    except Exception as ex:
        traceback.print_exc()
        raise ex


def get_oauth2_login_url_aiohttp(request: BaseRequest):
      host = request.host
      headers = request.headers
      scheme = request.url.scheme
      print(request.headers)
      url, state = get_oauth2_login_url_common(host, scheme, headers)
      res = web.HTTPFound(url)
      #res.cookies["playipapp_oauth_state"] = state
      res.set_cookie("playipapp_oauth_state", state, path="/")
      #print("state "+state)
      return res
