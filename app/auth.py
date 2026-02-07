import os
import secrets

from fastapi import Depends, HTTPException
from fastapi.security import HTTPBasic, HTTPBasicCredentials

security = HTTPBasic()


def basic_auth(credentials: HTTPBasicCredentials = Depends(security)) -> None:
    user = os.getenv("ADMIN_USER", "admin")
    pw = os.getenv("ADMIN_PASS", "truEnergy@2016")

    ok_user = secrets.compare_digest(credentials.username, user)
    ok_pass = secrets.compare_digest(credentials.password, pw)

    if not (ok_user and ok_pass):
        raise HTTPException(
            status_code=401,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )
