from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import select, and_
from typing import Annotated, Any
from fastapi.encoders import jsonable_encoder
from fastapi import Depends, HTTPException, status
from datetime import datetime, UTC, timedelta
from jwt import decode, encode, ExpiredSignatureError, PyJWTError

from app.schemas.auth_schema import UserSchema, SignInSchema, TokenDataSchema
from app.schemas.error_schema import ErrorSchema
from app.models.auth_model import UserModel
from crow_database import CORE_DATABASE, update
from crow_config import (
    ACCESS_TOKEN_ALGORITHM,
    ACCESS_TOKEN_SECRET_KEY,
    ACCESS_TOKEN_EXPIRE_MINUTES,
    OAUTH2_SCHEME
    )
from app.object_mappings import orm_to_schema


async def get_user(email: str, password: str) -> UserSchema | None:
    """
    Takes the first element returned by the statement execution,
    converts to dictionary and tries to get the item with class name of ORM object
    resolved by schema passed as an argument of the function.
    This is reliable because we will be outputting UserSchema only from this function
    as well as because we expect only auth schemas to enter.
    They are by default mapped as UserModel in resolve_model function.
    However, just in case, if that's not the case, resolve_model(schema).__qualname__ will be different
    then what is stored in results.first()._asdict(), so the .get method will return None,
    therefore we will raise HTTP exception.
    """

    if email is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=ErrorSchema(
                err_type=SQLAlchemyError,
                err_message="User doesn't exist in the database. Please Sign Up."
            )
        )
    session = CORE_DATABASE.get_session()
    try:
        async with session as session:
            statement = select(UserModel).filter(
                and_(UserModel.email == email, UserModel.password == password)).limit(1)
            results = await session.execute(statement)
            db_user = results.first()._asdict().get("UserModel")
            if db_user is None:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail=jsonable_encoder(ErrorSchema(
                        err_type=SQLAlchemyError,
                        err_message="User doesn't exist in the database. Please Sign Up."
                    )
                ))
            user_schema = orm_to_schema(model=db_user)
            if not isinstance(user_schema, UserSchema):
                raise TypeError(f"Results retrieved by the database,"
                                f" resolved to a type{user_schema.__class__.__name__} "
                                f"instead of {UserSchema.__qualname__}")
            await session.commit()
            return user_schema
    except SQLAlchemyError as err:
        raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                            detail=jsonable_encoder(ErrorSchema(
                                err_type=err,
                                err_message=err.orig.args[0]
                            )))


def decode_jwt(token: str) -> Any:
    """
    :param token: token to be decoded
    :return: return decoded token
    """
    try:
        return decode(token, ACCESS_TOKEN_SECRET_KEY, algorithms=[ACCESS_TOKEN_ALGORITHM])
    except ExpiredSignatureError as err:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                            detail=ErrorSchema(
                                err_type=err,
                                err_message=err.args[0]
                            ))


def encode_jwt(user: SignInSchema) -> str:
    """
    :param user: user that tried logging in to the website.
    :return: return encoded jwt token
    """
    payload = {
        "email": user.email,
        "password": user.password,
        "exp": datetime.now(UTC) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    }
    try:
        return "bearer " + encode(payload, ACCESS_TOKEN_SECRET_KEY, algorithm=ACCESS_TOKEN_ALGORITHM)
    except TypeError as err:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=jsonable_encoder(ErrorSchema(
                                err_type=err,
                                err_message=err.args[0]
                            )))


async def authenticate(token: Annotated[str, Depends(OAUTH2_SCHEME)]) -> UserSchema:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials.",
        headers={"WWW-Authenticate": "Bearer"},
    )
    if token == "":
        raise credentials_exception
    try:
        payload = decode_jwt(token)
        email = payload.get("email")
        if email is None:
            raise credentials_exception
        password = payload.get("password")
        if password is None:
            raise credentials_exception
        exp = payload.get("exp")
        if exp is None:
            raise credentials_exception
        # perform validation on data
        _ = TokenDataSchema(email=email, password=password, exp=exp)
    except PyJWTError:
        raise credentials_exception
    user = await get_user(email=email, password=password)
    if user is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST,
                            detail=jsonable_encoder(ErrorSchema(
                                err_type=SQLAlchemyError,
                                err_message="User doesn't exist in the database."
                            )))
    return user


async def update_user(schema: UserSchema | SignInSchema) -> Any:
    """
    :param schema: Schema that will be updated.
    :return: Results returned by selecting from core database.
    """
    session = CORE_DATABASE.get_session()
    async with session as session:
        try:
            statement = update(UserModel).filter(UserModel.email == schema.email).values(schema.model_dump())
            await session.execute(statement)
            await session.commit()
            return schema
        except SQLAlchemyError as err:
            raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                                detail=jsonable_encoder(ErrorSchema(
                                    err_type=err,
                                    err_message=err.orig.args[1]
                                )))

VerifiedUser = Annotated[UserSchema, Depends(authenticate)]
