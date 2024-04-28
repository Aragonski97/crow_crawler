import asyncio
import uvicorn
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import APIRouter, HTTPException, status, FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.logger.crow_logger import queue_handler

from app.schemas.selector_schema import SelectorSchema
from app.schemas.auth_schema import SignUpSchema, TokenSchema, SignInSchema, UserSchema
from app.schemas.error_schema import ErrorSchema
from app.schemas.profile_schema import ProfileSchema
from app.schemas.scraper_schema import ScraperModel


from app.object_mappings import (
    profile_schema_to_profile_model,
    user_from_signup_schemas,
    selector_schema_to_selector_model,
    scraper_schema_to_scraper_model
)

from app.services.auth_service import get_user, encode_jwt, VerifiedUser, update_user
from app.class_models.pipeline import ScrapingPipeline
from crow_database import CORE_DATABASE

from engines.async_engine import AsyncEngine, mp, Task
from engines.sync_engine import SyncEngine

from fastapi.encoders import jsonable_encoder


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan deals with server lifecycle operations.
    """
    global ASYNC_ENGINE
    global SYNC_ENGINE
    global SYNC_PROCESS
    global ASYNC_ENGINE_RUN_TASK
    await CORE_DATABASE.create_core_tables()
    try:
        SYNC_ENGINE = SyncEngine(inbound=SYNC_INBOUND, outbound=SYNC_OUTBOUND)
        ASYNC_ENGINE = AsyncEngine(inbound=SYNC_OUTBOUND, outbound=SYNC_INBOUND)
        SYNC_PROCESS = mp.Process(target=SYNC_ENGINE.initiate)

        SYNC_PROCESS.start()
        ASYNC_ENGINE_RUN_TASK = asyncio.create_task(ASYNC_ENGINE.run())
    except KeyboardInterrupt:
        ASYNC_ENGINE_RUN_TASK.cancel(msg="Keyboard Interrupt.")
        await CORE_DATABASE.close()
        exit(-1)
    except Exception as err:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=ErrorSchema(
                                err_type=err,
                                err_message=err.args[0]
                            ))
    yield
    queue_handler.listener.stop()
    SYNC_PROCESS.join()
    #ASYNC_ENGINE_RUN_TASK.cancel()
    await CORE_DATABASE.close()

app = FastAPI(debug=True, lifespan=lifespan)

origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


SYNC_INBOUND = mp.Queue()
SYNC_OUTBOUND = mp.Queue()
SYNC_ENGINE: SyncEngine
ASYNC_ENGINE: AsyncEngine
SYNC_PROCESS: mp.Process
ASYNC_ENGINE_RUN_TASK: Task

pipeline = APIRouter()

current_pipeline: ScrapingPipeline


@pipeline.post("/initiate_pipeline")
async def initiate_pipeline():
    global current_pipeline
    try:
        await ASYNC_ENGINE.pipeline_backlog.put(current_pipeline)
    except NameError as err:
        raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                            detail=jsonable_encoder(ErrorSchema(
                                err_type=err,
                                err_message=err.args[0]
                            )))
    current_pipeline = None
    return True


@pipeline.post("/create_profile")
async def create_profile(
        profile: ProfileSchema,
        user: VerifiedUser
):
    global current_pipeline
    profile_cls = profile_schema_to_profile_model(profile)
    current_pipeline = ScrapingPipeline(profile=profile_cls)
    return profile


@pipeline.post("/create_scraper")
async def add_scraper(
        scraper: ScraperModel,
        user: VerifiedUser
):
    global current_pipeline
    try:
        scraper_id = current_pipeline.get_scraper_order_by_name(name=scraper.name)
    except NameError as err:
        raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                            detail=jsonable_encoder(ErrorSchema(
                                err_type=err,
                                err_message=err.args[0]
                            )))
    if scraper_id is not None:
        raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                            detail=ErrorSchema(
                                err_type=ValueError,
                                err_message="Scraper already exists in the current pipeline."
                            ))
    current_pipeline.scrapers.append(
        scraper_schema_to_scraper_model(schema=scraper,
                                        pipeline_order_id=len(current_pipeline.scrapers),
                                        headers=current_pipeline.profile.headers,
                                        burst_rate=current_pipeline.profile.burst_rate))
    return scraper


@pipeline.delete("/delete_scraper/{scraper_name}")
async def delete_scraper(
        scraper_name: str,
        user: VerifiedUser
):
    global current_pipeline
    try:
        scraper_id = current_pipeline.get_scraper_order_by_name(name=scraper_name)
    except NameError as err:
        raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                            detail=jsonable_encoder(ErrorSchema(
                                err_type=err,
                                err_message=err.args[0]
                            )))
    if scraper_id is None:
        raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                            detail=jsonable_encoder(ErrorSchema(
                                err_type=ValueError,
                                err_message="Scraper doesn't exist"
                            )))
    scraper = current_pipeline.scrapers[scraper_id]
    current_pipeline.scrapers.remove(scraper)
    return True


@pipeline.post("/{scraper}/add_selector")
async def add_selector(
        scraper: str,
        selector: SelectorSchema,
        user: VerifiedUser
) -> SelectorSchema:
    global current_pipeline
    try:
        scraper_id = current_pipeline.get_scraper_order_by_name(name=scraper)
    except NameError as err:
        raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                            detail=jsonable_encoder(ErrorSchema(
                                err_type=err,
                                err_message=err.args[0]
                            )))
    if scraper_id is None:
        raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                            detail=ErrorSchema(
                                err_type=ValueError,
                                err_message="You tried adding selector to a non-existing scraper."
                            ))
    selector_id = current_pipeline.scrapers[scraper_id].get_selector_id_by_name(name=selector.name)
    if selector_id is not None:
        raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                            detail=ErrorSchema(
                                err_type=ValueError,
                                err_message="Selector with given name already exists."
                            ))
    else:
        current_pipeline.scrapers[scraper_id].selectors.append(selector_schema_to_selector_model(schema=selector))
    return selector


@pipeline.delete("/{scraper}/delete_selector/{selector_name}")
async def delete_selector(
        scraper: str,
        selector_name: str,
        user: VerifiedUser
) -> bool:
    global current_pipeline
    scraper_id = current_pipeline.get_scraper_order_by_name(name=scraper)
    if scraper_id is None:
        raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                            detail=ErrorSchema(
                                err_type=ValueError,
                                err_message="You tried removing a selector from a non-existing scraper."
                            ))
    selector_id = current_pipeline.scrapers[scraper_id].get_selector_id_by_name(name=selector_name)
    if selector_id is None:
        raise HTTPException(status_code=status.HTTP_405_METHOD_NOT_ALLOWED,
                            detail=ErrorSchema(
                                err_type=ValueError,
                                err_message="Selector with given name already exists."
                            ))
    else:
        sel = current_pipeline.scrapers[scraper_id].selectors[selector_id]
        current_pipeline.scrapers[scraper_id].selectors.remove(sel)
    return True

auth_router = APIRouter()


@auth_router.post("/signin")
async def signin(schema: SignInSchema) -> TokenSchema:
    """
    Create SignIn model and awaits the response from database to check if the user exists.
    If it does, return an access jwt token, otherwise raises exception.
    """
    user: UserSchema = await get_user(email=schema.email, password=schema.password)
    token_string = encode_jwt(user=schema)
    user.last_login = datetime.now()
    _ = await update_user(schema=user)
    return TokenSchema(ACCESS_TOKEN=token_string)


@auth_router.post("/signup")
async def signup(schema: SignUpSchema):
    """
    Create SignUpSchema and converts it to SQLAlchemy.ORM Declarative Base subclass(Base)
    This function is not yet completely finished, but eases up creation of new users.
    Otherwise, I'd planned to implement SMTP handler to send emails to admins to allow or disallow new user creation.
    """
    user = user_from_signup_schemas(signup=schema)
    return await CORE_DATABASE.insert_item(schema=user)


@auth_router.get("/is_authenticated")
async def is_authenticated(user: VerifiedUser):
    """
    Check if user token decomposed email relates to an existing User.
    Gets called on all frontend calls.
    """
    if user is not None:
        return True
    else:
        return False


app.include_router(auth_router)
app.include_router(pipeline)

if __name__ == "__main__":
    uvicorn.run("crow_server:app", host="0.0.0.0", port=8000)
