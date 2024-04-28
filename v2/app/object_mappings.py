from datetime import datetime

from .schemas.selector_schema import SelectorSchema, BaseModel
from .schemas.scraper_schema import ScraperModel
from .schemas.profile_schema import ProfileSchema
from .schemas.auth_schema import UserSchema, SignUpSchema, SignInSchema, TokenDataSchema

from .models.selector_model import SelectorModel, Base
from .models.scraper_model import ScraperModel
from .models.profile_model import ProfileModel
from .models.auth_model import UserModel

from .class_models.selector import *
from .class_models.step import Scraper
from .class_models.profile import Profile

from typing import Type


def resolve_model(schema: BaseModel) -> Type[Base]:
    match schema.__class__.__name__:
        case SelectorSchema.__qualname__: return SelectorModel
        case ScraperModel.__qualname__: return ScraperModel
        case ProfileSchema.__qualname__: return ProfileModel
        case UserSchema.__qualname__: return UserModel
        # in case of these, they are being resolved
        # as UserORM since we won't create SignIn and SignUp attempts.
        case SignInSchema.__qualname__: return UserModel
        case SignUpSchema.__qualname__: return UserModel
        case TokenDataSchema.__qualname__: return UserModel
        case _: raise TypeError(f"Unable to convert {schema.__class__.__name__} into an ORM object.")


def resolve_schema(model: Base) -> Type[BaseModel]:
    print(model.__class__.__name__)
    match model.__class__.__name__:
        case SelectorModel.__qualname__: return SelectorSchema
        case ScraperModel.__qualname__: return ScraperModel
        case ProfileModel.__qualname__: return ProfileSchema
        case UserModel.__qualname__: return UserSchema
        case _: raise TypeError


def resolve_selector(selector: SelectorSchema) -> Type[CrowSelector]:
    match selector.type:
        case "regex": return RegexSelector
        case "xpath": return XpathSelector
        case "json": return JsonSelector
        case "static": return StaticSelector
        case "css": return CssSelector
        case _: raise TypeError


def custom_model_to_schema(custom_model: Profile | Scraper | CrowSelector) -> BaseModel:
    if isinstance(custom_model, Profile):
        return ProfileSchema(
            name=custom_model.name,
            burst_rate=custom_model.burst_rate,
            headers=custom_model.headers,
            cookies=custom_model.cookies
        )
    elif isinstance(custom_model, Scraper):
        return ScraperModel(
            name=custom_model.name,
            type='scraper',
            headers=custom_model.headers,
            initial_url=custom_model.initial_url
        )
    elif isinstance(custom_model, CrowSelector):
        return SelectorSchema(
            name=custom_model.name,
            type=custom_model._name,
            method=custom_model.method,
            directive=custom_model.directive,
            required=custom_model.required,
            default_return=custom_model.default_return,
            post_processor=custom_model.post_processor
        )
    else:
        raise TypeError(f"Custom model conversion of {custom_model.__class__.__name__}"
                        f" has not been implemented.")


def schema_to_orm(schema: BaseModel) -> Base:
    orm_item = resolve_model(schema)
    return orm_item(
        **schema.model_dump()
    )


def orm_to_schema(model: Base) -> BaseModel:
    schema = resolve_schema(model)
    return schema(
        **model.to_dict()
    )


def selector_schema_to_selector_model(schema: SelectorSchema) -> CrowSelector:
    sel = resolve_selector(selector=schema)
    dump = schema.model_dump()
    dump.pop("type")
    return sel(
        **dump
    )


def scraper_schema_to_scraper_model(
        schema: ScraperModel,
        pipeline_order_id: int,
        burst_rate: int,
        headers: dict,
        ) -> Scraper:

    return Scraper(
        name=schema.name,
        headers=(lambda x: x if x is not None else headers)(schema.headers),
        pipeline_order_id=pipeline_order_id,
        initial_url=schema.initial_url,
        burst_rate=(lambda x: x if x is not None else burst_rate)(schema.burst_rate)
    )


def profile_schema_to_profile_model(schema: ProfileSchema) -> Profile:
    return Profile(
        **schema.model_dump()
    )


def user_from_signup_schemas(signup: SignUpSchema) -> UserSchema:
    """Extends SignUpSchema class into UserSchema"""
    return UserSchema(
        id=None,
        first_name=signup.first_name,
        last_name=signup.last_name,
        email=signup.email,
        password=signup.password,
        last_login=datetime.now()
    )
