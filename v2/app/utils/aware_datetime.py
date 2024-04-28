from sqlalchemy.types import DateTime, TypeDecorator
from datetime import datetime
import pytz


class AwareDateTime(TypeDecorator):

    impl = DateTime
    cache_ok = False

    def process_result_value(self, value, dialect):
        return value.replace(tzinfo=pytz.utc)
