import datetime as dt
import json
import logging
from typing_extensions import override
import re

LOG_RECORD_BUILTIN_ATTRS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
    "taskName",
}


class JsonFormatter(logging.Formatter):
    def __init__(
        self,
        *,
        fmt_keys: dict[str, str] | None = None,
    ):
        super().__init__()
        self.fmt_keys = fmt_keys if fmt_keys is not None else {}

    @override
    def format(self, record: logging.LogRecord) -> str:
        message = self._prepare_log_dict(record)
        return json.dumps(message, default=str)

    def _prepare_log_dict(self, record: logging.LogRecord):
        always_fields = {
            "message": record.getMessage(),
            "timestamp": dt.datetime.fromtimestamp(
                record.created, tz=dt.timezone.utc
            ).isoformat(),
        }
        if record.exc_info is not None:
            always_fields["exc_info"] = self.formatException(record.exc_info)

        if record.stack_info is not None:
            always_fields["stack_info"] = self.formatStack(record.stack_info)

        message = {
            key: msg_val
            if (msg_val := always_fields.pop(val, None)) is not None
            else getattr(record, val)
            for key, val in self.fmt_keys.items()
        }
        message.update(always_fields)

        for key, val in record.__dict__.items():
            if key not in LOG_RECORD_BUILTIN_ATTRS:
                message[key] = val

        return message


class PipelineFilter(logging.Filter):
    @override
    def filter(self, record: logging.LogRecord) -> bool | logging.LogRecord:
        return record.module == "crow_pipeline"


class ServerFilter(logging.Filter):
    @override
    def filter(self, record: logging.LogRecord) -> bool | logging.LogRecord:
        return record.module == "crow_server"


class AsyncEngineFilter(logging.Filter):
    @override
    def filter(self, record: logging.LogRecord) -> bool | logging.LogRecord:
        return True


class SyncEngineFilter(logging.Filter):
    @override
    def filter(self, record: logging.LogRecord) -> bool | logging.LogRecord:
        return record.module == "sync_services"


class SpecificPipelineFilter(logging.Filter):

    def __init__(self, pipeline_id: int, name=''):
        super().__init__(name=name)
        self.pipeline_id = pipeline_id

    @override
    def filter(self, record: logging.LogRecord) -> bool | logging.LogRecord:
        return bool(re.search(pattern=re.compile(rf"ID: {self.pipeline_id}"), string=record.msg))


class ErrPipelineFilter(logging.Filter):

    def __init__(self, pipeline_id: int, name=''):
        super().__init__(name=name)
        self.pipeline_id = pipeline_id

    @override
    def filter(self, record: logging.LogRecord) -> bool | logging.LogRecord:
        filter1 = bool(re.search(pattern=re.compile(rf"ID: {self.pipeline_id}"), string=record.msg))
        filter2 = record.levelno >= logging.WARNING
        return filter1 and filter2


class StdoutFilter(logging.Filter):
    @override
    def filter(self, record: logging.LogRecord) -> bool | logging.LogRecord:
        return bool(re.search(pattern="STDOUT", string=record.msg))
