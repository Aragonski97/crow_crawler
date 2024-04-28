import logging
from app.logger.crow_logger_objects import (JsonFormatter,
                                            StdoutFilter,
                                            AsyncEngineFilter,
                                            SyncEngineFilter,
                                            ServerFilter)
from crow_config import APP_PATH, SYS_DEL

config = {
  "version": 1,
  "disable_existing_loggers": False,
  "formatters": {
    "json": {
      "()": JsonFormatter,
      "fmt_keys": {
        "level": "levelname",
        "message": "message",
        "timestamp": "timestamp",
        "logger": "name",
        "module": "module",
        "function": "funcName",
        "line": "lineno"
      }
    }
  },
  "filters": {
    "async_engine_filter": {
      "()": AsyncEngineFilter
    },
    "sync_engine_filter": {
      "()": SyncEngineFilter
    },
    "server_filter": {
      "()": ServerFilter
    },
    "stdout": {
      "()": StdoutFilter
    }
  },
  "handlers": {
    "stdout": {
      "class": logging.StreamHandler,
      "level": "DEBUG",
      "formatter": "json",
      "filters": ["stdout"],
      "stream": "ext://sys.stdout"
    },
    "error": {
        "class": logging.FileHandler,
        "level": "WARNING",
        "formatter": "json",
        "filename": APP_PATH / f"logger{SYS_DEL}logs{SYS_DEL}errs.log",
    },
    "async_engine_file": {
      "class": logging.FileHandler,
      "level": "DEBUG",
      "formatter": "json",
      "filename": APP_PATH / fr"logger{SYS_DEL}logs{SYS_DEL}async_engine.log",
      "filters": ["async_engine_filter"]
    },
    "sync_engine_file": {
      "class": logging.FileHandler,
      "level": "DEBUG",
      "formatter": "json",
      "filename": APP_PATH / f"logger{SYS_DEL}logs{SYS_DEL}sync_engine.log",
      "filters": ["sync_engine_filter"]
    },
    "server_file": {
      "class": logging.FileHandler,
      "level": "DEBUG",
      "formatter": "json",
      "filename": APP_PATH / f"logger{SYS_DEL}logs{SYS_DEL}server.log",
      "filters": ["server_filter"]
    },
    "queue_handler": {
      "class": "logging.handlers.QueueHandler",
      "handlers": ["server_file", "sync_engine_file", "async_engine_file", "stdout"],
      "respect_handler_level": True
    }
  },
  "loggers": {
    "root": {
      "level": "DEBUG",
      "handlers": ["queue_handler"]
    }
  }
}
