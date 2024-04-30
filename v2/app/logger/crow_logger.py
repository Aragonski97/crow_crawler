from app.logger.crow_logger_config import config
import logging.config
import logging.handlers

logger = logging.getLogger("asyncio")
logging.config.dictConfig(config)
queue_handler = logging.getHandlerByName("queue_handler")
if queue_handler is not None:
    queue_handler.listener.start()

