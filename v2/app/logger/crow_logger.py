from app.logger.crow_logger_config import config
import logging.config
import logging.handlers

logger = logging.getLogger("asyncio")
logging.config.dictConfig(config)
queue_handler = logging.getHandlerByName("queue_handler")
if queue_handler is not None:
    queue_handler.listener.start()


"""
    def configure_logger(self):
        pipeline_filter = logger.crow_logger_objects.SpecificPipelineFilter(pipeline_id=self.profile.id)
        pipeline_err_filter = logger.crow_logger_objects.ErrPipelineFilter(pipeline_id=self.profile.id)
        pipeline_formatter = logger.crow_logger_objects.JsonFormatter(fmt_keys={"level": "levelname",
                                                                                "message": "message",
                                                                                "timestamp": "timestamp",
                                                                                "module": "module",
                                                                                "function": "funcName",
                                                                                "line": "lineno"})
        pipeline_err_formatter = logging.Formatter(fmt="[%(levelname)s|%(module)s|L%(lineno)d] %(asctime)s: %(message)s",
                                                   datefmt="%Y-%m-%dT%H:%M:%S%z")
        pipeline_handler = RotatingFileHandler(filename=f"/home/axiom/PycharmProjects/Crow_1/logs/pipelines"
                                                        f"/{self.profile.id}.log",
                                               maxBytes=102400,
                                               backupCount=3)
        pipeline_err_handler = RotatingFileHandler(filename=f"/home/axiom/PycharmProjects/Crow_1/logs/pipelines"
                                                            f"/{self.profile.id}_err.log",
                                                   maxBytes=102400,
                                                   backupCount=3)
        pipeline_err_handler.addFilter(pipeline_err_filter)
        pipeline_err_handler.formatter = pipeline_err_formatter
        self.pipeline_handler = pipeline_handler
        pipeline_handler.addFilter(pipeline_filter)
        pipeline_handler.formatter = pipeline_formatter
        logger.addHandler(pipeline_handler)
        logger.addHandler(pipeline_err_handler)
        return
"""

