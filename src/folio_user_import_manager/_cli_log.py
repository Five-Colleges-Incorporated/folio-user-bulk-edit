import logging
from logging.config import dictConfig
from pathlib import Path


def initialize(
    log_directory: Path,
    now: str,
    console_level: int = logging.WARNING,
    verbose_file_level: int = logging.INFO,
) -> None:
    log_directory.mkdir(exist_ok=True, parents=True)

    (log_directory / f"{now}-verbose.log.tsv").write_text(
        "asctime\tlevelname\tprocess\tthread\ttaskName\tmodule\tfilename\tlineno\tmessage\n",
    )
    dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {"format": "[%(levelname)-8s] %(message)s"},
                "verbose": {
                    "format": '"%(asctime)s"\t%(levelname)s\t'
                    "%(process)d\t%(thread)d\t%(taskName)s\t"
                    '%(module)s\t%(filename)s\t%(lineno)d\t"%(message)s"',
                },
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "standard"
                    if console_level > logging.DEBUG
                    else "verbose",
                    "level": console_level,
                    "stream": "ext://sys.stdout",
                },
                "file": {
                    "class": "logging.FileHandler",
                    "level": logging.WARNING,
                    "filename": log_directory / f"{now}.log",
                    "formatter": "standard",
                },
                "verbose_file": {
                    "class": "logging.FileHandler",
                    "level": verbose_file_level,
                    "filename": log_directory / f"{now}-verbose.log.tsv",
                    "formatter": "verbose",
                },
            },
            "loggers": {
                "": {
                    "handlers": ["console", "file", "verbose_file"],
                    "level": min(logging.WARNING, console_level, verbose_file_level),
                },
            },
        },
    )
