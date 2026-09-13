"""File logging for dataset ingestion runs."""

import logging
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.config import LOG_DIR


DEFAULT_LOG_DIR = LOG_DIR
GMT_PLUS_2 = timezone(timedelta(hours=2), name="GMT+2")


class IngestionLogger:
    """Write timestamped ingestion progress and data-quality events to a file."""

    def __init__(self, dataset_path, enabled=True, log_dir=DEFAULT_LOG_DIR):
        self.path = None
        self._logger = None
        self._handler = None
        self.enabled = enabled
        if not enabled:
            return

        self._log_dir = Path(log_dir)
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._started = datetime.now(GMT_PLUS_2)
        self._table_name = None
        self.path = self._log_dir / self._filename()

        self._logger = logging.getLogger(f"ingestion.{os.getpid()}.{id(self)}")
        self._logger.setLevel(logging.INFO)
        self._logger.propagate = False
        self._attach_handler()
        self.info("Started ingestion for %s", dataset_path)

    def _filename(self):
        timestamp = self._started.strftime("%Y-%m-%d_%H-%M-%S_%f")
        table = "pending" if self._table_name is None else self._table_name
        table = re.sub(r"[^A-Za-z0-9_.-]+", "_", table).strip("._") or "unknown"
        return f"ingestion_{timestamp}_pid-{os.getpid()}_table-{table}.log"

    def _attach_handler(self):
        self._handler = logging.FileHandler(self.path, encoding="utf-8")
        formatter = logging.Formatter(
            fmt="%(asctime)s GMT+2 %(levelname)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
        formatter.converter = lambda timestamp: datetime.fromtimestamp(
            timestamp, GMT_PLUS_2
        ).timetuple()
        self._handler.setFormatter(formatter)
        self._logger.addHandler(self._handler)

    def set_table_name(self, table_name):
        """Add the config's table name to the log filename after config loading."""
        if not self.enabled or self._table_name == table_name:
            return
        old_path = self.path
        self._table_name = str(table_name)
        self._handler.close()
        self._logger.removeHandler(self._handler)
        self.path = self._log_dir / self._filename()
        old_path.rename(self.path)
        self._attach_handler()

    def info(self, message, *args):
        if self._logger is not None:
            self._logger.info(message, *args)

    def step(self, name, message):
        self.info("STEP [%s] %s", name, message)

    def removed(self, reason, count, before=None, after=None):
        details = f"Deleted {count:,} row(s): {reason}"
        if before is not None and after is not None:
            details += f" (rows before: {before:,}, after: {after:,})"
        self.info(details)

    def close(self):
        if self._handler is not None:
            self._handler.close()
            self._logger.removeHandler(self._handler)
            self._handler = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_value is not None:
            self.info("Ingestion failed: %s", exc_value)
        else:
            self.info("Ingestion completed")
        self.close()
