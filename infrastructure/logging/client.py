"""Pipeline logger — writes structured plain-text logs to console and a dated file.

Log line format::

    2024-01-01 10:00:00 | INFO    | ingestion | run_20240101T120000Z | Starting pipeline

Usage::

    from infrastructure.logging.client import PipelineLogger

    with PipelineLogger(run_id="20240101T120000Z", phase="ingestion") as logger:
        logger.info("Ingestion started")
        logger.warning("Quota is running low")
        logger.error("API call failed")
"""
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

# Default log directory relative to the project root
_DEFAULT_LOG_DIR = Path(__file__).parent / "logs"


class _PipelineFormatter(logging.Formatter):
    """Formats every log record as a fixed-width pipe-delimited line.

    Output::

        2024-01-01 10:00:00 | INFO    | ingestion | run_123 | message
    """

    def __init__(self, run_id: str, phase: str) -> None:
        super().__init__()
        self._run_id = run_id
        self._phase = phase

    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.fromtimestamp(
            record.created, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")
        # Pad level to 7 chars so columns stay aligned (WARNING is the longest)
        level = record.levelname.ljust(7)
        return (
            f"{timestamp} | {level} | {self._phase} | {self._run_id} | {record.getMessage()}"
        )


class PipelineLogger:
    """Structured logger for a single pipeline run and phase.

    Each instance writes to two destinations:

    * **Console** (stdout) — real-time output during a run.
    * **File** — ``<log_dir>/<YYYY-MM-DD>.log``; all runs on the same date
      append to the same file.

    ``run_id`` and ``phase`` are stamped on every line so interleaved runs
    in the same log file remain distinguishable.

    The logger is usable as a context manager — calling ``close()``
    flushes and releases the file handle::

        with PipelineLogger("run_001", "ingestion") as log:
            log.info("started")

    Or manage it explicitly::

        log = PipelineLogger("run_001", "ingestion")
        log.info("started")
        log.close()

    Args:
        run_id:  Unique identifier for this pipeline run
                 (e.g. orchestrator ``run_id`` or ``pipeline_runs.id``).
        phase:   Pipeline phase label, e.g. ``"ingestion"``,
                 ``"transformation"``, ``"loading"``.
        log_dir: Directory where dated log files are written.
                 Defaults to ``infrastructure/logging/logs/``.
        level:   Minimum level to emit (``logging.DEBUG`` by default —
                 show everything).
    """

    def __init__(
        self,
        run_id: str,
        phase: str,
        log_dir: str | Path = _DEFAULT_LOG_DIR,
        level: int = logging.DEBUG,
    ) -> None:
        self._run_id = run_id
        self._phase = phase

        # Use a unique logger name so multiple PipelineLogger instances
        # with different run/phase combos never share handlers.
        logger_name = f"pipeline.{run_id}.{phase}"
        self._logger = logging.getLogger(logger_name)
        self._logger.setLevel(level)
        self._logger.propagate = False  # do not leak into root logger

        # Guard against duplicate handlers when the same named logger is
        # retrieved more than once in a long-running process.
        if not self._logger.handlers:
            formatter = _PipelineFormatter(run_id, phase)
            self._attach_console_handler(formatter, level)
            self._attach_file_handler(formatter, level, Path(log_dir))

    # ------------------------------------------------------------------
    # Public log methods
    # ------------------------------------------------------------------

    def debug(self, message: str) -> None:
        """Emit a DEBUG-level message."""
        self._logger.debug(message)

    def info(self, message: str) -> None:
        """Emit an INFO-level message."""
        self._logger.info(message)

    def warning(self, message: str) -> None:
        """Emit a WARNING-level message."""
        self._logger.warning(message)

    def error(self, message: str) -> None:
        """Emit an ERROR-level message."""
        self._logger.error(message)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Flush and close all handlers, releasing the log file handle."""
        for handler in self._logger.handlers[:]:
            handler.flush()
            handler.close()
            self._logger.removeHandler(handler)

    def __enter__(self) -> "PipelineLogger":
        return self

    def __exit__(self, *_) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _attach_console_handler(
        self, formatter: _PipelineFormatter, level: int
    ) -> None:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)

    def _attach_file_handler(
        self, formatter: _PipelineFormatter, level: int, log_dir: Path
    ) -> None:
        log_dir.mkdir(parents=True, exist_ok=True)
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        log_file = log_dir / f"{date_str}.log"
        handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        handler.setLevel(level)
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)
