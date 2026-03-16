"""Persistent Result Queue.

Stores unsubmitted job results to disk when the API is unreachable
(circuit breaker OPEN). Results are retried when connectivity is restored.

File format: JSON array of result dicts, each annotated with a
``_queuedAt`` timestamp (seconds since epoch). Expired entries are
pruned on load and on every enqueue.
"""

import json
import logging
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger("ec-im-agent.result_queue")

# Maximum age of queued results before they are discarded (24 hours)
RESULT_TTL_SECONDS = 86400
# Maximum number of results to queue (prevent unbounded disk usage)
MAX_QUEUED_RESULTS = 500


class ResultQueue:
    """File-based persistent queue for unsubmitted job results.

    Results are serialised to a JSON file on disk so they survive agent
    restarts. Each entry carries an internal ``_queuedAt`` timestamp used
    to expire stale items.

    Args:
        queue_path: Path to the JSON file used for persistence.
    """

    def __init__(self, queue_path: str | Path) -> None:
        self._path = Path(queue_path).expanduser()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._data: list[dict[str, Any]] = []
        self._load()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load(self) -> None:
        """Load queued results from disk, pruning expired entries."""
        if not self._path.exists():
            self._data = []
            return
        try:
            raw = self._path.read_text(encoding="utf-8")
            self._data = json.loads(raw) if raw.strip() else []
            # Prune expired entries
            now = time.time()
            before = len(self._data)
            self._data = [
                entry for entry in self._data
                if now - entry.get("_queuedAt", 0) < RESULT_TTL_SECONDS
            ]
            if len(self._data) != before:
                pruned = before - len(self._data)
                logger.info(
                    "Pruned %d expired result(s) from queue", pruned,
                )
                self._save()
        except (json.JSONDecodeError, OSError) as exc:
            logger.warning(
                "Failed to load result queue from %s: %s", self._path, exc,
            )
            self._data = []

    def _save(self) -> None:
        """Atomically write queued results to disk (write-then-rename)."""
        tmp_path = self._path.with_suffix(".tmp")
        try:
            tmp_path.write_text(
                json.dumps(self._data, default=str), encoding="utf-8",
            )
            tmp_path.replace(self._path)
        except OSError as exc:
            logger.error("Failed to save result queue: %s", exc)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def enqueue(self, results: list[dict[str, Any]]) -> int:
        """Add results to the persistent queue.

        Entries older than ``RESULT_TTL_SECONDS`` are pruned first.  If the
        queue is at capacity, remaining results are dropped with a warning.

        Args:
            results: List of job result dicts to queue.

        Returns:
            Number of results actually queued.
        """
        # Prune before adding to reclaim space
        now = time.time()
        self._data = [
            entry for entry in self._data
            if now - entry.get("_queuedAt", 0) < RESULT_TTL_SECONDS
        ]

        queued = 0
        for result in results:
            if len(self._data) >= MAX_QUEUED_RESULTS:
                logger.warning(
                    "Result queue full (%d items), dropping remaining %d result(s)",
                    MAX_QUEUED_RESULTS,
                    len(results) - queued,
                )
                break
            entry = {**result, "_queuedAt": now}
            self._data.append(entry)
            queued += 1

        if queued > 0:
            self._save()
            logger.info(
                "Queued %d result(s) for later submission (total: %d)",
                queued,
                len(self._data),
            )
        return queued

    def drain(self) -> list[dict[str, Any]]:
        """Remove and return all queued results, stripping internal metadata.

        Returns:
            List of result dicts ready for submission.  Empty list when the
            queue is empty.
        """
        if not self._data:
            return []
        results: list[dict[str, Any]] = []
        for entry in self._data:
            clean = {k: v for k, v in entry.items() if not k.startswith("_")}
            results.append(clean)
        count = len(results)
        self._data = []
        self._save()
        logger.info("Drained %d result(s) from queue", count)
        return results

    @property
    def size(self) -> int:
        """Number of results currently queued."""
        return len(self._data)
