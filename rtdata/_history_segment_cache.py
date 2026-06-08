"""rtdata SDK 历史 K 线分段二进制缓存"""

import json
import logging
import os
import struct
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


logger = logging.getLogger(__name__)


KlineRow = Tuple[int, float, float, float, float, int, float, int]

MAGIC = b"RTKH"
SCHEMA_VERSION = 1
HEADER_STRUCT = struct.Struct("<4sB3xqqII")
RECORD_STRUCT = struct.Struct("<qddddqdq")
PERIOD_MS = {
    "1m": 60_000,
    "5m": 5 * 60_000,
    "15m": 15 * 60_000,
    "30m": 30 * 60_000,
    "1h": 60 * 60_000,
    "2h": 2 * 60 * 60_000,
    "4h": 4 * 60 * 60_000,
    "1d": 24 * 60 * 60_000,
    "1w": 7 * 24 * 60 * 60_000,
    "1M": 31 * 24 * 60 * 60_000,
}


try:
    import fcntl  # type: ignore
except ImportError:  # pragma: no cover
    fcntl = None

try:
    import msvcrt  # type: ignore
except ImportError:  # pragma: no cover
    msvcrt = None


class _FileLock:
    def __init__(self, path: Path):
        self._path = path
        self._fp = None

    def __enter__(self):
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fp = open(self._path, "a+b")
        if fcntl is not None:
            fcntl.flock(self._fp.fileno(), fcntl.LOCK_EX)
        elif msvcrt is not None:  # pragma: no cover
            msvcrt.locking(self._fp.fileno(), msvcrt.LK_LOCK, 1)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._fp is None:
            return
        try:
            if fcntl is not None:
                fcntl.flock(self._fp.fileno(), fcntl.LOCK_UN)
            elif msvcrt is not None:  # pragma: no cover
                self._fp.seek(0)
                msvcrt.locking(self._fp.fileno(), msvcrt.LK_UNLCK, 1)
        finally:
            self._fp.close()
            self._fp = None


class HistorySegmentCache:
    """按 symbol + period + adjust 维护纯文件分段历史缓存。"""

    def __init__(self, cache_dir: Optional[str] = None, enabled: bool = True,
                 tail_refresh_bars: int = 1):
        self._enabled = enabled
        self._tail_refresh_bars = max(0, int(tail_refresh_bars))
        base_dir = Path(cache_dir) if cache_dir else Path.home() / ".rtdata"
        self._root_dir = base_dir / "history_v1"
        self._series_locks: Dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()
        if enabled:
            self._root_dir.mkdir(parents=True, exist_ok=True)

    @property
    def enabled(self) -> bool:
        return self._enabled

    def get_missing_ranges(self, symbol: str, period: str,
                           adjust: str,
                           start_ms: int, end_exclusive_ms: int) -> List[Tuple[int, int]]:
        if not self.enabled or start_ms <= 0 or end_exclusive_ms <= start_ms:
            return [(start_ms, end_exclusive_ms)]

        with self._series_guard(symbol, period, adjust) as series_dir:
            index = self._load_index(series_dir, symbol, period, adjust)
            coverages = self._effective_coverages(index, period)
            return self._difference([(start_ms, end_exclusive_ms)], coverages)

    def load_range(self, symbol: str, period: str, adjust: str,
                   start_ms: int, end_ms: int) -> List[KlineRow]:
        if not self.enabled or start_ms <= 0 or end_ms < start_ms:
            return []

        with self._series_guard(symbol, period, adjust) as series_dir:
            index = self._load_index(series_dir, symbol, period, adjust)
            relevant = [
                seg for seg in index["segments"]
                if int(seg["start_ms"]) <= end_ms and int(seg["end_ms"]) > start_ms
            ]
            relevant.sort(key=lambda seg: (int(seg.get("created_at_ms", 0)), seg["file"]))

            merged: Dict[int, KlineRow] = {}
            for seg in relevant:
                for row in self._read_segment(series_dir / "segments" / seg["file"], start_ms, end_ms):
                    merged[row[0]] = row

        return [merged[ts] for ts in sorted(merged)]

    def store_range(self, symbol: str, period: str, adjust: str,
                    request_start_ms: int, request_end_exclusive_ms: int,
                    klines: Iterable[KlineRow]):
        if not self.enabled or request_start_ms <= 0 or request_end_exclusive_ms <= request_start_ms:
            return

        period_ms = self._period_ms(period)
        rows = self._normalize_rows(klines)
        fetched_at_ms = self._now_ms()

        with self._series_guard(symbol, period, adjust) as series_dir:
            index = self._load_index(series_dir, symbol, period, adjust)
            if not rows:
                return

            segments_dir = series_dir / "segments"
            segments_dir.mkdir(parents=True, exist_ok=True)

            seg_start_ms = rows[0][0]
            seg_end_ms = rows[-1][0] + max(period_ms, 1)
            final_name = f"{seg_start_ms}_{seg_end_ms}_{len(rows)}_{fetched_at_ms}.rtk"
            temp_name = final_name + ".part"
            self._write_segment(segments_dir / temp_name, rows, seg_start_ms, seg_end_ms)
            os.replace(segments_dir / temp_name, segments_dir / final_name)
            index["segments"].append({
                "file": final_name,
                "start_ms": seg_start_ms,
                "end_ms": seg_end_ms,
                "rows": len(rows),
                "created_at_ms": fetched_at_ms,
            })

            index["coverage"].append({
                "start_ms": request_start_ms,
                "end_ms": request_end_exclusive_ms,
                "fetched_at_ms": fetched_at_ms,
            })
            index["coverage"] = self._merge_coverages(index["coverage"])
            index["segments"] = self._compact_segments(series_dir, index["segments"])
            self._save_index(series_dir, index)

    @contextmanager
    def _series_guard(self, symbol: str, period: str, adjust: str):
        series_key = f"{symbol}::{period}::{adjust}"
        with self._locks_guard:
            lock = self._series_locks.setdefault(series_key, threading.Lock())
        with lock:
            series_dir = self._series_dir(symbol, period, adjust)
            series_dir.mkdir(parents=True, exist_ok=True)
            lock_path = series_dir / "locks" / "cache.lock"
            with _FileLock(lock_path):
                yield series_dir

    def _series_dir(self, symbol: str, period: str, adjust: str) -> Path:
        safe_symbol = symbol.replace("/", "_")
        safe_period = period.replace("/", "_")
        safe_adjust = adjust.replace("/", "_")
        return self._root_dir / safe_symbol / safe_period / safe_adjust

    def _index_path(self, series_dir: Path) -> Path:
        return series_dir / "index.json"

    def _load_index(self, series_dir: Path, symbol: str, period: str, adjust: str) -> dict:
        self._cleanup_temp_files(series_dir / "segments")
        index_path = self._index_path(series_dir)
        if index_path.exists():
            try:
                with open(index_path, "r", encoding="utf-8") as f:
                    index = json.load(f)
                if index.get("schema_version") == SCHEMA_VERSION:
                    index.setdefault("coverage", [])
                    index.setdefault("segments", [])
                    index["segments"] = self._filter_valid_segments(series_dir, index["segments"])
                    index["coverage"] = self._merge_coverages(index["coverage"])
                    if not index["segments"] and index["coverage"]:
                        index["coverage"] = []
                        self._save_index(series_dir, index)
                    return index
            except Exception as exc:
                logger.warning("Failed to load history index for %s %s: %s", symbol, period, exc)
        index = self._rebuild_index(series_dir, symbol, period, adjust)
        self._save_index(series_dir, index)
        return index

    def _save_index(self, series_dir: Path, index: dict):
        index_path = self._index_path(series_dir)
        temp_path = index_path.with_suffix(".json.part")
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(index, f, ensure_ascii=False, indent=2, sort_keys=True)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_path, index_path)

    def _rebuild_index(self, series_dir: Path, symbol: str, period: str, adjust: str) -> dict:
        segments = self._scan_segments(series_dir)
        coverage = [
            {
                "start_ms": int(seg["start_ms"]),
                "end_ms": int(seg["end_ms"]),
                "fetched_at_ms": int(seg.get("created_at_ms", 0)),
            }
            for seg in segments
        ]
        return {
            "schema_version": SCHEMA_VERSION,
            "symbol": symbol,
            "period": period,
            "adjust": adjust,
            "tail_refresh_bars": self._tail_refresh_bars,
            "segments": segments,
            "coverage": self._merge_coverages(coverage),
        }

    def _scan_segments(self, series_dir: Path) -> List[dict]:
        segments_dir = series_dir / "segments"
        segments_dir.mkdir(parents=True, exist_ok=True)
        segments = []
        for path in segments_dir.glob("*.rtk"):
            meta = self._segment_meta_from_file(path)
            if meta is not None:
                segments.append(meta)
        segments.sort(key=lambda seg: (int(seg["start_ms"]), int(seg.get("created_at_ms", 0)), seg["file"]))
        return segments

    def _filter_valid_segments(self, series_dir: Path, segments: List[dict]) -> List[dict]:
        valid = []
        seen = set()
        for seg in segments:
            filename = seg.get("file")
            if not filename or filename in seen:
                continue
            path = series_dir / "segments" / filename
            meta = self._segment_meta_from_file(path)
            if meta is None:
                continue
            valid.append(meta)
            seen.add(filename)
        valid.sort(key=lambda seg: (int(seg["start_ms"]), int(seg.get("created_at_ms", 0)), seg["file"]))
        return valid

    def _cleanup_temp_files(self, segments_dir: Path):
        if not segments_dir.exists():
            return
        for path in segments_dir.glob("*.part"):
            try:
                path.unlink()
            except FileNotFoundError:
                pass

    def _segment_meta_from_file(self, path: Path) -> Optional[dict]:
        try:
            with open(path, "rb") as f:
                header = f.read(HEADER_STRUCT.size)
            magic, version, start_ms, end_ms, rows, record_size = HEADER_STRUCT.unpack(header)
            if magic != MAGIC or version != SCHEMA_VERSION or record_size != RECORD_STRUCT.size:
                raise ValueError("invalid segment header")
            expected_size = HEADER_STRUCT.size + rows * RECORD_STRUCT.size
            if path.stat().st_size != expected_size:
                raise ValueError("segment size mismatch")
            created_at_ms = self._parse_created_at(path.name)
            return {
                "file": path.name,
                "start_ms": int(start_ms),
                "end_ms": int(end_ms),
                "rows": int(rows),
                "created_at_ms": created_at_ms,
            }
        except Exception:
            logger.warning("Removing invalid history segment: %s", path)
            try:
                path.unlink()
            except FileNotFoundError:
                pass
            return None

    def _parse_created_at(self, filename: str) -> int:
        stem = filename[:-4] if filename.endswith(".rtk") else filename
        parts = stem.split("_")
        if len(parts) >= 4:
            try:
                return int(parts[3])
            except ValueError:
                return 0
        return 0

    def _write_segment(self, path: Path, rows: List[KlineRow], start_ms: int, end_ms: int):
        with open(path, "wb") as f:
            f.write(HEADER_STRUCT.pack(
                MAGIC,
                SCHEMA_VERSION,
                int(start_ms),
                int(end_ms),
                len(rows),
                RECORD_STRUCT.size,
            ))
            for row in rows:
                f.write(RECORD_STRUCT.pack(
                    int(row[0]),
                    float(row[1]),
                    float(row[2]),
                    float(row[3]),
                    float(row[4]),
                    int(row[5]),
                    float(row[6]),
                    int(row[7]),
                ))
            f.flush()
            os.fsync(f.fileno())

    def _read_segment(self, path: Path, start_ms: int, end_ms: int) -> List[KlineRow]:
        rows: List[KlineRow] = []
        with open(path, "rb") as f:
            header = f.read(HEADER_STRUCT.size)
            magic, version, _start, _end, count, record_size = HEADER_STRUCT.unpack(header)
            if magic != MAGIC or version != SCHEMA_VERSION or record_size != RECORD_STRUCT.size:
                raise ValueError(f"Invalid history segment: {path}")
            for _ in range(count):
                raw = f.read(RECORD_STRUCT.size)
                row = RECORD_STRUCT.unpack(raw)
                if start_ms <= row[0] <= end_ms:
                    rows.append((
                        int(row[0]),
                        float(row[1]),
                        float(row[2]),
                        float(row[3]),
                        float(row[4]),
                        int(row[5]),
                        float(row[6]),
                        int(row[7]),
                    ))
        return rows

    def _compact_segments(self, series_dir: Path, segments: List[dict]) -> List[dict]:
        if len(segments) <= 1:
            return self._filter_valid_segments(series_dir, segments)

        ordered = self._filter_valid_segments(series_dir, segments)
        groups: List[List[dict]] = []
        for seg in ordered:
            if not groups:
                groups.append([seg])
                continue
            last = groups[-1][-1]
            if int(seg["start_ms"]) <= int(last["end_ms"]):
                groups[-1].append(seg)
            else:
                groups.append([seg])

        compacted: List[dict] = []
        segments_dir = series_dir / "segments"
        for group in groups:
            if len(group) == 1:
                compacted.append(group[0])
                continue

            merged_map: Dict[int, KlineRow] = {}
            group_sorted = sorted(group, key=lambda item: (int(item.get("created_at_ms", 0)), item["file"]))
            newest_created_at = max(int(item.get("created_at_ms", 0)) for item in group_sorted)
            for seg in group_sorted:
                path = segments_dir / seg["file"]
                for row in self._read_segment(path, -2**63, 2**63 - 1):
                    merged_map[row[0]] = row

            merged_rows = [merged_map[ts] for ts in sorted(merged_map)]
            if not merged_rows:
                for seg in group:
                    try:
                        (segments_dir / seg["file"]).unlink()
                    except FileNotFoundError:
                        pass
                continue

            merged_start_ms = merged_rows[0][0]
            merged_end_ms = merged_rows[-1][0] + 1
            merged_name = (
                f"{merged_start_ms}_{merged_end_ms}_{len(merged_rows)}_{newest_created_at}.rtk"
            )
            merged_temp = merged_name + ".part"
            self._write_segment(segments_dir / merged_temp, merged_rows, merged_start_ms, merged_end_ms)
            os.replace(segments_dir / merged_temp, segments_dir / merged_name)
            for seg in group:
                old_path = segments_dir / seg["file"]
                if old_path.name == merged_name:
                    continue
                try:
                    old_path.unlink()
                except FileNotFoundError:
                    pass
            compacted.append({
                "file": merged_name,
                "start_ms": merged_start_ms,
                "end_ms": merged_end_ms,
                "rows": len(merged_rows),
                "created_at_ms": newest_created_at,
            })

        compacted.sort(key=lambda seg: (int(seg["start_ms"]), int(seg.get("created_at_ms", 0)), seg["file"]))
        return compacted

    def _effective_coverages(self, index: dict, period: str) -> List[Tuple[int, int]]:
        now_ms = self._now_ms()
        refresh_ms = self._period_ms(period) * self._tail_refresh_bars
        coverages: List[Tuple[int, int]] = []
        for item in index.get("coverage", []):
            start_ms = int(item["start_ms"])
            end_ms = int(item["end_ms"])
            if end_ms <= start_ms:
                continue
            if refresh_ms > 0 and end_ms >= now_ms - refresh_ms:
                end_ms = max(start_ms, end_ms - refresh_ms)
            if end_ms > start_ms:
                coverages.append((start_ms, end_ms))
        return self._merge_ranges(coverages)

    def _merge_coverages(self, coverages: List[dict]) -> List[dict]:
        ordered = sorted(
            (
                {
                    "start_ms": int(item["start_ms"]),
                    "end_ms": int(item["end_ms"]),
                    "fetched_at_ms": int(item.get("fetched_at_ms", 0)),
                }
                for item in coverages
                if int(item["end_ms"]) > int(item["start_ms"])
            ),
            key=lambda item: (item["start_ms"], item["end_ms"]),
        )
        if not ordered:
            return []

        merged = [ordered[0]]
        for item in ordered[1:]:
            last = merged[-1]
            if item["start_ms"] <= last["end_ms"]:
                last["end_ms"] = max(last["end_ms"], item["end_ms"])
                last["fetched_at_ms"] = max(last["fetched_at_ms"], item["fetched_at_ms"])
            else:
                merged.append(item)
        return merged

    def _difference(self, requested: List[Tuple[int, int]],
                    covered: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
        missing: List[Tuple[int, int]] = []
        covered = self._merge_ranges(covered)
        for req_start, req_end in requested:
            cursor = req_start
            for cov_start, cov_end in covered:
                if cov_end <= cursor:
                    continue
                if cov_start >= req_end:
                    break
                if cov_start > cursor:
                    missing.append((cursor, min(cov_start, req_end)))
                cursor = max(cursor, cov_end)
                if cursor >= req_end:
                    break
            if cursor < req_end:
                missing.append((cursor, req_end))
        return [(start, end) for start, end in missing if end > start]

    def _merge_ranges(self, ranges: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
        ordered = sorted((int(start), int(end)) for start, end in ranges if int(end) > int(start))
        if not ordered:
            return []
        merged = [ordered[0]]
        for start, end in ordered[1:]:
            last_start, last_end = merged[-1]
            if start <= last_end:
                merged[-1] = (last_start, max(last_end, end))
            else:
                merged.append((start, end))
        return merged

    def _normalize_rows(self, klines: Iterable[KlineRow]) -> List[KlineRow]:
        rows = {
            int(row[0]): (
                int(row[0]),
                float(row[1]),
                float(row[2]),
                float(row[3]),
                float(row[4]),
                int(row[5]),
                float(row[6]),
                int(row[7]),
            )
            for row in klines
        }
        return [rows[ts] for ts in sorted(rows)]

    def _period_ms(self, period: str) -> int:
        return PERIOD_MS.get(period, 60_000)

    def _now_ms(self) -> int:
        return int(round(__import__("time").time() * 1000))
