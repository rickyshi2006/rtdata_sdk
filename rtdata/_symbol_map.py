"""rtdata SDK 品种映射管理

解析 SYMBOL_MAP 消息，维护 symbol_id ↔ code 双向映射，
支持本地磁盘缓存。
"""
import json
import os
import threading
import logging
from pathlib import Path
from typing import Optional, Dict, List

from . import _protocol as proto

logger = logging.getLogger(__name__)


class SymbolMap:

    def __init__(self, cache_dir: Optional[str] = None):
        self._lock = threading.Lock()
        self._id_to_code: Dict[int, str] = {}
        self._code_to_id: Dict[str, int] = {}
        self._version: int = 0

        if cache_dir:
            self._cache_path = Path(cache_dir) / 'symbol_map.json'
        else:
            self._cache_path = Path.home() / '.rtdata' / 'symbol_map.json'

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._id_to_code)

    @property
    def version(self) -> int:
        with self._lock:
            return self._version

    def update_from_dict(self, mapping: Dict[int, str], version: int = 0) -> int:
        """从字典更新映射（用于 HTTP API 下载的 symbol map），返回品种数量"""
        with self._lock:
            self._id_to_code = mapping
            self._code_to_id = {code: sid for sid, code in mapping.items()}
            if version > 0:
                self._version = version
        self._save_cache()
        logger.info(f"Symbol map updated from dict: {len(mapping)} symbols, version={version}")
        return len(mapping)

    def update_from_payload(self, payload: bytes) -> int:
        """解析 SYMBOL_MAP payload，更新映射，返回品种数量"""
        mapping = proto.decode_symbol_map(payload)
        if not mapping:
            logger.warning("Received empty symbol map payload, keeping existing map")
            return self.size
        with self._lock:
            self._id_to_code = mapping
            self._code_to_id = {code: sid for sid, code in mapping.items()}
        self._save_cache()
        logger.info(f"Symbol map updated: {len(mapping)} symbols")
        return len(mapping)

    def id_to_code(self, symbol_id: int) -> Optional[str]:
        with self._lock:
            return self._id_to_code.get(symbol_id)

    def code_to_id(self, code: str) -> Optional[int]:
        with self._lock:
            return self._code_to_id.get(code)

    def codes_to_ids(self, codes: List[str]) -> List[int]:
        """批量转换，未知品种跳过并记录警告"""
        ids = []
        with self._lock:
            for code in codes:
                sid = self._code_to_id.get(code)
                if sid is not None:
                    ids.append(sid)
                else:
                    logger.warning(f"Symbol not found: {code}")
        return ids

    def get_all_codes(self) -> List[str]:
        with self._lock:
            return list(self._code_to_id.keys())

    def load_cache(self) -> bool:
        """从磁盘加载缓存，成功返回 True"""
        try:
            if not self._cache_path.exists():
                return False
            with open(self._cache_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            # 兼容新旧格式：新格式 {"version": N, "symbols": {...}}，旧格式 {"1": "code", ...}
            if isinstance(data, dict) and 'symbols' in data:
                version = data.get('version', 0)
                mapping = {int(k): v for k, v in data['symbols'].items()}
            else:
                version = 0
                mapping = {int(k): v for k, v in data.items()}
            with self._lock:
                self._id_to_code = mapping
                self._code_to_id = {code: sid for sid, code in mapping.items()}
                self._version = version
            logger.info(f"Symbol map loaded from cache: {len(mapping)} symbols, version={version}")
            return True
        except Exception as e:
            logger.debug(f"Failed to load symbol map cache: {e}")
            return False

    def _save_cache(self):
        try:
            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
            with self._lock:
                data = {
                    'version': self._version,
                    'symbols': {str(k): v for k, v in self._id_to_code.items()},
                }
            with open(self._cache_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False)
        except Exception as e:
            logger.debug(f"Failed to save symbol map cache: {e}")
