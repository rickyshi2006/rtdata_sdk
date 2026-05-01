"""rtdata SDK 服务发现模块

通过 HTTPS API 获取 TCP 连接地址和 Symbol Map，
零外部依赖（仅使用 urllib.request）。
"""
import json
import ssl
import logging
from typing import Optional, Tuple, Dict
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

from .exceptions import DiscoveryError

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 10
_UA = 'rtdata-sdk/0.1'


def _make_ssl_context():
    """创建兼容 Cloudflare 的 SSL context"""
    ctx = ssl.create_default_context()
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    return ctx


def _urlopen_with_retry(req, timeout, max_retries=2):
    """带 SSL 重试的 urlopen"""
    ctx = _make_ssl_context()
    last_err = None
    for i in range(max_retries + 1):
        try:
            return urlopen(req, timeout=timeout, context=ctx)
        except URLError as e:
            if isinstance(e.reason, ssl.SSLError) and i < max_retries:
                logger.debug(f"SSL error (attempt {i+1}), retrying...")
                last_err = e
                continue
            raise
    raise last_err


def discover_endpoint(api_url: str, token: str,
                      timeout: float = DEFAULT_TIMEOUT) -> dict:
    """POST /api/v1/connect — 获取 TCP 地址和 symbol_map 版本

    返回: {"tcp_host": str, "tcp_port": int,
           "symbol_map_version": int, "symbol_count": int}
    """
    url = api_url.rstrip('/') + '/api/v1/connect'
    body = json.dumps({"token": token}).encode('utf-8')
    req = Request(url, data=body, method='POST',
                  headers={'Content-Type': 'application/json',
                           'User-Agent': _UA})
    try:
        with _urlopen_with_retry(req, timeout) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            logger.debug(f"Discovery response received: "
                        f"version={data.get('symbol_map_version')}, "
                        f"symbols={data.get('symbol_count')}")
            return data
    except HTTPError as e:
        body_text = ''
        try:
            body_text = e.read().decode('utf-8', errors='replace')
        except Exception:
            pass
        if e.code == 401:
            raise DiscoveryError(f"Authentication failed: {body_text}") from e
        raise DiscoveryError(
            f"Discovery request failed (HTTP {e.code}): {body_text}") from e
    except URLError as e:
        raise DiscoveryError(f"Cannot reach discovery API: {e.reason}") from e
    except Exception as e:
        raise DiscoveryError(f"Discovery error: {e}") from e


def fetch_symbol_map(api_url: str, token: str,
                     local_version: Optional[int] = None,
                     timeout: float = DEFAULT_TIMEOUT
                     ) -> Tuple[Optional[Dict[int, str]], int]:
    """GET /api/v1/symbol_map — 获取品种映射表（支持条件下载）

    返回: (symbols_dict, version)
        - 如果版本未变（304）: (None, local_version)
        - 如果有新版本: ({id: code, ...}, new_version)
    """
    url = api_url.rstrip('/') + '/api/v1/symbol_map'
    if local_version is not None:
        url += f'?version={local_version}'

    req = Request(url, method='GET',
                  headers={'Authorization': f'Bearer {token}',
                           'User-Agent': _UA})
    try:
        with _urlopen_with_retry(req, timeout) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            version = data.get('version', 0)
            raw_symbols = data.get('symbols', {})
            symbols = {int(k): v for k, v in raw_symbols.items()}
            logger.debug(f"Fetched symbol map: version={version}, "
                        f"count={len(symbols)}")
            return symbols, version
    except HTTPError as e:
        if e.code == 304:
            logger.info(f"Symbol map unchanged (version={local_version})")
            return None, local_version or 0
        body_text = ''
        try:
            body_text = e.read().decode('utf-8', errors='replace')
        except Exception:
            pass
        raise DiscoveryError(
            f"Symbol map fetch failed (HTTP {e.code}): {body_text}") from e
    except URLError as e:
        raise DiscoveryError(
            f"Cannot reach symbol map API: {e.reason}") from e
    except Exception as e:
        raise DiscoveryError(f"Symbol map fetch error: {e}") from e
