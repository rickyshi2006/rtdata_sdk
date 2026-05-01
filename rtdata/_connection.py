"""rtdata SDK TCP 连接管理

负责 TCP socket 连接、接收循环、心跳、自动重连。
"""
import socket
import struct
import threading
import time
import random
import logging
from typing import Callable, Optional

from . import _protocol as proto
from .exceptions import ConnectionError, ProtocolError

logger = logging.getLogger(__name__)


class Connection:

    def __init__(
        self,
        host: str,
        port: int,
        on_message: Callable,
        on_disconnected: Callable,
        heartbeat_interval: float = 20.0,
        auto_reconnect: bool = True,
    ):
        self._host = host
        self._port = port
        self._on_message = on_message
        self._on_disconnected = on_disconnected
        self._heartbeat_interval = heartbeat_interval
        self._auto_reconnect = auto_reconnect

        self._sock: Optional[socket.socket] = None
        self._send_lock = threading.Lock()
        self._recv_thread: Optional[threading.Thread] = None
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._connected = False

        # 重连回调（由 Client 设置）
        self._on_reconnected: Optional[Callable] = None
        self._on_before_reconnect: Optional[Callable] = None  # 重连前（discovery 等）

        # 防止并发重连
        self._reconnect_lock = threading.Lock()
        self._reconnecting = False

    @property
    def connected(self) -> bool:
        return self._connected

    def connect(self, timeout: float = 10.0):
        """建立 TCP 连接"""
        self._stop_event.clear()
        self._do_connect(timeout)

    def _do_connect(self, timeout: float = 10.0):
        """底层连接"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            sock.connect((self._host, self._port))
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.settimeout(None)  # 接收线程用阻塞模式
            self._sock = sock
            self._connected = True
            logger.info("Connected to gateway")
        except Exception as e:
            raise ConnectionError(f"Failed to connect to {self._host}:{self._port}: {e}")

    def start_recv_loop(self):
        """启动接收线程和心跳线程"""
        self._recv_thread = threading.Thread(
            target=self._recv_loop, name='rtdata-recv', daemon=True)
        self._recv_thread.start()

        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, name='rtdata-heartbeat', daemon=True)
        self._heartbeat_thread.start()

    def send(self, data: bytes):
        """线程安全发送"""
        with self._send_lock:
            sock = self._sock
            if sock is None:
                return
            try:
                sock.sendall(data)
            except Exception as e:
                logger.debug(f"Send failed: {e}")
                self._handle_disconnect("send error")

    def close(self):
        """关闭连接和所有线程"""
        self._stop_event.set()
        self._connected = False
        self._auto_reconnect = False  # 主动关闭不重连
        sock = self._sock
        self._sock = None
        if sock:
            try:
                sock.close()
            except Exception:
                pass
        if self._recv_thread and self._recv_thread.is_alive():
            self._recv_thread.join(timeout=3)
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=3)

    # ========================================================================
    # 接收循环
    # ========================================================================

    def _recv_loop(self):
        buf = bytearray()
        while not self._stop_event.is_set():
            try:
                sock = self._sock
                if sock is None:
                    break
                # 设置超时以便检查 stop_event
                sock.settimeout(1.0)
                try:
                    chunk = sock.recv(65536)
                except socket.timeout:
                    continue
                if not chunk:
                    self._handle_disconnect("connection closed by server")
                    return

                logger.debug(f"Received {len(chunk)} bytes")
                buf.extend(chunk)

                # 解析完整消息
                while len(buf) >= proto.HEADER_SIZE:
                    payload_len, symbol_id, msg_type = proto.decode_header(
                        bytes(buf[:proto.HEADER_SIZE]))

                    logger.debug(f"Parsing message: msg_type=0x{msg_type:04x} payload_len={payload_len} buf_len={len(buf)}")

                    if payload_len > proto.MAX_PAYLOAD_SIZE:
                        logger.error(f"Invalid payload_length: {payload_len}")
                        self._handle_disconnect("protocol error")
                        return

                    total_len = proto.HEADER_SIZE + payload_len
                    if len(buf) < total_len:
                        logger.debug(f"Incomplete message: need {total_len} bytes, have {len(buf)}")
                        break  # 不完整，等更多数据

                    payload = bytes(buf[proto.HEADER_SIZE:total_len])
                    del buf[:total_len]

                    try:
                        self._on_message(msg_type, symbol_id, payload)
                    except Exception as e:
                        logger.error(f"Message handler error: {e}")

            except Exception as e:
                if not self._stop_event.is_set():
                    self._handle_disconnect(f"recv error: {e}")
                return

    # ========================================================================
    # 心跳
    # ========================================================================

    def _heartbeat_loop(self):
        while not self._stop_event.is_set():
            if self._stop_event.wait(timeout=self._heartbeat_interval):
                break  # stop_event set
            if self._connected:
                try:
                    self.send(proto.encode_heartbeat())
                except Exception as e:
                    logger.debug(f"Heartbeat send failed: {e}")

    # ========================================================================
    # 断线重连
    # ========================================================================

    def _handle_disconnect(self, reason: str):
        with self._reconnect_lock:
            if not self._connected:
                return
            self._connected = False
            # 已有重连循环在跑 → 只关 socket，不启动新的重连
            should_reconnect = (self._auto_reconnect
                                and not self._stop_event.is_set()
                                and not self._reconnecting)

        # 关闭旧 socket
        sock = self._sock
        self._sock = None
        if sock:
            try:
                sock.close()
            except Exception:
                pass

        # 通知上层
        try:
            self._on_disconnected(reason)
        except Exception:
            pass

        # 自动重连（仅当没有并发的重连循环时）
        if should_reconnect:
            self._reconnect_loop()

    def _reconnect_loop(self):
        with self._reconnect_lock:
            self._reconnecting = True

        try:
            self._do_reconnect_loop()
        finally:
            with self._reconnect_lock:
                self._reconnecting = False

    def _do_reconnect_loop(self):
        attempt = 0
        base_delay = 1.0
        max_delay = 30.0
        delay = base_delay

        while not self._stop_event.is_set():
            jitter = random.uniform(0, delay * 0.5)
            actual_delay = delay + jitter
            logger.info(f"Reconnecting in {actual_delay:.1f}s (attempt {attempt + 1})...")

            if self._stop_event.wait(timeout=actual_delay):
                break

            try:
                # 重连前回调（服务发现，更新 host:port）
                if self._on_before_reconnect:
                    try:
                        self._on_before_reconnect()
                    except Exception as e:
                        logger.warning(f"Pre-reconnect callback failed: {e}")

                self._do_connect(timeout=10.0)
                # 重新启动接收循环（必须在认证之前启动，以便接收认证响应）
                self._recv_thread = threading.Thread(
                    target=self._recv_loop, name='rtdata-recv', daemon=True)
                self._recv_thread.start()
                time.sleep(0.1)

                # 检查连接是否在启动 recv 线程后立刻断开
                if not self._connected:
                    logger.warning("Connection lost immediately after reconnect, retrying...")
                    attempt += 1
                    continue

                # 重连成功，让上层重新认证和恢复订阅
                if self._on_reconnected:
                    try:
                        self._on_reconnected()
                    except Exception as e:
                        logger.error(f"Reconnect callback failed: {e}", exc_info=True)
                        # 回调失败（auth/subscribe 未完成），关闭当前连接并重试
                        self._connected = False
                        sock = self._sock
                        self._sock = None
                        if sock:
                            try:
                                sock.close()
                            except Exception:
                                pass
                        attempt += 1
                        continue
                else:
                    logger.warning("No reconnect callback set!")

                logger.info("Reconnected successfully")
                return
            except Exception as e:
                logger.warning(f"Reconnect failed: {e}")
                attempt += 1
                delay = min(delay * 2.0, max_delay)
