"""有界 MID1 身份分块解码；不修改实时行情格式。"""

import struct

try:
    import zstandard as _zstd
except ImportError:
    _zstd = None

MESSAGE_TYPE = 0x74
HEADER = struct.Struct('<4sBBHIIQQQIIIHHQ')
CATEGORIES = ('', 'cn_equity', 'hk_equity', 'futures', 'us_equity', 'fx',
              'overseas_futures', 'crypto')


def available():
    return _zstd is not None


def decode(payload, allow_zstd=False):
    if not 64 <= len(payload) <= 65536:
        raise ValueError('market binary envelope size')
    (magic, codec, done, header, raw_size, body_size, request, generation, epoch,
     offset, next_offset, total, count, revision_size, reserved) = HEADER.unpack_from(payload)
    if (magic != b'MID1' or codec not in (0, 1) or done not in (0, 1) or header != 64
            or reserved or not generation or not 0 < raw_size <= 61440
            or body_size != len(payload) - 64 or count > 512
            or not 1 <= revision_size <= min(128, raw_size)
            or not 0 <= offset <= next_offset <= total <= 500000
            or next_offset - offset != count or bool(done) != (next_offset == total)
            or (not done and not count)):
        raise ValueError('market binary envelope')
    raw = payload[64:]
    if codec:
        if not allow_zstd or _zstd is None or raw[:4] != b'\x28\xb5\x2f\xfd':
            raise ValueError('market binary unnegotiated compression')
        try:
            params = _zstd.get_frame_parameters(raw)
            if (params.content_size != raw_size or params.window_size > 65536
                    or params.dict_id or not params.has_checksum):
                raise ValueError('market binary zstd parameters')
            raw = _zstd.ZstdDecompressor(max_window_size=65536).decompress(
                raw, max_output_size=raw_size, allow_extra_data=False)
        except _zstd.ZstdError as exc:
            raise ValueError('market binary zstd') from exc
    if len(raw) != raw_size:
        raise ValueError('market binary raw size')

    def text(at, size, limit):
        if size > limit or at + size > len(raw):
            raise ValueError('market binary text size')
        value = raw[at:at + size].decode('utf-8')
        if '\0' in value:
            raise ValueError('market binary embedded NUL')
        return value

    revision = text(0, revision_size, 128)
    entries, at = [], revision_size
    for _ in range(count):
        if len(raw) - at < 5:
            raise ValueError('market binary truncated row')
        action, sid = struct.unpack_from('<BI', raw, at)
        at += 5
        if not sid or action not in (0, 1, 2):
            raise ValueError('market binary row')
        row = dict(symbol_id=sid, action=('remove', 'add', 'reference')[action])
        if action == 1:
            if len(raw) - at < 16:
                raise ValueError('market binary truncated identity')
            category, active, source, instrument, a, b, c = struct.unpack_from('<BBIIHHH', raw, at)
            at += 16
            if category >= len(CATEGORIES) or active != 1 or not a:
                raise ValueError('market binary identity fields')
            row.update(category=CATEGORIES[category], active=True, source_market=source,
                       instrument_type=instrument)
            for key, size in (('code', a), ('permission_market', b), ('issue', c)):
                row[key] = text(at, size, 256)
                at += size
        entries.append(row)
    if at != len(raw):
        raise ValueError('market binary trailing bytes')
    return dict(version=1, request_id=request, generation=generation, identity_epoch=epoch,
                catalog_revision=revision, offset=offset, next_offset=next_offset,
                total=total, done=bool(done), entries=entries)
