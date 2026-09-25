"""接收边界统一的 UTC 时间校验、规范化与比较。"""
from __future__ import annotations
from datetime import datetime, timezone

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

def parse_utc(value: str, field: str = "observed_at") -> datetime:
    """解析 ISO 8601 时间（接受 Z 或任意偏移；缺省时区按 UTC），返回 UTC 时刻。"""
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} 必须是 ISO 8601 时间") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)

def utc_text(value: datetime) -> str:
    """UTC 规范文本（Z 结尾），同一绝对时刻的表示唯一。"""
    if value.tzinfo is None:
        raise ValueError("时间必须带时区")
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def canonical_time(value: str, field: str = "observed_at") -> str:
    """把任意接受的表示形式归一为 UTC 规范文本。"""
    return utc_text(parse_utc(value, field))

def epoch_micros(value: str) -> int:
    """绝对时刻的微秒时间戳；排序、窗口边界和分页游标据此比较，与表示形式无关。"""
    delta = parse_utc(value) - _EPOCH
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
