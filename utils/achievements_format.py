from html import escape

try:
    from wcwidth import wcswidth as _wlen
except Exception:  # pragma: no cover - fallback when dependency is missing
    # Fallback to len() if wcwidth is unavailable so formatting still works.
    _wlen = lambda s: len(s)  # type: ignore[arg-type]


BOX_TL = "┌"
BOX_TR = "┐"
BOX_BL = "└"
BOX_BR = "┘"
BOX_H = "─"
BOX_V = "│"


def _pad(s: str, width: int) -> str:
    """Pad string with spaces up to target width, taking unicode width into account."""
    diff = width - _wlen(s)
    return s + (" " * max(0, diff))


def _box(text_lines: list[str], min_width: int | None = None) -> str:
    content_width = max((_wlen(line) for line in text_lines), default=0)
    w = max(content_width, min_width or 0)
    top = f"{BOX_TL}{BOX_H * (w + 2)}{BOX_TR}"
    bot = f"{BOX_BL}{BOX_H * (w + 2)}{BOX_BR}"
    body = [f"{BOX_V} {_pad(line, w)} {BOX_V}" for line in text_lines]
    return "\n".join([top, *body, bot])


def format_achievement_message(
    user_id: int,
    user_name: str,
    ach_title: str,
    level: int | None,
    description: str,
) -> str:
    safe_name = escape(user_name or "user")
    mention = f'<a href="tg://user?id={user_id}">{safe_name}</a>'

    title_line = f"🏆 <b>{escape(ach_title)}</b>"
    level_line = f"✨ Уровень: {level}" if level is not None else "✨ Новая ачивка!"
    desc_line = f"— {escape(description)}"

    box = _box([title_line, level_line, desc_line])
    return f"{mention}\n{box}"
