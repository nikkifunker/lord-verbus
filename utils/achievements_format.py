from html import escape

BOX_TL = "┌"
BOX_TR = "┐"
BOX_BL = "└"
BOX_BR = "┘"
BOX_H  = "─"
BOX_V  = "│"


def _box(text_lines: list[str], width: int | None = None) -> str:
    # авто-ширина по самой длинной строке
    content_width = max(len(line) for line in text_lines) if text_lines else 0
    w = max(content_width, width or 0)
    top = f"{BOX_TL}{BOX_H * (w + 2)}{BOX_TR}"
    bot = f"{BOX_BL}{BOX_H * (w + 2)}{BOX_BR}"
    body = [f"{BOX_V} {line.ljust(w)} {BOX_V}" for line in text_lines]
    return "\n".join([top, *body, bot])


def format_achievement_message(
    user_id: int,
    user_name: str,
    ach_code: str,
    ach_title: str,
    level: int | None,
    description: str,
    progress: tuple[int, int] | None = None,
) -> str:
    safe_name = escape(user_name or "user")
    mention = f'<a href="tg://user?id={user_id}">{safe_name}</a>'
    title_line = f"🏆 {escape(ach_title)}"
    level_line = f"✨ Уровень: {level}" if level is not None else "✨ Новая ачивка!"
    code_line = f"🆔 Код: {escape(ach_code)}"
    desc_line = f"— {escape(description)}"
    prog_line = ""
    if progress:
        cur, total = progress
        if total:
            prog_line = f"📈 Прогресс: {cur}/{total}"
    lines = [title_line, level_line, code_line, desc_line]
    if prog_line:
        lines.append(prog_line)
    box = _box(lines)
    return f"{mention}\n{box}"
