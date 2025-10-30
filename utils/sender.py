from aiogram import Bot


async def send_achievement_award(bot: Bot, chat_id: int, text: str):
    await bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode="HTML",
        disable_web_page_preview=True,
    )
