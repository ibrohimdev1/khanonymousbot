import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from config import settings
from handlers import start, language, anonymous, settings as settings_handlers, admin


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("khanonymous-bot")


async def main() -> None:
    bot = Bot(
        token=settings.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()

    # Routers
    dp.include_router(start.router)
    dp.include_router(language.router)
    dp.include_router(anonymous.router)
    dp.include_router(settings_handlers.router)
    dp.include_router(admin.router)  # Admin router oxirida, chunki u F.text ni qayta ishlaydi

    logger.info("khanonymous bot started (polling)")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())


