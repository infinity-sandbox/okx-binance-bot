import asyncio

from loguru import logger
from telethon import TelegramClient, events

import helpers


def send_telegram_message(msg: str):
    config = helpers.load_config_from_yaml()
    session = "telegram_bot.session"
    api_id = config["telegram_api_id"]
    api_hash = config["telegram_api_hash"]
    bot_token = config["telegram_bot_token"]
    #channel_id = int(config["telegram_ch_id"])

    #client = TelegramClient(session, api_id, api_hash).start(bot_token=bot_token)

    #with client as bot:
    #    bot.send_message(entity=channel_id, message=msg)


async def main():
    SESSION = "telegram_user.session"
    API_ID = init_config["telegram_api_id"]
    API_HASH = init_config["telegram_api_hash"]

    client = TelegramClient(SESSION, API_ID, API_HASH)

    async def start_bot():
        await client.start()
        logger.success("Telegram bot is running!")

    STATUS_COMMAND = '!status' # Change this to your desired bot command
    def get_scripts_status():
        config = helpers.load_config_from_yaml()

        scripts_to_check = config["scripts_to_check"]
        results_as_dict = {}
        for script in scripts_to_check:
            is_script_running = helpers.is_command_running(command_to_find=script)
            results_as_dict[script] = is_script_running
        
        results_as_msg = ""
        for script, state in results_as_dict.items():
            if state is True:
                results_as_msg += script + f": ✅\n"
            else:
                results_as_msg += script + f": ❌\n"

        results_as_msg = results_as_msg.strip()
        return results_as_msg
    

    @client.on(events.NewMessage)
    async def handle_command(event):
        # Check if the message is from an allowed channel
        try:
            if event.chat and event.chat.title in ALLOWED_CHANNELS:
                command = event.raw_text.split(' ')[0]
                if command == STATUS_COMMAND:
                    scripts_status_msg = get_scripts_status()
                    await event.reply(f"Scripts status:\n{scripts_status_msg}")
        except AttributeError as e:
            # dirty fix
            pass

    await start_bot()
    await client.run_until_disconnected()

if __name__ == '__main__':
    ALLOWED_CHANNELS = ["Binance Leaderboard Bot"]
    init_config = helpers.load_config_from_yaml()
    asyncio.run(main())
