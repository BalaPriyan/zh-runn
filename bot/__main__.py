from asyncio import create_subprocess_exec, gather
from os import execl as osexecl
from signal import SIGINT, signal
from sys import executable
from time import time, monotonic
from uuid import uuid4

from aiofiles import open as aiopen
from aiofiles.os import path as aiopath
from aiofiles.os import remove as aioremove
from psutil import (boot_time, cpu_count, cpu_percent, cpu_freq, disk_usage,
                    net_io_counters, swap_memory, virtual_memory)
from pyrogram.filters import command
from pyrogram.handlers import MessageHandler

from bot import (DATABASE_URL, INCOMPLETE_TASK_NOTIFIER, LOGGER,
                 STOP_DUPLICATE_TASKS, Interval, QbInterval, bot, botStartTime,
                 config_dict, scheduler, user_data)
from bot.helper.listeners.aria2_listener import start_aria2_listener

from .helper.ext_utils.bot_utils import (cmd_exec, get_readable_file_size,
                                         get_readable_time, new_thread, set_commands,
                                         sync_to_async, get_progress_bar_string)
from .helper.ext_utils.db_handler import DbManger
from .helper.ext_utils.fs_utils import clean_all, exit_clean_up, start_cleanup
from .helper.telegram_helper.bot_commands import BotCommands
from .helper.telegram_helper.filters import CustomFilters
from .helper.telegram_helper.message_utils import (editMessage, sendFile,
                                                   sendMessage, auto_delete_message)
from .modules import (anonymous, authorize, bot_settings, cancel_mirror,
                      category_select, clone, eval, gd_count, gd_delete,
                      gd_list, leech_del, mirror_leech, rmdb, rss,
                      shell, status, torrent_search,
                      torrent_select, users_settings, ytdlp)
from .helper.themes import BotTheme

@new_thread
async def stats(_, message):
    if await aiopath.exists('.git'):
        last_commit = (await cmd_exec("git log -1 --date=short --pretty=format:'%cr'", True))[0]
        version = (await cmd_exec("git describe --abbrev=0 --tags", True))[0]
        change_log = (await cmd_exec("git log -1 --pretty=format:'%s'", True))[0]
    else:
        last_commit = 'No UPSTREAM_REPO'
        version = 'N/A'
        change_log = 'N/A'

    sysTime = get_readable_time(time() - boot_time())
    botTime = get_readable_time(time() - botStartTime)
    remaining_time = 86400 - (time() - botStartTime)
    res_time = '⚠️ Soon ⚠️' if remaining_time <= 0 else get_readable_time(remaining_time)
    total, used, free, disk= disk_usage('/')
    total = get_readable_file_size(total)
    used = get_readable_file_size(used)
    free = get_readable_file_size(free)
    sent = get_readable_file_size(net_io_counters().bytes_sent)
    recv = get_readable_file_size(net_io_counters().bytes_recv)
    cpuUsage = cpu_percent(interval=1)
    v_core = cpu_count(logical=True) - cpu_count(logical=False)
    memory = virtual_memory()
    swap = swap_memory()
    mem_p = memory.percent

    DIR = 'Unlimited' if config_dict['DIRECT_LIMIT'] == '' else config_dict['DIRECT_LIMIT']
    YTD = 'Unlimited' if config_dict['YTDLP_LIMIT'] == '' else config_dict['YTDLP_LIMIT']
    GDL = 'Unlimited' if config_dict['GDRIVE_LIMIT'] == '' else config_dict['GDRIVE_LIMIT']
    TOR = 'Unlimited' if config_dict['TORRENT_LIMIT'] == '' else config_dict['TORRENT_LIMIT']
    CLL = 'Unlimited' if config_dict['CLONE_LIMIT'] == '' else config_dict['CLONE_LIMIT']
    MGA = 'Unlimited' if config_dict['MEGA_LIMIT'] == '' else config_dict['MEGA_LIMIT']
    TGL = 'Unlimited' if config_dict['LEECH_LIMIT'] == '' else config_dict['LEECH_LIMIT']
    UMT = 'Unlimited' if config_dict['USER_MAX_TASKS'] == '' else config_dict['USER_MAX_TASKS']
    BMT = 'Unlimited' if config_dict['QUEUE_ALL'] == '' else config_dict['QUEUE_ALL']

    stats = BotTheme('STATS',
                     last_commit=last_commit,
                     bot_version=get_version(),
                     commit_details=changelog,
                     bot_uptime=get_readable_time(time() - botStartTime),
                     os_uptime=get_readable_time(time() - boot_time()),
                     os_arch=f"{platform.system()}, {platform.release()}, {platform.machine()}",
                     cpu=cpuUsage,
                     cpu_bar=get_progress_bar_string(cpuUsage),
                     cpu_freq=f"{cpu_freq(percpu=False).current / 1000:.2f} GHz" if cpu_freq() else "Access Denied",
                     p_core=cpu_count(logical=False),
                     v_core=cpu_count(logical=True) - cpu_count(logical=False),
                     total_core=cpu_count(logical=True),
                     ram_bar=get_progress_bar_string(memory.percent),
                     ram=memory.percent,
                     ram_u=get_readable_file_size(memory.used),
                     ram_f=get_readable_file_size(memory.available),
                     ram_t=get_readable_file_size(memory.total),
                     swap_bar=get_progress_bar_string(swap.percent),
                     swap=swap.percent,
                     swap_u=get_readable_file_size(swap.used),
                     swap_f=get_readable_file_size(swap.free),
                     swap_t=get_readable_file_size(swap.total),
                     disk=disk,
                     disk_bar=get_progress_bar_string(disk),
                     disk_t=get_readable_file_size(total),
                     disk_u=get_readable_file_size(used),
                     disk_f=get_readable_file_size(free),
                     up_data=get_readable_file_size(
                         net_io_counters().bytes_sent),
                     dl_data=get_readable_file_size(
                         net_io_counters().bytes_recv)
                     )
    await sendMessage(message, stats, photo='IMAGES')
    reply_message = await sendMessage(message, stats)
    await auto_delete_message(message, reply_message)


async def start(_, message):
    if len(message.command) > 1:
        userid = message.from_user.id
        input_token = message.command[1]
        if userid not in user_data:
            return await sendMessage(message, 'This token is not yours!\n\nKindly generate your own.')
        data = user_data[userid]
        if 'token' not in data or data['token'] != input_token:
            return await sendMessage(message, 'Token already used!\n\nKindly generate a new one.')
        data['token'] = str(uuid4())
        data['time'] = time()
        user_data[userid].update(data)
        msg = 'Token refreshed successfully!\n\n'
        msg += f'Validity: {get_readable_time(int(config_dict["TOKEN_TIMEOUT"]))}'
        return await sendMessage(message, msg)
    elif config_dict['DM_MODE']:
        start_string = 'Bot Started.\n' \
                       'Now I can send your stuff here.\n' \
                       'Use me here: @Z_Mirror'
    else:
        start_string = 'Sorry, you cant use me here!\n' \
                       'Join @Z_Mirror to use me.\n' \
                       'Thank You'
    await sendMessage(message, start_string)


async def restart(_, message):
    restart_message = await sendMessage(message, "Restarting...")
    if scheduler.running:
        scheduler.shutdown(wait=False)
    for interval in [QbInterval, Interval]:
        if interval:
            interval[0].cancel()
    await sync_to_async(clean_all)
    proc1 = await create_subprocess_exec('pkill', '-9', '-f', '-e', 'gunicorn|buffet|openstack|render|zcl')
    proc2 = await create_subprocess_exec('python3', 'update.py')
    await gather(proc1.wait(), proc2.wait())
    async with aiopen(".restartmsg", "w") as f:
        await f.write(f"{restart_message.chat.id}\n{restart_message.id}\n")
    osexecl(executable, executable, "-m", "bot")

@new_thread
async def ping(_, message):
    start_time = monotonic()
    reply = await sendMessage(message, "Starting Ping")
    end_time = monotonic()
    ping_time = int((end_time - start_time) * 1000)
    await editMessage(reply, f'{ping_time} ms')

async def log(_, message):
    await sendFile(message, 'Z_Logs.txt')

help_string = f'''<b><i>㊂ Help Guide :</i></b>

- <b>NOTE: <i>Click on any CMD to see more minor detalis.</i></b>

╭─<b>Use Mirror commands to download your link/file/rcl</b>
╰  /{BotCommands.MirrorCommand[0]} or /{BotCommands.MirrorCommand[1]}: Download via file/url/media to Upload to Cloud Drive.

╭─<b>Use qBit commands for torrents only:</b>
├ /{BotCommands.QbMirrorCommand[0]} or /{BotCommands.QbMirrorCommand[1]}: Download using qBittorrent and Upload to Cloud Drive.
╰  /{BotCommands.BtSelectCommand}: Select files from torrents by btsel_gid or reply.

╭─<b>Use yt-dlp commands for YouTube or any supported sites:</b>
╰  /{BotCommands.YtdlCommand[0]} or /{BotCommands.YtdlCommand[1]}: Mirror yt-dlp supported link.

╭─<b>Use Leech commands for upload to Telegram:</b>
├ /{BotCommands.LeechCommand[0]} or /{BotCommands.LeechCommand[1]}: Upload to Telegram.
├ /{BotCommands.QbLeechCommand[0]} or /{BotCommands.QbLeechCommand[1]}: Download using qBittorrent and upload to Telegram(For torrents only).
╰ /{BotCommands.YtdlLeechCommand[0]} or /{BotCommands.YtdlLeechCommand[1]}: Download using Yt-Dlp(supported link) and upload to telegram.

╭─<b>G-Drive commands:</b>
├ /{BotCommands.CloneCommand[0]}: Copy file/folder to Cloud Drive.
├ /{BotCommands.CountCommand} [drive_url]: Count file/folder of Google Drive.
├ /{BotCommands.DeleteCommand} [drive_url]: Delete file/folder from Google Drive (Only Owner & Sudo).
╰ /{BotCommands.GDCleanCommand[0]} or /{BotCommands.GDCleanCommand[1]} [drive_id]: Delete all files from specific folder in Google Drive.

╭─<b>Cancel Tasks:</b>
├ /{BotCommands.CancelMirror}: Cancel task by cancel_gid or reply.
╰ /{BotCommands.CancelAllCommand[0]}: Cancel all Tasks & /{BotCommands.CancelAllCommand[1]} for Multiple Bots.

╭─<b>Torrent/Drive Search:</b>
├ /{BotCommands.ListCommand} [query]: Search in Google Drive(s).
╰ /{BotCommands.SearchCommand} [query]: Search for torrents with API.

╭─<b>Bot Settings:</b>
├ /{BotCommands.UserSetCommand[0]} or /{BotCommands.UserSetCommand[1]} [query]: Open User Settings (PM also)
├ /{BotCommands.UsersCommand}: Show User Stats Info (Only Owner & Sudo).
╰ /{BotCommands.BotSetCommand[0]} or /{BotCommands.BotSetCommand[0]} [query]: Open Bot Settings (Only Owner & Sudo).

╭─<b>Authentication:</b>
├ /login: Login to Bot to Access Bot without Temp Pass System (Private)
├ /{BotCommands.AuthorizeCommand[0]} or /{BotCommands.AuthorizeCommand[1]}: Authorize a chat or a user to use the bot (Only Owner & Sudo).
├ /{BotCommands.UnAuthorizeCommand[0]} or /{BotCommands.UnAuthorizeCommand[1]}: Unauthorize a chat or a user to use the bot (Only Owner & Sudo).
├ /{BotCommands.AddSudoCommand}: Add sudo user (Only Owner).
╰ /{BotCommands.RmSudoCommand}: Remove sudo users (Only Owner).

╭─<b>Bot Stats:</b>
├ /{BotCommands.BroadcastCommand[0]} or /{BotCommands.BroadcastCommand[1]} [reply_msg]: Broadcast to PM users who have started the bot anytime.
├ /{BotCommands.StatusCommand[0]} or /{BotCommands.StatusCommand[1]}: Shows a status page of all active tasks.
├ /{BotCommands.StatsCommand[0]} or /{BotCommands.StatsCommand[1]}: Show Server detailed stats.
╰ /{BotCommands.PingCommand[0]} or /{BotCommands.PingCommand[1]}: Check how long it takes to Ping the Bot.

╭─<b>Maintainance:</b>
├ /{BotCommands.RestartCommand[0]} or /{BotCommands.RestartCommand[1]}: Restart and Update the Bot (Only Owner & Sudo).
├ /{BotCommands.RestartCommand[2]}: Restart and Update all Bots (Only Owner & Sudo).
╰ /{BotCommands.LogCommand}: Get a log file of the bot. Handy for getting crash reports (Only Owner & Sudo).

╭─<b>Extras:</b>
├ /{BotCommands.ShellCommand}: Run shell commands (Only Owner).
├ /{BotCommands.EvalCommand}: Run Python Code Line | Lines (Only Owner).
├ /{BotCommands.ExecCommand}: Run Commands In Exec (Only Owner).
╰ /{BotCommands.ClearLocalsCommand}: Clear {BotCommands.EvalCommand} or {BotCommands.ExecCommand} locals (Only Owner).

╭─<b>RSS Feed:</b>
╰ /{BotCommands.RssCommand}: Open RSS Menu.

- <b>Attention: Read the first line again!</b>
'''

@new_thread
async def bot_help(_, message):
    reply_message = await sendMessage(message, help_string)
    await auto_delete_message(message, reply_message)


async def restart_notification():
    now=datetime.now(timezone(config_dict['TIMEZONE']))
    if await aiopath.isfile(".restartmsg"):
        with open(".restartmsg") as f:
            chat_id, msg_id = map(int, f)
    else:
        chat_id, msg_id = 0, 0

    async def send_incompelete_task_message(cid, msg):
        try:
            if msg.startswith('Restarted Successfully!'):
                await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text='Restarted Successfully!')
                await bot.send_message(chat_id, msg, disable_web_page_preview=True, reply_to_message_id=msg_id)
                await aioremove(".restartmsg")
            else:
                await bot.send_message(chat_id=cid, text=msg, disable_web_page_preview=True,
                                       disable_notification=True)
        except Exception as e:
            LOGGER.error(e)
    if DATABASE_URL:
        if INCOMPLETE_TASK_NOTIFIER and (notifier_dict := await DbManger().get_incomplete_tasks()):
            for cid, data in notifier_dict.items():
                msg = 'Restarted Successfully!' if cid == chat_id else 'Bot Restarted!'
                for tag, links in data.items():
                    msg += f"\n\n👤 {tag} Do your tasks again. \n"
                    for index, link in enumerate(links, start=1):
                        msg += f" {index}: {link} \n"
                        if len(msg.encode()) > 4000:
                            await send_incompelete_task_message(cid, msg)
                            msg = ''
                if msg:
                    await send_incompelete_task_message(cid, msg)

        if STOP_DUPLICATE_TASKS:
            await DbManger().clear_download_links()


    if await aiopath.isfile(".restartmsg"):
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text='Restarted Successfully!')
        except:
            pass
        await aioremove(".restartmsg")


async def main():
    await gather(start_cleanup(), torrent_search.initiate_search_tools(), restart_notification(), set_commands(bot))
    await sync_to_async(start_aria2_listener, wait=False)

    bot.add_handler(MessageHandler(
        start, filters=command(BotCommands.StartCommand)))
    bot.add_handler(MessageHandler(log, filters=command(
        BotCommands.LogCommand) & CustomFilters.sudo))
    bot.add_handler(MessageHandler(restart, filters=command(
        BotCommands.RestartCommand) & CustomFilters.sudo))
    bot.add_handler(MessageHandler(ping, filters=command(
        BotCommands.PingCommand) & CustomFilters.authorized))
    bot.add_handler(MessageHandler(bot_help, filters=command(
        BotCommands.HelpCommand) & CustomFilters.authorized))
    bot.add_handler(MessageHandler(stats, filters=command(
        BotCommands.StatsCommand) & CustomFilters.authorized))
    LOGGER.info("Spidy Parker Bot Started Successfully!")
    signal(SIGINT, exit_clean_up)

bot.loop.run_until_complete(main())
bot.loop.run_forever()
