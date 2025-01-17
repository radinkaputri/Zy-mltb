from html import escape
from psutil import virtual_memory, cpu_percent, disk_usage
from time import time
from asyncio import iscoroutinefunction

from ... import (
    task_dict,
    task_dict_lock,
    bot_start_time,
    status_dict,
)
from ...core.config_manager import Config
from .bot_utils import sync_to_async
from ..telegram_helper.button_build import ButtonMaker

SIZE_UNITS = ["B", "KB", "MB", "GB", "TB", "PB"]


class MirrorStatus:
    STATUS_UPLOAD = "Upload"
    STATUS_DOWNLOAD = "Download"
    STATUS_CLONE = "Clone"
    STATUS_QUEUEDL = "QueueDl"
    STATUS_QUEUEUP = "QueueUp"
    STATUS_PAUSED = "Pause"
    STATUS_ARCHIVE = "Archive"
    STATUS_EXTRACT = "Extract"
    STATUS_SPLIT = "Split"
    STATUS_CHECK = "CheckUp"
    STATUS_SEED = "Seed"
    STATUS_SAMVID = "SamVid"
    STATUS_CONVERT = "Convert"
    STATUS_FFMPEG = "FFmpeg"


STATUSES = {
    "ALL": "All",
    "DL": MirrorStatus.STATUS_DOWNLOAD,
    "UP": MirrorStatus.STATUS_UPLOAD,
    "QD": MirrorStatus.STATUS_QUEUEDL,
    "QU": MirrorStatus.STATUS_QUEUEUP,
    "AR": MirrorStatus.STATUS_ARCHIVE,
    "EX": MirrorStatus.STATUS_EXTRACT,
    "SD": MirrorStatus.STATUS_SEED,
    "CL": MirrorStatus.STATUS_CLONE,
    "CM": MirrorStatus.STATUS_CONVERT,
    "SP": MirrorStatus.STATUS_SPLIT,
    "SV": MirrorStatus.STATUS_SAMVID,
    "FF": MirrorStatus.STATUS_FFMPEG,
    "PA": MirrorStatus.STATUS_PAUSED,
    "CK": MirrorStatus.STATUS_CHECK,
}


async def get_task_by_gid(gid: str):
    gid = gid[:8]
    async with task_dict_lock:
        for tk in task_dict.values():
            if hasattr(tk, "seeding"):
                await sync_to_async(tk.update)
            if tk.gid()[:8] == gid:
                return tk
        return None


def get_specific_tasks(status, user_id):
    if status == "All":
        if user_id:
            return [tk for tk in task_dict.values() if tk.listener.user_id == user_id]
        else:
            return list(task_dict.values())
    elif user_id:
        return [
            tk
            for tk in task_dict.values()
            if tk.listener.user_id == user_id
            and (
                (st := tk.status())
                and st == status
                or status == MirrorStatus.STATUS_DOWNLOAD
                and st not in STATUSES.values()
            )
        ]
    else:
        return [
            tk
            for tk in task_dict.values()
            if (st := tk.status())
            and st == status
            or status == MirrorStatus.STATUS_DOWNLOAD
            and st not in STATUSES.values()
        ]


async def get_all_tasks(req_status: str, user_id):
    async with task_dict_lock:
        return await sync_to_async(get_specific_tasks, req_status, user_id)


def get_readable_file_size(size_in_bytes):
    if not size_in_bytes:
        return "0B"

    index = 0
    while size_in_bytes >= 1024 and index < len(SIZE_UNITS) - 1:
        size_in_bytes /= 1024
        index += 1

    return f"{size_in_bytes:.2f}{SIZE_UNITS[index]}"


def get_readable_time(seconds: int):
    periods = [("d", 86400), ("h", 3600), ("m", 60), ("s", 1)]
    result = ""
    for period_name, period_seconds in periods:
        if seconds >= period_seconds:
            period_value, seconds = divmod(seconds, period_seconds)
            result += f"{int(period_value)}{period_name}"
    return result


def time_to_seconds(time_duration):
    try:
        parts = time_duration.split(":")
        if len(parts) == 3:
            hours, minutes, seconds = map(float, parts)
        elif len(parts) == 2:
            hours = 0
            minutes, seconds = map(float, parts)
        elif len(parts) == 1:
            hours = 0
            minutes = 0
            seconds = float(parts[0])
        else:
            return 0
        return hours * 3600 + minutes * 60 + seconds
    except:
        return 0


def speed_string_to_bytes(size_text: str):
    size = 0
    size_text = size_text.lower()
    if "k" in size_text:
        size += float(size_text.split("k")[0]) * 1024
    elif "m" in size_text:
        size += float(size_text.split("m")[0]) * 1048576
    elif "g" in size_text:
        size += float(size_text.split("g")[0]) * 1073741824
    elif "t" in size_text:
        size += float(size_text.split("t")[0]) * 1099511627776
    elif "b" in size_text:
        size += float(size_text.split("b")[0])
    return size


def get_progress_bar_string(pct):
    pct = float(pct.strip("%"))
    p = min(max(pct, 0), 100)
    cFull = int(p // 8)
    p_str = "■" * cFull
    p_str += "□" * (12 - cFull)
    return f"[{p_str}]"


async def get_readable_message(sid, is_user, page_no=1, status="All", page_step=1):
    STATUS_LIMIT = Config.STATUS_LIMIT
    tasks = await sync_to_async(get_specific_tasks, status, sid if is_user else None)
    tasks_no = len(tasks)
    pages = max(1, -(-tasks_no // STATUS_LIMIT))  # Ceiling division
    page_no = max(1, (page_no - 1) % pages + 1)
    start_position = (page_no - 1) * STATUS_LIMIT
    msg = ""
    
    for index, task in enumerate(tasks[start_position : start_position + STATUS_LIMIT], start=1):
        tstatus = await sync_to_async(task.status) if status == "All" else status
        task_gid = task.gid()[:8]
        cancel_task = f"<b>/cancel_{task_gid}</b>" if "-" not in task_gid else f"<code>/cancel {task_gid}</code>"
        msg += (
            f"<b>{index + start_position}.{'<a href=\"' + task.listener.message.link + '\">' if task.listener.is_super_chat else ''}{tstatus}{'</a>' if task.listener.is_super_chat else ''}: </b>"
            f"<code>{escape(task.name())}</code>"
        )
        if task.listener.subname:
            msg += f"\n<i>{task.listener.subname}</i>"

        if tstatus not in [MirrorStatus.STATUS_SEED, MirrorStatus.STATUS_QUEUEUP] and task.listener.progress:
            progress = (
                await task.progress() if iscoroutinefunction(task.progress) else task.progress()
            )
            subsize = f"/{get_readable_file_size(task.listener.subsize)}" if task.listener.subname else ""
            count = (
                f"({task.listener.proceed_count}/{len(task.listener.files_to_proceed) or '?'})"
                if task.listener.subname else ""
            )
            msg += (
                f"\n{get_progress_bar_string(progress)} {progress}"
                f"\n<b>Processed:</b> {task.processed_bytes()}{subsize} {count}"
                f"\n<b>Size:</b> {task.size()}\n<b>Speed:</b> {task.speed()}\n<b>ETA:</b> {task.eta()}"
            )
            if hasattr(task, "seeders_num"):
                msg += f"\n<b>Seeders:</b> {task.seeders_num()} | <b>Leechers:</b> {task.leechers_num()}"
        elif tstatus == MirrorStatus.STATUS_SEED:
            msg += (
                f"\n<b>Size: </b>{task.size()} | <b>Speed: </b>{task.seed_speed()} | <b>Uploaded: </b>{task.uploaded_bytes()}"
                f"\n<b>Ratio: </b>{task.ratio()} | <b>Time: </b>{task.seeding_time()}"
            )
        else:
            msg += f"\n<b>Size: </b>{task.size()}"

        msg += f"\n<blockquote>{cancel_task}</blockquote>\n\n"

    if not msg:
        return (None, None) if status == "All" else (f"No Active {status} Tasks!\n\n", None)

    buttons = ButtonMaker()
    if not is_user:
        buttons.data_button("☰", f"status {sid} ov", position="header")
    if tasks_no > STATUS_LIMIT:
        msg += f"<b>Page:</b> {page_no}/{pages} | <b>Tasks:</b> {tasks_no} | <b>Step:</b> {page_step}\n"
        buttons.data_button("<<", f"status {sid} pre", position="header")
        buttons.data_button(">>", f"status {sid} nex", position="header")
        if tasks_no > 30:
            for i in [1, 2, 4, 6, 8, 10, 15]:
                buttons.data_button(i, f"status {sid} ps {i}", position="footer")
    if status != "All" or tasks_no > 20:
        for label, status_value in STATUSES.items():
            if status_value != status:
                buttons.data_button(label, f"status {sid} st {status_value}")
    buttons.data_button("♻️", f"status {sid} ref", position="header")
    msg += (
        f"<code>CPU:</code> {cpu_percent()}% | <code>FREE:</code> {get_readable_file_size(disk_usage(Config.DOWNLOAD_DIR).free)}"
        f"\n<code>RAM:</code> {virtual_memory().percent}% | <code>UPTM:</code> {get_readable_time(time() - bot_start_time)}"
    )
    return msg, buttons.build_menu(8)