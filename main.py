

from __future__ import annotations

import asyncio
import contextlib
import csv
import math
import hashlib
import html
import io
import json
import logging
import os
import re
import secrets
import shlex
import signal
import sys
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiogram
import libsql_client
import regex
from aiohttp import web
from aiogram import Bot, Dispatcher, types as T
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import (
    TelegramAPIError,
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramRetryAfter,
)
from arq import create_pool, cron
from arq.connections import RedisSettings
from arq.worker import Worker
from croniter import croniter
from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field

LOG = logging.getLogger("marie")
UTC = timezone.utc
PREFIX = "marie:"
MAX_FILE = 1_000_000

# Explicit inventory: help never advertises an unimplemented command as working.
SECTIONS = {
    "core": "start help helpadmin helpuser about id info ping rules report menu",
    "moderation": "ban tban unban kick mute tmute unmute warn unwarn resetwarns warns warnlimit warnaction warntime softban shadowmute unshadowmute strike strikes anonban anonmute anonkick anonwarn undo case cases casetag casefilter mynote modstats adminperf bulkban bulkmute bulkwarn",
    "workflow": "duty shiftnote shiftnotes claim unclaim reports",
    "protection": "flood setflood floodtime floodaction antiflood antiraid raidmode raidaction raidtime nousername cooldown setdupemode dupetime nopreview setmentionlimit setcapslimit capsmode setemojilimit emojimode banpack unbanpack bannedpacks banforward unbanforward bannedforwards readonly",
    "locks": "lock unlock locks allowlist rmallowlist silentactions",
    "content": "filter filters rmfilter rmfilters save get notes rmnote rmnotes clearallnotes addblocklist blocklist rmblocklist rmblocklistall blocklistmode setblocklistreason",
    "greetings": "setwelcome welcome clearwelcome setgoodbye goodbye cleargoodbye setrules clearrules welcomebutton clearwelcomebuttons setdrip drip cleardrip",
    "members": "tag untag tags setxp setxprate addxp removexp xp xpleaderboard setrank ranks rank approve unapprove approved unapproveall",
    "captcha": "captcha captchamode captchakicktime captchamutetime captcharules captchaaction",
    "federation": "newfed renamefed delfed fedpromote feddemote joinfed leavefed fban unfban fedstat fbanlist subfed unsubfed quietfed fedinfo fedadmins myfed",
    "scheduler": "poll quiz closepoll checklist checklists schedule schedulerecur schedules cancelschedule pin unpin unpinall countdown",
    "cleanup": "purge spurge purgefrom del cleancommand keepcommand cleancommandtypes cleanservice cleanservicetypes lockreactions allowreactions",
    "topics": "newtopic renametopic closetopic reopentopic deletetopic settopiclog",
    "settings": "settings setlog unsetlog logchannel logcategories groupinfo aisettings aimode aispam aitoxic aipii aithreshold ailog exportsettings importsettings reset connect disconnect connection bot2bot",
    "owner": "panel broadcast",
}
# No deferred placeholders: genuinely unavailable platform operations are reported
# separately, never disguised as implementations or removed from the inventory.
DEFERRED: dict[str, str] = {}
UNSUPPORTED = {
    "newaccountguard": "Telegram does not expose account creation dates ◡",
    "slowmode": "native slow mode cannot be set through this Bot API; use group settings, or /cooldown for Marie's message-deletion policy ◡",
}
SECTIONS["moderation"] += " setwarnladder propose template templates"
SECTIONS["protection"] += " bioblacklist rmbioblacklist"
SECTIONS["federation"] += " importfbans"
SECTIONS["scheduler"] += " permapin unpermapin"
SECTIONS["cleanup"] += " purgeto delreactions"
SECTIONS["members"] += " setbirthday birthday setbirthdaymsg"
COMMANDS = {c for names in SECTIONS.values() for c in names.split()}
PUBLIC = set(
    "start help helpadmin helpuser about id info ping rules report warns strikes xp xpleaderboard rank ranks get notes connection countdown setbirthday".split()
)
DEFAULTS = {
    "warnlimit": 3,
    "warnaction": "mute",
    "warntime": 7,
    "antiflood": False,
    "flood": 5,
    "floodtime": 10,
    "floodaction": "mute",
    "antiraid": False,
    "raidaction": "mute",
    "raidtime": 300,
    "nousername": False,
    "cooldown": 0,
    "dupetime": 60,
    "setdupemode": "delete",
    "nopreview": False,
    "setmentionlimit": 5,
    "setcapslimit": 0,
    "capsmode": "delete",
    "setemojilimit": 0,
    "emojimode": "delete",
    "readonly": False,
    "silentactions": True,
    "allowlist": [],
    "blocklistmode": "delete",
    "setblocklistreason": "blocked content",
    "captcha": False,
    "captchamode": "button",
    "captchakicktime": 60,
    "captchamutetime": 300,
    "captcharules": "please verify to join ·͜·",
    "captchaaction": "kick",
    "setxp": False,
    "setxprate": 10,
    "cleancommand": False,
    "cleanservice": False,
    "cleancommandtypes": [],
    "cleanservicetypes": [],
    "bot2bot": False,
    "spam_enabled": False,
    "toxic_enabled": False,
    "pii_enabled": False,
    "spam_threshold": 0.85,
    "toxic_threshold": 0.8,
    "pii_threshold": 0.85,
    "log_categories": [],
    "quietfed": False,
    "warn_ladder": [],
}
BOOL_SETTINGS = set(
    "antiflood antiraid nousername nopreview readonly silentactions captcha setxp cleancommand cleanservice bot2bot".split()
)
INT_SETTINGS = {
    "flood": (2, 100),
    "floodtime": (1, 3600),
    "raidtime": (30, 86400),
    "cooldown": (0, 3600),
    "dupetime": (1, 3600),
    "setmentionlimit": (1, 100),
    "setcapslimit": (0, 100),
    "setemojilimit": (0, 100),
    "warnlimit": (1, 100),
    "warntime": (1, 3650),
    "captchakicktime": (30, 3600),
    "captchamutetime": (30, 86400),
    "setxprate": (1, 100),
}
CHOICE_SETTINGS = {
    "floodaction": {"mute", "kick", "ban"},
    "raidaction": {"mute", "ban"},
    "setdupemode": {"delete", "warn", "mute", "ban"},
    "capsmode": {"delete", "warn"},
    "emojimode": {"delete", "warn"},
    "warnaction": {"mute", "kick", "ban"},
    "blocklistmode": {"delete", "warn", "mute", "kick", "ban"},
    "captchamode": {"button", "math", "text"},
    "captchaaction": {"kick", "ban", "mute"},
}
LOCK_TYPES = set(
    "text media sticker gif game inline command forward url button poll location contact audio video document voice videonote rtl spoiler emoji topic_create topic_delete".split()
)


PHASE3_COMMANDS = set(
    "adminperf setdrip drip cleardrip setbirthday birthday setbirthdaymsg shiftnotes".split()
)
PHASE1_COMMANDS = set(
    "setwarnladder propose template templates bioblacklist rmbioblacklist importfbans permapin unpermapin purgeto delreactions".split()
)


def validate_ladder(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list) or len(value) not in {0, 3}:
        raise ValueError("use exactly three warning steps ◡")
    previous = 0
    for step in value:
        if not isinstance(step, dict) or set(step) != {"warns", "action", "seconds"}:
            raise ValueError("hmm, check the warning steps ◡")
        if type(step["warns"]) is not int or not previous < step["warns"] <= 100:
            raise ValueError("warning thresholds must increase, up to 100 ◡")
        if step["action"] not in {"mute", "kick"} or type(step["seconds"]) is not int:
            raise ValueError("choose mute or kick ◡")
        if step["action"] == "mute" and not 60 <= step["seconds"] <= 365 * 86400:
            raise ValueError("mute duration must be 1m–365d ◡")
        if step["action"] == "kick" and step["seconds"] != 0:
            raise ValueError("kick has no duration; enter 0 ◡")
        previous = step["warns"]
    return value


def parse_fed_import(content: bytes) -> tuple[list[dict[str, Any]], int]:
    if len(content) > MAX_FILE:
        raise ValueError("keep the import under 1 MB ◡")
    try:
        text = content.decode("utf-8-sig").strip()
    except UnicodeError as exc:
        raise ValueError("send a UTF-8 text or JSON file ◡") from exc
    if not text:
        raise ValueError("that file is empty ◡")
    if text.startswith("["):
        try:
            entries = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("hmm, check the JSON file ◡") from exc
        if not isinstance(entries, list):
            raise ValueError("use a JSON array of user_id and reason ◡")
    else:
        entries = []
        for line in text.splitlines():
            pieces = line.strip().split(maxsplit=1)
            entries.append(
                {
                    "user_id": pieces[0],
                    "reason": pieces[1] if len(pieces) > 1 else "imported",
                }
                if pieces
                else None
            )
    if len(entries) > 10000:
        raise ValueError("10000 entries per import, please ◡")
    valid, skipped, seen = [], 0, set()
    for entry in entries:
        raw = entry.get("user_id") if isinstance(entry, dict) else None
        reason = entry.get("reason", "imported") if isinstance(entry, dict) else None
        if (
            isinstance(raw, bool)
            or not isinstance(raw, (str, int))
            or not re.fullmatch(r"[1-9]\d{0,15}", str(raw))
            or int(raw) >= 2**52
            or not isinstance(reason, str)
            or len(reason) > 1000
        ):
            skipped += 1
            continue
        uid = int(raw)
        if uid in seen:
            skipped += 1
            continue
        valid.append({"user_id": uid, "reason": reason or "imported"})
        seen.add(uid)
    return valid, skipped


COOLDOWN_LUA = """local prev=redis.call('GET',KEYS[1]); local current=tonumber(ARGV[1]);
if prev and current-tonumber(prev)<tonumber(ARGV[2]) then
 local n=redis.call('INCR',KEYS[2]); redis.call('EXPIRE',KEYS[2],ARGV[3]); return n end;
redis.call('SET',KEYS[1],ARGV[1],'EX',ARGV[3]); return 0"""


def friendly_error(error: Exception) -> str:
    text = str(error)
    if (
        not isinstance(error, ValueError)
        or not text
        or len(text) > 450
        or any(
            token in text
            for token in (
                "invalid literal",
                "validation error",
                "Expecting",
                "Extra data",
                "could not convert",
                "does not match format",
                "unconverted data",
                "Error:",
                "error:",
            )
        )
    ):
        return "hmm, check the input with /help ◡"
    if text in {
        "invalid table",
        "invalid column",
        "unknown operation",
        "unknown moderation action",
        "callback too long",
    }:
        return "hmm, reopen that menu and try again ◡"
    return text if text.endswith(("◡", "ᯓ", "·͜·", "˚")) else "hmm, " + text + " ◡"


def birthday_date(value: str) -> tuple[int, int]:
    if not re.fullmatch(r"\d{2}-\d{2}", value):
        raise ValueError("use MM-DD, like 03-21 ◡")
    try:
        date = datetime.strptime("2000-" + value, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("hmm, that birthday date isn't valid ◡") from exc
    return date.month, date.day


def next_birthday(month: int, day: int, today: datetime) -> datetime:
    year = today.year
    while True:
        try:
            candidate = datetime(year, month, day, tzinfo=UTC)
        except ValueError:
            year += 1
            continue
        if candidate.date() >= today.date():
            return candidate
        year += 1


def duty_active(start: str, end: str, current: str) -> bool:
    return (
        start <= current <= end if start <= end else current >= start or current <= end
    )


def appeal_column(table: str) -> str:
    if table not in {"bans", "mutes"}:
        raise ValueError("hmm, couldn't find that appeal ◡")
    return "ban_id" if table == "bans" else "mute_id"


def schema_columns(table: str) -> list[str]:
    return re.findall(r"(?:^|,)\s*([a-z_]+)\s+(?:INTEGER|TEXT|REAL)", SCHEMA[table])


def message_locator(message: Any, chat: int) -> dict[str, Any]:
    return {
        "chat": message.chat.id if isinstance(message, T.Message) else chat,
        "message": message.message_id if isinstance(message, T.Message) else None,
        "ephemeral": message.ephemeral_message_id
        if isinstance(message, T.Message)
        else None,
    }


async def progress_screen(
    app: Marie, loc: dict[str, Any], screen: Screen, uid: int
) -> None:
    try:
        if loc.get("message"):
            if loc.get("ephemeral"):
                await app.bot.edit_ephemeral_message_text(
                    chat_id=loc["chat"],
                    receiver_user_id=uid,
                    ephemeral_message_id=loc["ephemeral"],
                    text="",
                    rich_message=screen.rich(),
                    reply_markup=screen.markup(),
                )
            else:
                await app.bot.edit_message_text(
                    chat_id=loc["chat"],
                    message_id=loc["message"],
                    text="",
                    rich_message=screen.rich(),
                    reply_markup=screen.markup(),
                )
        else:
            message = await app.show(uid, uid, screen)
            loc.update(message_locator(message, uid))
    except TelegramAPIError:
        LOG.warning("progress update unavailable user=%s", uid)


def ladder_lines(steps: list[dict[str, Any]]) -> list[str]:
    return [
        f"{'└' if i == len(steps) - 1 else '├'} {s['warns']} warns → {s['action']} · {s['seconds']}s"
        for i, s in enumerate(steps)
    ] or ["no ladder configured ｡"]


def tree_screen(title: str, lines: list[str], page: int, prefix: str) -> Screen:
    page = max(0, min(page, max(0, (len(lines) - 1) // 10)))
    part = lines[page * 10 : page * 10 + 10]
    tree = [
        f"{'└' if i == len(part) - 1 else '├'} {line}" for i, line in enumerate(part)
    ] or ["nothing here yet ｡"]
    rows = [
        [
            button(
                "←" if page else "⌁ first page",
                f"{prefix}:{page - 1}" if page else None,
            ),
            button(f"page {page + 1} · status"),
            button(
                "→" if (page + 1) * 10 < len(lines) else "⌁ last page",
                f"{prefix}:{page + 1}" if (page + 1) * 10 < len(lines) else None,
            ),
        ],
        [button("← back", "sections")],
    ]
    return Screen(title, lines=tree, rows=rows, expandable=True)


def now() -> float:
    return time.time()


def stamp() -> str:
    return datetime.now(UTC).isoformat()


def duration(value: str) -> int:
    match = re.fullmatch(r"([1-9]\d{0,6})([smhdw])", value.lower())
    if not match:
        raise ValueError("use a duration like 30m, 1h, or 7d ◡")
    seconds = (
        int(match[1]) * {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}[match[2]]
    )
    if not 60 <= seconds <= 365 * 86400:
        # Stay away from Telegram's <30s / >366d permanent-restriction boundary.
        raise ValueError("duration must be 1m–365d ◡")
    return seconds


def normalize(text: str) -> str:
    text = unicodedata.normalize("NFKC", text).casefold()
    # Common Greek/Cyrillic lookalikes, not a claim of universal confusable detection.
    text = text.translate(str.maketrans("аесорхуіјѕοαρϲм", "aecopxyijsoapcm"))
    return "".join(c for c in text if unicodedata.category(c) != "Cf")


def matches(text: str, phrase: str, mode: str = "substring") -> bool:
    text, phrase = normalize(text), normalize(phrase)
    if mode == "exact":
        return text == phrase
    if mode == "prefix":
        return text.startswith(phrase)
    if mode == "wildcard":
        import fnmatch

        return fnmatch.fnmatchcase(text, phrase)
    return phrase in text


def button(
    text: str, data: str | None = None, style: str = "primary", url: str | None = None
) -> T.InlineKeyboardButton:
    kw: dict[str, Any] = {"text": text, "style": style}
    if url:
        kw["url"] = url
    elif data:
        if len(data.encode()) > 64:
            raise ValueError("callback too long")
        kw["callback_data"] = data
    else:
        kw["disabled"] = T.DisabledButton()
        if not any(
            reason in text.lower()
            for reason in (
                "need",
                "no rights",
                "already",
                "not set",
                "not configured",
                "unavailable",
                "first page",
                "last page",
                "current",
                "status",
                "expired",
            )
        ):
            if text == "←":
                kw["text"] = "⌁ first page"
            elif text == "→":
                kw["text"] = "⌁ last page"
            elif re.fullmatch(r"[0-9 /]+", text):
                kw["text"] = text + " · current page"
            else:
                kw["text"] = text + " · not configured"
    return T.InlineKeyboardButton(**kw)


def keyboard(*rows: list[T.InlineKeyboardButton]) -> T.InlineKeyboardMarkup:
    if any(len(row) > 3 for row in rows):
        raise ValueError("three buttons per row maximum")
    return T.InlineKeyboardMarkup(inline_keyboard=list(rows))


@dataclass
class Screen:
    title: str
    summary: str = ""
    lines: list[str] | None = None
    rows: list[list[T.InlineKeyboardButton]] | None = None
    table: tuple[list[str], list[list[str]]] | None = None
    expandable: bool = False

    def __post_init__(self) -> None:
        if self.summary.count("\n") >= 2:
            self.lines = self.summary.splitlines() + list(self.lines or [])
            self.summary = ""
        if self.lines:
            self.lines = [
                piece for line in self.lines for piece in str(line).splitlines()
            ]

    def rich(self) -> T.InputRichMessage:
        blocks: list[Any] = [
            T.InputRichBlockSectionHeading(text=f"「 {self.title} 」", size=2)
        ]
        if self.summary:
            blocks.append(T.InputRichBlockParagraph(text=self.summary))
        if self.lines:
            if len(self.lines) > 6 or self.expandable:
                blocks.append(
                    T.InputRichBlockExpandableBlockQuotation(text="\n".join(self.lines))
                )
            else:
                blocks.append(
                    T.InputRichBlockBlockQuotation(
                        blocks=[T.InputRichBlockParagraph(text="\n".join(self.lines))]
                    )
                )
        if self.table:
            headers, records = self.table
            cells = [
                [
                    T.RichBlockTableCell(
                        align="left", valign="top", text=c, is_header=True
                    )
                    for c in headers
                ]
            ]
            cells += [
                [
                    T.RichBlockTableCell(align="left", valign="top", text=c)
                    for c in record
                ]
                for record in records
            ]
            blocks.append(
                T.InputRichBlockBlockQuotation(
                    blocks=[
                        T.InputRichBlockTable(
                            cells=cells, is_compact=True, is_striped=True
                        )
                    ]
                )
            )
        return T.InputRichMessage(blocks=blocks)

    def markup(self) -> T.InlineKeyboardMarkup | None:
        return keyboard(*(self.rows or [])) if self.rows else None

    def fallback(self) -> str:
        content = f"<b>「 {html.escape(self.title[:100])} 」</b>"
        if self.summary:
            content += "\n" + html.escape(self.summary[:2000])
        lines = list(self.lines or [])
        if self.table:
            headers, records = self.table
            lines += [" · ".join(headers)] + [" · ".join(row) for row in records]
        if lines:
            attr = " expandable" if len(lines) > 6 or self.expandable else ""
            content += (
                f"\n<blockquote{attr}>"
                + html.escape("\n".join(lines)[:1500])
                + "</blockquote>"
            )
        return content


class Export(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    version: int = Field(default=1, ge=1, le=1)
    settings: dict[str, Any] = Field(default_factory=dict)
    locks: list[dict[str, Any]] = Field(default_factory=list, max_length=100)
    blocklist: list[dict[str, Any]] = Field(default_factory=list, max_length=1000)
    filters: list[dict[str, Any]] = Field(default_factory=list, max_length=1000)
    notes: list[dict[str, Any]] = Field(default_factory=list, max_length=1000)
    welcomes: list[dict[str, Any]] = Field(default_factory=list, max_length=1)
    goodbyes: list[dict[str, Any]] = Field(default_factory=list, max_length=1)
    rules: list[dict[str, Any]] = Field(default_factory=list, max_length=1)


def validate_setting(key: str, value: Any) -> Any:
    if key in BOOL_SETTINGS or key.endswith("_enabled"):
        if type(value) is not bool:
            raise ValueError("expected on or off ◡")
    elif key in INT_SETTINGS:
        lo, hi = INT_SETTINGS[key]
        if type(value) is not int or not lo <= value <= hi:
            raise ValueError(f"use a value from {lo} to {hi} ◡")
    elif key in CHOICE_SETTINGS:
        if value not in CHOICE_SETTINGS[key]:
            raise ValueError("choose " + " · ".join(sorted(CHOICE_SETTINGS[key])))
    elif key.endswith("_threshold"):
        if type(value) not in (int, float) or not 0.5 <= value <= 1:
            raise ValueError("threshold must be 0.50–1.00 ◡")
    elif key in {
        "allowlist",
        "cleancommandtypes",
        "cleanservicetypes",
        "log_categories",
    }:
        if (
            not isinstance(value, list)
            or len(value) > 100
            or not all(isinstance(x, str) and len(x) <= 200 for x in value)
        ):
            raise ValueError("invalid list ◡")
    elif key in {"captcharules", "setblocklistreason"}:
        if not isinstance(value, str) or not 1 <= len(value) <= 2000:
            raise ValueError("text is too long ◡")
    elif key == "warn_ladder":
        validate_ladder(value)
    elif key == "quietfed":
        if type(value) is not bool:
            raise ValueError("expected a boolean ◡")
    elif key not in DEFAULTS:
        raise ValueError(f"unknown setting: {key}")
    return value


# SQL identifiers below are static, never supplied directly by a user.
SCHEMA = {
    "groups": "group_id INTEGER PRIMARY KEY, title TEXT, username TEXT, member_count INTEGER DEFAULT 0, joined_at REAL, settings_json TEXT DEFAULT '{}', log_channel_id INTEGER, log_topics_json TEXT DEFAULT '{}', active INTEGER DEFAULT 1",
    "users": "user_id INTEGER PRIMARY KEY, username TEXT, first_name TEXT, last_name TEXT, is_bot INTEGER, first_seen REAL, last_seen REAL, message_count INTEGER DEFAULT 0, xp INTEGER DEFAULT 0, rank_id INTEGER, dm_opt_in INTEGER DEFAULT 0",
    "group_members": "group_id INTEGER, user_id INTEGER, joined_at REAL, left_at REAL, warn_count INTEGER DEFAULT 0, is_approved INTEGER DEFAULT 0, is_tagged INTEGER DEFAULT 0, tag_label TEXT, xp INTEGER DEFAULT 0, PRIMARY KEY(group_id,user_id)",
    "cases": "case_id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER, user_id INTEGER, admin_id INTEGER, action TEXT, reason TEXT, duration INTEGER, created_at REAL, expires_at REAL, resolved INTEGER DEFAULT 0, tags_json TEXT DEFAULT '[]', notes_json TEXT DEFAULT '[]', status TEXT DEFAULT 'pending', error TEXT",
    "warnings": "warn_id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER, user_id INTEGER, admin_id INTEGER, reason TEXT, created_at REAL, expires_at REAL, is_expired INTEGER DEFAULT 0",
    "bans": "ban_id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER, user_id INTEGER, admin_id INTEGER, reason TEXT, created_at REAL, expires_at REAL, is_fed_ban INTEGER DEFAULT 0, fed_id TEXT, resolved INTEGER DEFAULT 0",
    "mutes": "mute_id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER, user_id INTEGER, admin_id INTEGER, reason TEXT, created_at REAL, expires_at REAL, is_shadow INTEGER DEFAULT 0, resolved INTEGER DEFAULT 0",
    "filters": "filter_id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER, keyword TEXT, action TEXT, reply_text TEXT, reply_media_id TEXT, match_type TEXT DEFAULT 'substring', allow_bots INTEGER DEFAULT 0, created_at REAL, UNIQUE(group_id,keyword)",
    "notes": "note_id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER, name TEXT, content TEXT, media_id TEXT, media_type TEXT, buttons_json TEXT DEFAULT '[]', created_at REAL, UNIQUE(group_id,name)",
    "blocklist": "entry_id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER, phrase TEXT, match_type TEXT DEFAULT 'substring', action TEXT, reason TEXT, created_at REAL, UNIQUE(group_id,phrase)",
    "locks": "group_id INTEGER, lock_type TEXT, is_locked INTEGER, allowlist_json TEXT DEFAULT '[]', PRIMARY KEY(group_id,lock_type)",
    "federations": "fed_id TEXT PRIMARY KEY, name TEXT, owner_id INTEGER, created_at REAL, quiet_mode INTEGER DEFAULT 0, settings_json TEXT DEFAULT '{}'",
    "fed_members": "fed_id TEXT, group_id INTEGER UNIQUE, joined_at REAL, PRIMARY KEY(fed_id,group_id)",
    "fed_admins": "fed_id TEXT, user_id INTEGER, added_at REAL, PRIMARY KEY(fed_id,user_id)",
    "fed_bans": "fed_id TEXT, user_id INTEGER, reason TEXT, banned_by INTEGER, banned_at REAL, PRIMARY KEY(fed_id,user_id)",
    "fed_subscriptions": "fed_id TEXT, subscribed_to_fed_id TEXT, PRIMARY KEY(fed_id,subscribed_to_fed_id)",
    "welcomes": "group_id INTEGER PRIMARY KEY, welcome_text TEXT, media_id TEXT, media_type TEXT, buttons_json TEXT DEFAULT '[]', is_rich INTEGER DEFAULT 1, delete_after_seconds INTEGER DEFAULT 0",
    "goodbyes": "group_id INTEGER PRIMARY KEY, goodbye_text TEXT, media_id TEXT, media_type TEXT, buttons_json TEXT DEFAULT '[]'",
    "rules": "group_id INTEGER PRIMARY KEY, rules_text TEXT, is_rich INTEGER DEFAULT 1, buttons_json TEXT DEFAULT '[]'",
    "captcha": "group_id INTEGER PRIMARY KEY, mode TEXT, kick_time INTEGER, mute_time INTEGER, custom_message TEXT, fail_action TEXT",
    "captcha_pending": "user_id INTEGER, group_id INTEGER, message_id INTEGER, challenge TEXT, answer TEXT, expires_at REAL, PRIMARY KEY(group_id,user_id)",
    "flood": "group_id INTEGER PRIMARY KEY, limit_count INTEGER, window_seconds INTEGER, action TEXT, mute_duration INTEGER",
    "scheduled_posts": "post_id TEXT PRIMARY KEY, group_id INTEGER, topic_id INTEGER, content_json TEXT, media_id TEXT, run_at REAL, recur_cron TEXT, is_active INTEGER DEFAULT 1",
    "drip_sequences": "seq_id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER, delay_seconds INTEGER, content_json TEXT, position INTEGER",
    "drip_sent": "user_id INTEGER, group_id INTEGER, seq_id INTEGER, sent_at REAL, PRIMARY KEY(user_id,group_id,seq_id)",
    "mod_templates": "template_id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER, admin_id INTEGER, name TEXT, action TEXT, reason TEXT, duration INTEGER",
    "shift_notes": "note_id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER, admin_id INTEGER, content TEXT, created_at REAL",
    "duty_roster": "roster_id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER, admin_id INTEGER, start_time TEXT, end_time TEXT, is_active INTEGER DEFAULT 1",
    "report_inbox": "report_id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER, reporter_id INTEGER, target_id INTEGER, message_id INTEGER, status TEXT DEFAULT 'pending', claimed_by INTEGER, created_at REAL",
    "admin_proposals": "proposal_id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER, proposer_id INTEGER, action TEXT, target_id INTEGER, reason TEXT, status TEXT, created_at REAL",
    "sticker_pack_bans": "group_id INTEGER, pack_name TEXT, banned_by INTEGER, banned_at REAL, PRIMARY KEY(group_id,pack_name)",
    "forward_source_bans": "group_id INTEGER, source_chat_id INTEGER, banned_by INTEGER, banned_at REAL, PRIMARY KEY(group_id,source_chat_id)",
    "xp_ranks": "rank_id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER, min_xp INTEGER, rank_name TEXT, UNIQUE(group_id,min_xp)",
    "checklists": "list_id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER, title TEXT, items_json TEXT DEFAULT '[]', created_by INTEGER, created_at REAL, is_pinned INTEGER DEFAULT 0",
    "audit_log": "log_id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER, admin_id INTEGER, action TEXT, target_id INTEGER, details_json TEXT, created_at REAL",
    "bot_settings": "key TEXT PRIMARY KEY, value TEXT",
    "broadcast_history": "broadcast_id INTEGER PRIMARY KEY AUTOINCREMENT, admin_id INTEGER, content_json TEXT, target_type TEXT, sent_count INTEGER, failed_count INTEGER DEFAULT 0, created_at REAL",
    "update_channel": "url TEXT",
    "main_menu_video": "file_id TEXT, source_link TEXT",
    "panel_menu_video": "file_id TEXT, source_link TEXT",
    "ai_settings": "group_id INTEGER PRIMARY KEY, spam_enabled INTEGER DEFAULT 0, toxic_enabled INTEGER DEFAULT 0, pii_enabled INTEGER DEFAULT 0, spam_threshold REAL DEFAULT .85, toxic_threshold REAL DEFAULT .8, pii_threshold REAL DEFAULT .85, nsfw_enabled INTEGER DEFAULT 0, nsfw_threshold REAL DEFAULT .85",
    "ai_log": "log_id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER, user_id INTEGER, message_text TEXT, capability TEXT, score REAL, action_taken TEXT, created_at REAL",
    "blacklisted_groups": "group_id INTEGER PRIMARY KEY, reason TEXT, created_at REAL",
    "schema_version": "version INTEGER PRIMARY KEY, applied_at REAL",
    "bioblacklist": "entry_id INTEGER PRIMARY KEY AUTOINCREMENT, group_id INTEGER NOT NULL, pattern TEXT NOT NULL, created_at INTEGER NOT NULL",
    "permapin": "group_id INTEGER PRIMARY KEY, message_id INTEGER NOT NULL",
    "reaction_seen": "group_id INTEGER, message_id INTEGER, actor_id INTEGER, actor_type TEXT, updated_at REAL, PRIMARY KEY(group_id,message_id,actor_id,actor_type)",
    "birthdays": "user_id INTEGER NOT NULL, group_id INTEGER NOT NULL, month INTEGER NOT NULL, day INTEGER NOT NULL, PRIMARY KEY(user_id,group_id)",
    "birthday_msg": "group_id INTEGER PRIMARY KEY, template TEXT NOT NULL DEFAULT 'happy birthday ·͜· {user}'",
    "birthday_delivery": "group_id INTEGER, user_id INTEGER, delivery_date TEXT, status TEXT, PRIMARY KEY(group_id,user_id,delivery_date)",
    "broadcast_delivery": "broadcast_key TEXT, target_id INTEGER, status TEXT, PRIMARY KEY(broadcast_key,target_id)",
}


class DB:
    def __init__(self, url: str, token: str):
        if not url.startswith(("libsql://", "wss://", "ws://", "http://")):
            raise ValueError(
                "TURSO_URL must use libsql://, wss://, ws://, or http:// for transaction support; local files are forbidden"
            )
        self.client = libsql_client.create_client(url=url, auth_token=token)

    async def query(
        self, sql: str, args: list[Any] | None = None
    ) -> list[dict[str, Any]]:
        result = await self.client.execute(sql, args or [])
        return [dict(zip(result.columns, row)) for row in result.rows]

    async def one(
        self, sql: str, args: list[Any] | None = None
    ) -> dict[str, Any] | None:
        rows = await self.query(sql, args)
        return rows[0] if rows else None

    async def execute(self, sql: str, args: list[Any] | None = None) -> Any:
        return await self.client.execute(sql, args or [])

    async def batch(self, statements: list[tuple[str, list[Any]]]) -> None:
        with self.client.transaction() as tx:
            for sql, args in statements:
                await tx.execute(sql, args)
            await tx.commit()

    async def initialize(self) -> None:
        for table, columns in SCHEMA.items():
            await self.execute(f"CREATE TABLE IF NOT EXISTS {table} ({columns})")
        await self.ensure_columns(
            {
                "admin_proposals": {
                    "duration": "INTEGER DEFAULT 0",
                    "reject_reason": "TEXT",
                    "reviewed_by": "INTEGER",
                },
                "bans": {"appeal_requested": "INTEGER DEFAULT 0"},
                "mutes": {"appeal_requested": "INTEGER DEFAULT 0"},
                "broadcast_history": {
                    "skipped_count": "INTEGER DEFAULT 0",
                    "time_seconds": "REAL DEFAULT 0",
                },
                "report_inbox": {"claimed_at": "REAL"},
                "groups": {"active": "INTEGER DEFAULT 1"},
                "users": {"last_seen": "INTEGER"},
            }
        )
        for name, table, cols in [
            ("cases_group_time", "cases", "group_id,created_at"),
            ("warnings_active", "warnings", "group_id,user_id,is_expired,expires_at"),
            ("audit_group_time", "audit_log", "group_id,created_at"),
            ("users_username", "users", "username"),
            ("schedules_active", "scheduled_posts", "is_active,run_at"),
        ]:
            await self.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {table} ({cols})")
        await self.execute("INSERT OR IGNORE INTO schema_version VALUES (1,?)", [now()])
        await self.execute("INSERT OR IGNORE INTO schema_version VALUES (2,?)", [now()])

    async def ensure_columns(self, changes: dict[str, dict[str, str]]) -> None:
        # libSQL follows SQLite: ADD COLUMN IF NOT EXISTS is not portable syntax.
        # Static identifiers + PRAGMA make migrations idempotent without destructive rebuilds.
        for table, columns in changes.items():
            existing = {
                r["name"] for r in await self.query(f"PRAGMA table_info({table})")
            }
            for name, declaration in columns.items():
                if name not in existing:
                    try:
                        await self.execute(
                            f"ALTER TABLE {table} ADD COLUMN {name} {declaration}"
                        )
                    except Exception:
                        refreshed = {
                            r["name"]
                            for r in await self.query(f"PRAGMA table_info({table})")
                        }
                        if name not in refreshed:
                            raise

    async def settings(self, gid: int) -> dict[str, Any]:
        row = await self.one("SELECT settings_json FROM groups WHERE group_id=?", [gid])
        saved = json.loads(row["settings_json"]) if row else {}
        missing = [k for k in ("warnlimit", "warnaction", "warntime") if k not in saved]
        if row and missing:
            global_defaults = await self.bot_setting("warn_defaults", {})
            changes = [
                (
                    "UPDATE groups SET settings_json=json_set(settings_json,?,json(?)) WHERE group_id=? AND json_type(settings_json,?) IS NULL",
                    [
                        f"$.{k}",
                        json.dumps(global_defaults.get(k, DEFAULTS[k])),
                        gid,
                        f"$.{k}",
                    ],
                )
                for k in missing
            ]
            await self.batch(changes)
            row = await self.one(
                "SELECT settings_json FROM groups WHERE group_id=?", [gid]
            )
            saved = json.loads(row["settings_json"])
        return DEFAULTS | saved

    async def set_setting(self, gid: int, key: str, value: Any) -> None:
        validate_setting(key, value)
        await self.execute(
            "UPDATE groups SET settings_json=json_set(settings_json,?,json(?)) WHERE group_id=?",
            [f"$.{key}", json.dumps(value), gid],
        )

    async def bot_setting(self, key: str, default: Any = None) -> Any:
        row = await self.one("SELECT value FROM bot_settings WHERE key=?", [key])
        return json.loads(row["value"]) if row else default

    async def set_bot_setting(self, key: str, value: Any) -> None:
        await self.execute(
            "INSERT INTO bot_settings(key,value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            [key, json.dumps(value)],
        )

    async def audit(
        self, gid: int, actor: int, action: str, target: int = 0, details: Any = None
    ) -> None:
        await self.execute(
            "INSERT INTO audit_log(group_id,admin_id,action,target_id,details_json,created_at) VALUES (?,?,?,?,?,?)",
            [gid, actor, action, target, json.dumps(details or {}), now()],
        )


# Redis holds ONLY volatile control state; persistent records belong to Turso.
CONSUME = """local v=redis.call('GET',KEYS[1]); if not v then return nil end;
local x=cjson.decode(v); if tostring(x.actor)~=ARGV[1] or tostring(x.chat)~=ARGV[2] then return nil end;
redis.call('DEL',KEYS[1]); return v"""
WINDOW = """local n=redis.call('INCR',KEYS[1]); if n==1 then redis.call('EXPIRE',KEYS[1],ARGV[1]) end; return n"""


class LocalGuards:
    """Local-only immutable model artifacts. No remote code or inference APIs.

    Each directory is a transformers-compatible sequence classifier; provide
    MODEL_*_SHA256 as the digest of the sorted relative-file-path/content hashes.
    Human evaluation and label mapping are deployment responsibilities.
    """

    def __init__(self):
        self.models: dict[str, Any] = {}
        self.pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="marie-guard")
        self.semaphore = asyncio.Semaphore(2)

    @staticmethod
    def tree_hash(path: Path) -> str:
        h = hashlib.sha256()
        for f in sorted(p for p in path.rglob("*") if p.is_file()):
            if f.is_symlink():
                raise ValueError("model symlinks forbidden")
            h.update(str(f.relative_to(path)).encode())
            h.update(hashlib.sha256(f.read_bytes()).digest())
        return h.hexdigest()

    def load_sync(self) -> None:
        if os.getenv("LOAD_LOCAL_AI", "false").lower() != "true":
            return
        os.environ["HF_HUB_OFFLINE"] = "1"
        from transformers import (
            AutoModelForSequenceClassification,
            AutoTokenizer,
            pipeline,
        )

        for capability in ("spam", "toxic", "pii"):
            key = capability.upper()
            path = Path(os.environ[f"MODEL_{key}_PATH"]).resolve()
            expected = os.environ[f"MODEL_{key}_SHA256"]
            if not path.is_dir() or self.tree_hash(path) != expected:
                raise ValueError(f"model integrity check failed for {capability}")
            model = AutoModelForSequenceClassification.from_pretrained(
                str(path), local_files_only=True, trust_remote_code=False
            )
            tokenizer = AutoTokenizer.from_pretrained(
                str(path), local_files_only=True, trust_remote_code=False
            )
            label = os.environ[f"MODEL_{key}_POSITIVE_LABEL"]
            if label not in model.config.id2label.values():
                raise ValueError(f"unknown positive label for {capability}")
            self.models[capability] = (
                pipeline(
                    "text-classification", model=model, tokenizer=tokenizer, device=-1
                ),
                label,
            )

    async def score(self, capability: str, text: str) -> float:
        if capability not in self.models:
            raise ValueError("this capability needs local model files first ◡")
        async with self.semaphore:
            model, label = self.models[capability]

            def infer() -> float:
                scores = model(text[:6000], truncation=True, top_k=None)
                if scores and isinstance(scores[0], list):
                    scores = scores[0]
                return next(float(x["score"]) for x in scores if x["label"] == label)

            return await asyncio.get_running_loop().run_in_executor(self.pool, infer)


class Marie:
    def __init__(self, bot: Bot, db: DB, redis: Any, owner: int, guards: LocalGuards):
        self.bot, self.db, self.redis, self.owner, self.guards = (
            bot,
            db,
            redis,
            owner,
            guards,
        )
        self.started = now()
        self.me: T.User | None = None
        self.stopping = asyncio.Event()
        self.inflight: set[asyncio.Task] = set()

    async def rate(self, key: str, limit: int, seconds: int) -> bool:
        count = await self.redis.eval(WINDOW, 1, PREFIX + key, seconds)
        return int(count) <= limit

    async def is_admin(self, gid: int, uid: int, right: str | None = None) -> bool:
        member = await self.bot.get_chat_member(gid, uid)
        if member.status == "creator":
            return True
        return member.status == "administrator" and (
            right is None or getattr(member, right, False) is True
        )

    async def require_admin(self, gid: int, uid: int, right: str | None = None) -> None:
        if not await self.is_admin(gid, uid, right):
            raise ValueError("not enough permissions ᯓ")

    async def safe_target(self, gid: int, uid: int) -> None:
        if uid in {self.owner, self.me.id if self.me else 0}:
            raise ValueError("I can't act on this member ◡")
        if await self.is_admin(gid, uid):
            raise ValueError("admins are protected ◡")

    async def resolve(
        self, message: T.Message, args: list[str]
    ) -> tuple[int, list[str]]:
        if args and (
            args[0].startswith("@") or re.fullmatch(r"[1-9]\d{0,18}", args[0])
        ):
            value = args[0]
            if value.startswith("@"):
                user = await self.db.one(
                    "SELECT user_id FROM users WHERE lower(username)=lower(?) ORDER BY last_seen DESC LIMIT 1",
                    [value[1:]],
                )
                if not user:
                    raise ValueError(
                        "I haven't met that username; reply to their message ◡"
                    )
                # Cached usernames can change. Verify the current member's username.
                member = await self.bot.get_chat_member(
                    await self.context(message), user["user_id"]
                )
                if (
                    not member.user.username
                    or member.user.username.casefold() != value[1:].casefold()
                ):
                    raise ValueError("that username changed; use their numeric id ◡")
                return user["user_id"], args[1:]
            return int(value), args[1:]
        if message.reply_to_message and message.reply_to_message.from_user:
            return message.reply_to_message.from_user.id, args
        raise ValueError("reply to a member, or give @username / numeric id ◡")

    async def context(self, message: T.Message) -> int:
        if message.chat.type in {"group", "supergroup"}:
            return message.chat.id
        uid = message.from_user.id if message.from_user else 0
        value = await self.redis.get(f"{PREFIX}connection:{uid}")
        if not value:
            raise ValueError("connect a group first: /connect group_id ◡")
        gid = int(value)
        await self.require_admin(gid, uid)
        return gid

    async def show(
        self,
        chat: int,
        uid: int,
        screen: Screen,
        *,
        private: bool = True,
        callback: T.CallbackQuery | None = None,
    ) -> Any:
        markup = screen.markup()
        msg = (
            callback.message
            if callback and isinstance(callback.message, T.Message)
            else None
        )
        try:
            if msg:
                if msg.ephemeral_message_id:
                    return await self.bot.edit_ephemeral_message_text(
                        chat_id=chat,
                        receiver_user_id=uid,
                        ephemeral_message_id=msg.ephemeral_message_id,
                        text="",
                        rich_message=screen.rich(),
                        reply_markup=markup,
                    )
                if chat > 0 or not private:
                    return await self.bot.edit_message_text(
                        chat_id=chat,
                        message_id=msg.message_id,
                        text="",
                        rich_message=screen.rich(),
                        reply_markup=markup,
                    )
            params = None
            if private and chat < 0:
                params = T.EphemeralMessageParameters(
                    receiver_user_id=uid,
                    callback_query_id=callback.id if callback else None,
                    replace_callback_query_message=bool(callback),
                )
            return await self.bot.send_rich_message(
                chat_id=chat,
                rich_message=screen.rich(),
                reply_markup=markup,
                ephemeral_message_parameters=params,
            )
        except TelegramBadRequest as exc:
            if "message is not modified" in str(exc).lower():
                return None
            # Never downgrade confidential group feedback to a public message.
            LOG.warning("Rich UI failed; private HTML fallback: %s", exc)
            try:
                return await self.bot.send_message(
                    uid if private else chat, screen.fallback(), reply_markup=markup
                )
            except (TelegramForbiddenError, TelegramBadRequest):
                if callback:
                    await callback.answer(
                        "open my private chat to continue ◡", show_alert=True
                    )
                return None

    async def say(self, message: T.Message, text: str) -> None:
        if message.from_user:
            await self.show(
                message.chat.id, message.from_user.id, Screen("marie", text)
            )

    async def audit(
        self, gid: int, uid: int, action: str, target: int = 0, details: Any = None
    ) -> None:
        await self.db.audit(gid, uid, action, target, details)
        row = await self.db.one(
            "SELECT log_channel_id,log_topics_json FROM groups WHERE group_id=?", [gid]
        )
        cfg = await self.db.settings(gid)
        if (
            row
            and row["log_channel_id"]
            and (not cfg["log_categories"] or action in cfg["log_categories"])
        ):
            try:
                await self.bot.send_message(
                    row["log_channel_id"],
                    f"<b>「 {html.escape(action)} 」</b>\n<blockquote>group: {gid}\nactor: {uid}\nmember: {target}</blockquote>",
                    message_thread_id=json.loads(row["log_topics_json"]).get(action),
                )
            except TelegramAPIError:
                LOG.exception("log delivery failed group=%s action=%s", gid, action)

    async def remember(self, m: T.Message) -> None:
        u = m.from_user
        if not u:
            return
        await self.db.execute(
            "INSERT INTO users(user_id,username,first_name,last_name,is_bot,first_seen,last_seen,message_count,dm_opt_in) VALUES (?,?,?,?,?,?,?,1,?) ON CONFLICT(user_id) DO UPDATE SET username=excluded.username,first_name=excluded.first_name,last_name=excluded.last_name,last_seen=excluded.last_seen,message_count=users.message_count+1,dm_opt_in=MAX(users.dm_opt_in,excluded.dm_opt_in)",
            [
                u.id,
                u.username,
                u.first_name,
                u.last_name,
                int(u.is_bot),
                now(),
                now(),
                int(m.chat.type == "private"),
            ],
        )
        if m.chat.type in {"group", "supergroup"}:
            await self.db.execute(
                "INSERT INTO groups(group_id,title,username,joined_at) VALUES (?,?,?,?) ON CONFLICT(group_id) DO UPDATE SET title=excluded.title,username=excluded.username",
                [m.chat.id, m.chat.title, m.chat.username, now()],
            )
            await self.db.execute(
                "INSERT OR IGNORE INTO group_members(group_id,user_id,joined_at) VALUES (?,?,?)",
                [m.chat.id, u.id, now()],
            )
        await self.redis.incr(f"{PREFIX}processed:{datetime.now(UTC):%Y%m%d}")
        await self.redis.expire(f"{PREFIX}processed:{datetime.now(UTC):%Y%m%d}", 172800)

    async def video(self, chat: int, table: str) -> None:
        row = await self.db.one(f"SELECT * FROM {table} LIMIT 1")
        if not row:
            return
        try:
            if row["source_link"]:
                source, mid = telegram_link(row["source_link"])
                await self.bot.copy_message(chat, source, mid)
            else:
                data = json.loads(row["file_id"])
                fn = (
                    self.bot.send_animation
                    if data["type"] == "animation"
                    else self.bot.send_video
                )
                await fn(chat_id=chat, **{data["type"]: data["id"]})
        except TelegramAPIError:
            LOG.exception("menu media inaccessible table=%s", table)

    async def home(self, m: T.Message) -> None:
        uid = m.from_user.id
        if m.chat.type != "private":
            return await self.show(m.chat.id, uid, await self.section_menu(m.chat.id))
        await self.video(uid, "main_menu_video")
        channel = await self.db.one("SELECT url FROM update_channel LIMIT 1")
        support = await self.db.bot_setting("support", os.getenv("SUPPORT_CHAT", ""))
        rows = [
            [
                button(
                    "◈ add to group",
                    style="success",
                    url=f"https://t.me/{self.me.username}?startgroup=true",
                ),
                button("✦ support", url=support or None),
                button("∿ updates", url=channel["url"] if channel else None),
            ],
            [button("「 commands 」", "sections"), button("◇ about", "about")],
        ]
        if uid == self.owner:
            rows.append([button("⌘ admin panel", "panel:home", "danger")])
        await self.show(
            uid,
            uid,
            Screen(
                "marie",
                "·͜· cute, powerful, yours.",
                ["your personal group guardian ⋆"],
                rows,
            ),
        )

    async def section_menu(self, chat: int) -> Screen:
        sections = [s for s in SECTIONS if s != "owner"]
        buttons = [button(f"◈ {s}", f"help:{s}") for s in sections]
        rows = [buttons[i : i + 2] for i in range(0, len(buttons), 2)]
        if chat > 0:
            rows.append([button("← back", "home")])
        lines = [
            f"{'└' if i == len(sections) - 1 else '├'} {name}"
            for i, name in enumerate(sections)
        ]
        return Screen(
            "commands",
            "a little help, by section ·͜·",
            lines=lines,
            rows=rows,
            expandable=True,
        )

    def help_screen(self, section: str) -> Screen:
        if section not in SECTIONS or section == "owner":
            raise ValueError("unknown section ◡")
        cmds = SECTIONS[section].split()
        lines = [
            f"{'└' if i == len(cmds) - 1 else '├'} /{cmd}" for i, cmd in enumerate(cmds)
        ]
        return Screen(
            section,
            "choose a command below ⋆",
            lines,
            [[button("← back", "sections")]],
            expandable=True,
        )

    async def pending(
        self,
        chat: int,
        actor: int,
        gid: int,
        op: str,
        payload: dict[str, Any],
        description: str,
        callback: T.CallbackQuery | None = None,
    ) -> None:
        token = secrets.token_urlsafe(12)
        data = {
            "actor": actor,
            "chat": chat,
            "gid": gid,
            "op": op,
            "payload": payload,
            "expires": now() + 30,
        }
        await self.redis.set(
            f"{PREFIX}confirm:{token}", json.dumps(data), ex=30, nx=True
        )
        sent = await self.show(
            chat,
            actor,
            Screen(
                "confirm",
                description,
                ["expires in 30s · only you can confirm"],
                [
                    [
                        button("✦ confirm (30s)", f"confirm:{token}", "success"),
                        button("⌁ cancel", f"cancel:{token}"),
                    ]
                ],
            ),
            callback=callback,
        )
        if isinstance(sent, T.Message):
            locator = message_locator(sent, chat)
            await self.redis.set(
                f"{PREFIX}countdown:{token}",
                json.dumps(
                    {
                        "actor": actor,
                        "locator": locator,
                        "expires": data["expires"],
                        "description": description,
                    }
                ),
                ex=60,
            )
            job_id = f"{PREFIX}countdown-job:{token}:25"
            await self.redis.enqueue_job(
                "confirmation_tick", token, _defer_by=5, _job_id=job_id
            )
            await self.redis.set(f"{PREFIX}countdown-active:{token}", job_id, ex=60)

    async def setting_screen(self, gid: int) -> Screen:
        cfg = await self.db.settings(gid)
        keys = (
            "warnlimit",
            "warnaction",
            "warntime",
            "antiflood",
            "flood",
            "floodtime",
            "captcha",
            "captchamode",
            "antiraid",
            "setxp",
            "cooldown",
        )
        rows = [
            [
                button(f"◈ {key}: {str(cfg[key]).lower()}", f"v2:setting:{gid}:{key}"),
                button(
                    "toggle" if key in BOOL_SETTINGS else "edit",
                    f"v2:setting:{gid}:{key}",
                ),
            ]
            for key in keys
        ]
        rows += [
            [
                button("◈ ai capabilities", f"cfg:{gid}:ai:show"),
                button("⟡ help", "help:settings"),
            ],
            [button("← back", "sections")],
        ]
        return Screen(
            "settings",
            "private to you ⋆",
            table=(
                ["setting", "current value"],
                [[k, str(cfg[k]).lower()] for k in keys],
            ),
            rows=rows,
        )

    async def action(
        self,
        gid: int,
        actor: int,
        target: int,
        kind: str,
        reason: str,
        seconds: int = 0,
        *,
        automatic: bool = False,
    ) -> int:
        if not automatic:
            await self.require_admin(gid, actor, "can_restrict_members")
        await self.safe_target(gid, target)
        if not self.me:
            raise RuntimeError("bot identity not initialized")
        await self.require_admin(gid, self.me.id, "can_restrict_members")
        reason = reason[:1000] or "no reason given"
        result = await self.db.execute(
            "INSERT INTO cases(group_id,user_id,admin_id,action,reason,duration,created_at,expires_at) VALUES (?,?,?,?,?,?,?,?)",
            [
                gid,
                target,
                actor,
                kind,
                reason,
                seconds,
                now(),
                now() + seconds if seconds else None,
            ],
        )
        case_id = result.last_insert_rowid
        try:
            expiry = datetime.now(UTC) + timedelta(seconds=seconds) if seconds else None
            if kind in {"ban", "tban", "kick", "softban"}:
                await self.bot.ban_chat_member(
                    gid, target, until_date=expiry, revoke_messages=True
                )
                if kind in {"kick", "softban"}:
                    await self.bot.unban_chat_member(gid, target, only_if_banned=True)
                else:
                    await self.db.execute(
                        "INSERT INTO bans(group_id,user_id,admin_id,reason,created_at,expires_at) VALUES (?,?,?,?,?,?)",
                        [
                            gid,
                            target,
                            actor,
                            reason,
                            now(),
                            now() + seconds if seconds else None,
                        ],
                    )
            elif kind == "unban":
                await self.bot.unban_chat_member(gid, target, only_if_banned=True)
                await self.db.execute(
                    "UPDATE bans SET resolved=1 WHERE group_id=? AND user_id=?",
                    [gid, target],
                )
            elif kind in {"mute", "tmute"}:
                await self.bot.restrict_chat_member(
                    gid,
                    target,
                    T.ChatPermissions(can_send_messages=False),
                    until_date=expiry,
                    use_independent_chat_permissions=True,
                )
                await self.db.execute(
                    "INSERT INTO mutes(group_id,user_id,admin_id,reason,created_at,expires_at,is_shadow) VALUES (?,?,?,?,?,?,0)",
                    [
                        gid,
                        target,
                        actor,
                        reason,
                        now(),
                        now() + seconds if seconds else None,
                    ],
                )
            elif kind == "shadowmute":
                await self.db.execute(
                    "INSERT INTO mutes(group_id,user_id,admin_id,reason,created_at,is_shadow) VALUES (?,?,?,?,?,1)",
                    [gid, target, actor, reason, now()],
                )
                await self.redis.sadd(f"{PREFIX}shadowmuted_users:{gid}", str(target))
            elif kind in {"unmute", "unshadowmute"}:
                if kind == "unmute":
                    chat = await self.bot.get_chat(gid)
                    perms = chat.permissions or T.ChatPermissions(
                        **{k: True for k in T.ChatPermissions.model_fields}
                    )
                    await self.bot.restrict_chat_member(
                        gid, target, perms, use_independent_chat_permissions=True
                    )
                await self.db.execute(
                    "UPDATE mutes SET resolved=1 WHERE group_id=? AND user_id=? AND is_shadow=?",
                    [gid, target, int(kind == "unshadowmute")],
                )
                if kind == "unshadowmute":
                    await self.redis.srem(
                        f"{PREFIX}shadowmuted_users:{gid}", str(target)
                    )
            elif kind in {"warn", "strike"}:
                cfg = await self.db.settings(gid)
                await self.db.execute(
                    "INSERT INTO warnings(group_id,user_id,admin_id,reason,created_at,expires_at) VALUES (?,?,?,?,?,?)",
                    [
                        gid,
                        target,
                        actor,
                        ("strike · " if kind == "strike" else "") + reason,
                        now(),
                        now() + cfg["warntime"] * 86400,
                    ],
                )
                count = await self.db.one(
                    "SELECT count(*) n FROM warnings WHERE group_id=? AND user_id=? AND is_expired=0 AND expires_at>? AND reason NOT LIKE 'strike · %'",
                    [gid, target, now()],
                )
                ladder = cfg.get("warn_ladder", [])
                if kind == "warn" and ladder:
                    validate_ladder(ladder)
                    for step in ladder:
                        if count["n"] == step["warns"]:
                            gate = f"{PREFIX}ladder:{gid}:{target}:{step['warns']}"
                            if await self.redis.set(gate, "1", ex=30, nx=True):
                                try:
                                    await self.action(
                                        gid,
                                        self.me.id,
                                        target,
                                        step["action"],
                                        "warning ladder",
                                        step["seconds"],
                                        automatic=True,
                                    )
                                except Exception:
                                    await self.redis.delete(gate)
                                    raise
                            break
                elif kind == "warn" and count["n"] >= cfg["warnlimit"]:
                    await self.action(
                        gid,
                        self.me.id,
                        target,
                        cfg["warnaction"],
                        "warning threshold",
                        3600 if cfg["warnaction"] == "mute" else 0,
                        automatic=True,
                    )
                    await self.db.execute(
                        "UPDATE warnings SET is_expired=1 WHERE group_id=? AND user_id=?",
                        [gid, target],
                    )
            else:
                raise ValueError("unknown moderation action")
            await self.db.execute(
                "UPDATE cases SET status='applied' WHERE case_id=?", [case_id]
            )
            await self.audit(
                gid, actor, kind, target, {"case": case_id, "reason": reason}
            )
            return case_id
        except Exception as exc:
            await self.db.execute(
                "UPDATE cases SET status='failed',error=? WHERE case_id=?",
                [type(exc).__name__, case_id],
            )
            raise

    async def list_screen(
        self,
        title: str,
        rows: list[dict[str, Any]],
        columns: list[str],
        page: int = 0,
        callback_prefix: str | None = None,
        back: str = "sections",
    ) -> Screen:
        page = max(0, min(page, max(0, (len(rows) - 1) // 10)))
        selected = rows[page * 10 : page * 10 + 10]
        lines = [" · ".join(columns)] + [
            " · ".join(str(r.get(c, ""))[:120] for c in columns) for r in selected
        ]
        if not selected:
            lines = ["nothing here yet ｡"]
        buttons = []
        if callback_prefix:
            buttons.append(
                [
                    button("←", f"{callback_prefix}:{page - 1}" if page else None),
                    button(f"{page + 1} / {max(1, (len(rows) + 9) // 10)}"),
                    button(
                        "→",
                        f"{callback_prefix}:{page + 1}"
                        if (page + 1) * 10 < len(rows)
                        else None,
                    ),
                ]
            )
        buttons.append([button("← back", back)])
        table = (
            (columns, [[str(r.get(c, ""))[:120] for c in columns] for r in selected])
            if selected
            else None
        )
        return Screen(title, lines=None if table else lines, rows=buttons, table=table)

    async def flow(self, m: T.Message, state: dict[str, Any], prompt: str) -> None:
        uid = m.from_user.id
        if uid == self.owner and m.chat.type == "private":
            await self.redis.delete(f"{PREFIX}owner_input:{uid}")
        state |= {"actor": uid, "chat": m.chat.id}
        sent = await self.show(
            m.chat.id,
            uid,
            Screen(
                "a little input", prompt, rows=[[button("⌁ cancel", "v2:flowcancel")]]
            ),
        )
        if isinstance(sent, T.Message):
            state["prompt"] = sent.message_id
            state["chat"] = sent.chat.id
            state["ephemeral_prompt"] = sent.ephemeral_message_id
            # Native inline force_reply is available in the verified SDK.
            markup = keyboard([button("⌁ cancel", "v2:flowcancel")])
            markup.force_reply = True
            if sent.ephemeral_message_id:
                await self.bot.edit_ephemeral_message_reply_markup(
                    chat_id=sent.chat.id,
                    receiver_user_id=uid,
                    ephemeral_message_id=sent.ephemeral_message_id,
                    reply_markup=markup,
                )
            else:
                await self.bot.edit_message_reply_markup(
                    chat_id=sent.chat.id,
                    message_id=sent.message_id,
                    reply_markup=markup,
                )
        await self.redis.set(
            f"{PREFIX}flow:{uid}:{state['chat']}", json.dumps(state), ex=300
        )

    async def phase_input(self, m: T.Message) -> bool:
        if not m.from_user:
            return False
        raw = await self.redis.get(f"{PREFIX}flow:{m.from_user.id}:{m.chat.id}")
        if not raw:
            return False
        s = json.loads(raw)
        if (m.text or "").startswith("/"):
            if m.text == "/cancel":
                await self.redis.delete(f"{PREFIX}flow:{m.from_user.id}:{m.chat.id}")
                await self.say(m, "cancelled ˚")
                return True
            if (
                s["kind"] != "broadcast_content"
                or not m.reply_to_message
                or m.reply_to_message.message_id != s.get("prompt")
            ):
                return False
        if m.chat.id < 0:
            reply = m.reply_to_message
            bound = reply and (
                reply.ephemeral_message_id == s.get("ephemeral_prompt")
                if s.get("ephemeral_prompt")
                else reply.message_id == s.get("prompt")
            )
            if not bound:
                return False
        # Compare-and-delete serializes concurrent input messages.
        lock = f"{PREFIX}flow-lock:{m.from_user.id}:{m.chat.id}"
        nonce = secrets.token_hex(8)
        if not await self.redis.set(lock, nonce, nx=True, ex=30):
            return True
        try:
            if (
                await self.redis.get(f"{PREFIX}flow:{m.from_user.id}:{m.chat.id}")
                != raw
            ):
                return True
            await self.phase_flow_input(m, s)
        finally:
            await self.redis.eval(
                "if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) else return 0 end",
                1,
                lock,
                nonce,
            )
        return True

    async def phase_flow_input(self, m: T.Message, s: dict[str, Any]) -> None:
        kind, uid, text = s["kind"], m.from_user.id, (m.text or "").strip()
        if kind == "ladder":
            await self.require_admin(s["gid"], uid, "can_restrict_members")
            field = s["field"]
            step = s.get("current", {})
            if field == "warns":
                if (
                    not text.isdigit()
                    or not (s["steps"][-1]["warns"] if s["steps"] else 0)
                    < int(text)
                    <= 100
                ):
                    raise ValueError("choose a larger warning count, up to 100 ◡")
                step["warns"] = int(text)
                s["current"] = step
                s["field"] = "action"
                return await self.flow(m, s, "step action: mute or kick")
            if field == "action":
                if text not in {"mute", "kick"}:
                    raise ValueError("choose mute or kick ◡")
                step["action"] = text
                s["current"] = step
                s["field"] = "duration"
                return await self.flow(m, s, "duration: 1h, 3d, or 0 for kick")
            if step["action"] == "kick":
                if text != "0":
                    raise ValueError("kick has no duration; enter 0 ◡")
                step["seconds"] = 0
            else:
                step["seconds"] = duration(text)
            s["steps"].append(step)
            s["current"] = {}
            s["field"] = "warns"
            if len(s["steps"]) < 3:
                return await self.flow(
                    m, s, f"step {len(s['steps']) + 1}: warning count"
                )
            validate_ladder(s["steps"])
            await self.redis.delete(f"{PREFIX}flow:{uid}:{m.chat.id}")
            return await self.pending(
                m.chat.id,
                uid,
                s["gid"],
                "v2_ladder",
                {"steps": s["steps"]},
                "save this ladder?\n" + "\n".join(ladder_lines(s["steps"])),
            )
        if kind == "proposal_reject":
            await self.require_admin(s["gid"], uid, "can_promote_members")
            if not 1 <= len(text) <= 1000:
                raise ValueError("give a short reason, up to 1000 characters ◡")
            await self.redis.delete(f"{PREFIX}flow:{uid}:{m.chat.id}")
            return await self.pending(
                m.chat.id,
                uid,
                s["gid"],
                "v2_proposal_reject",
                {"id": s["id"], "reason": text},
                "reject this proposal?\n" + text,
            )
        if kind in {
            "broadcast_target",
            "broadcast_content",
            "global_target",
            "appeal_reason",
            "warn_default",
            "setting",
        }:
            return await self.panel_flow_input(m, s)
        raise ValueError("that input expired; open its menu again ◡")

    async def record_reaction(self, event: T.MessageReactionUpdated) -> None:
        actor = event.user or event.actor_chat
        if not actor:
            return
        kind = "user" if event.user else "chat"
        if event.new_reaction:
            await self.db.execute(
                "INSERT INTO reaction_seen(group_id,message_id,actor_id,actor_type,updated_at) VALUES (?,?,?,?,?) ON CONFLICT(group_id,message_id,actor_id,actor_type) DO UPDATE SET updated_at=excluded.updated_at",
                [event.chat.id, event.message_id, actor.id, kind, now()],
            )
        else:
            await self.db.execute(
                "DELETE FROM reaction_seen WHERE group_id=? AND message_id=? AND actor_id=? AND actor_type=?",
                [event.chat.id, event.message_id, actor.id, kind],
            )

    async def phase1_command(
        self, m: T.Message, cmd: str, text: str, args: list[str]
    ) -> None:
        gid, uid = await self.context(m), m.from_user.id
        await self.require_admin(gid, uid)
        if cmd == "setwarnladder":
            if args:
                raise ValueError("use /setwarnladder without arguments ◡")
            await self.require_admin(gid, uid, "can_restrict_members")
            cfg = await self.db.settings(gid)
            await self.show(
                m.chat.id,
                uid,
                Screen(
                    "warning ladder",
                    lines=ladder_lines(cfg.get("warn_ladder", [])),
                    expandable=True,
                ),
            )
            return await self.flow(
                m,
                {"kind": "ladder", "gid": gid, "steps": [], "field": "warns"},
                "step 1: warning count",
            )
        if cmd == "propose":
            if args == ["list"]:
                rows = await self.db.query(
                    "SELECT proposal_id,proposer_id,action,target_id,reason FROM admin_proposals WHERE group_id=? AND status='pending' ORDER BY proposal_id DESC LIMIT 1000",
                    [gid],
                )
                return await self.show_snapshot(
                    m,
                    "pending proposals",
                    rows,
                    ["proposal_id", "proposer_id", "action", "target_id", "reason"],
                )
            if len(args) < 2 or args[0] not in {"ban", "mute", "kick"}:
                raise ValueError("use /propose ban|mute|kick @member reason ◡")
            action = args[0]
            target, rest = await self.resolve(m, args[1:])
            await self.safe_target(gid, target)
            seconds = 3600 if action == "mute" else 0
            if action == "mute" and rest and re.fullmatch(r"\d+[smhdw]", rest[0]):
                seconds = duration(rest.pop(0))
            reason = " ".join(rest)
            if not 1 <= len(reason) <= 1000:
                raise ValueError("add a reason, up to 1000 characters ◡")
            result = await self.db.execute(
                "INSERT INTO admin_proposals(group_id,proposer_id,action,target_id,reason,status,created_at,duration) VALUES (?,?,?,?,?,'pending',?,?)",
                [gid, uid, action, target, reason, now(), seconds],
            )
            pid = result.last_insert_rowid
            for admin in await self.bot.get_chat_administrators(gid):
                if admin.user.is_bot or not await self.is_admin(
                    gid, admin.user.id, "can_promote_members"
                ):
                    continue
                await self.show(
                    gid,
                    admin.user.id,
                    Screen(
                        "proposal",
                        f"{action} {target}",
                        [reason, f"proposed by {uid}"],
                        [
                            [
                                button(
                                    "✦ approve",
                                    f"v2:proposal:approve:{gid}:{pid}",
                                    "success",
                                ),
                                button(
                                    "⌦ reject",
                                    f"v2:proposal:reject:{gid}:{pid}",
                                    "danger",
                                ),
                            ]
                        ],
                    ),
                )
            return await self.say(m, f"proposal {pid} sent to senior staff ✦")
        if cmd in {"template", "templates"}:
            if cmd == "templates":
                if args:
                    raise ValueError("use /templates without arguments ◡")
                rows = await self.db.query(
                    "SELECT name,action,reason,duration FROM mod_templates WHERE group_id=? ORDER BY name LIMIT 1000",
                    [gid],
                )
                return await self.tree_snapshot(
                    m,
                    "action templates",
                    [
                        f"{r['name']} · {r['action']} · {r['duration']}s · {r['reason']}"
                        for r in rows
                    ],
                )
            if not args or args[0] not in {"save", "use"}:
                raise ValueError(
                    "use /template save name action duration reason · or /template use name @member ◡"
                )
            if args[0] == "save":
                if (
                    len(args) < 5
                    or not re.fullmatch(r"[\w-]{1,40}", args[1])
                    or args[2] not in {"ban", "mute", "kick"}
                ):
                    raise ValueError("use /template save name mute 3d reason ◡")
                remaining = args[3:]
                token = (
                    remaining.pop(0)
                    if re.fullmatch(r"(?:\d+[smhdw]|0)", remaining[0])
                    else remaining.pop()
                )
                seconds = 0 if token == "0" else duration(token)
                if args[2] == "kick" and seconds:
                    raise ValueError("kick duration must be 0 ◡")
                reason = " ".join(remaining)
                if not 1 <= len(reason) <= 1000:
                    raise ValueError("add a short template reason ◡")
                payload = {
                    "name": args[1],
                    "action": args[2],
                    "seconds": seconds,
                    "reason": reason,
                }
                return await self.pending(
                    m.chat.id,
                    uid,
                    gid,
                    "v2_template_save",
                    payload,
                    f"save template {args[1]}?\n{args[2]} · {seconds}s · {reason}",
                )
            if len(args) != 3:
                raise ValueError(
                    "use /template use name @member; extra overrides are not accepted ◡"
                )
            row = await self.db.one(
                "SELECT * FROM mod_templates WHERE group_id=? AND name=? ORDER BY template_id DESC LIMIT 1",
                [gid, args[1]],
            )
            if not row:
                raise ValueError("hmm, couldn't find that template ◡")
            target, rest = await self.resolve(m, args[2:])
            if rest:
                raise ValueError("template reason and duration cannot be overridden ◡")
            await self.require_admin(gid, uid, "can_restrict_members")
            await self.safe_target(gid, target)
            reason = (
                row["reason"]
                .replace("{user}", str(target))
                .replace("{admin}", str(uid))
            )
            return await self.pending(
                m.chat.id,
                uid,
                gid,
                "moderate",
                {
                    "target": target,
                    "kind": row["action"],
                    "reason": reason,
                    "seconds": row["duration"],
                },
                f"{row['action']} {target}?\n{row['duration']}s · {reason}",
            )
        if cmd in {"bioblacklist", "rmbioblacklist"}:
            if args == ["list"] and cmd == "bioblacklist":
                rows = await self.db.query(
                    "SELECT pattern FROM bioblacklist WHERE group_id=? ORDER BY entry_id",
                    [gid],
                )
                return await self.tree_snapshot(
                    m, "biography policy", [r["pattern"] for r in rows]
                )
            if not 1 <= len(text) <= 200:
                raise ValueError("give a biography pattern, up to 200 characters ◡")
            if cmd == "rmbioblacklist":
                return await self.pending(
                    m.chat.id,
                    uid,
                    gid,
                    "v2_bio_remove",
                    {"pattern": text},
                    f"remove biography rule {text}?",
                )
            await self.require_admin(gid, uid, "can_restrict_members")
            await self.db.execute(
                "INSERT INTO bioblacklist(group_id,pattern,created_at) SELECT ?,?,? WHERE NOT EXISTS (SELECT 1 FROM bioblacklist WHERE group_id=? AND pattern=?)",
                [gid, text, now(), gid, text],
            )
            return await self.say(m, "saved ˚")
        if cmd == "importfbans":
            fed = await self.db.one(
                "SELECT fed_id FROM fed_members WHERE group_id=?", [gid]
            )
            if not fed:
                raise ValueError("join a federation first ◡")
            await self.fed_authorize(fed["fed_id"], uid)
            attachment = m.document or (
                m.reply_to_message.document if m.reply_to_message else None
            )
            if not attachment or (attachment.file_size or 0) > MAX_FILE:
                raise ValueError("attach or reply to a JSON/text file under 1 MB ◡")
            stream = io.BytesIO()
            await self.bot.download(attachment, destination=stream)
            entries, skipped = parse_fed_import(stream.getvalue())
            safe = [r for r in entries if r["user_id"] not in {self.owner, self.me.id}]
            skipped += len(entries) - len(safe)
            token = secrets.token_urlsafe(10)
            await self.redis.set(
                f"{PREFIX}fed-import:{token}", json.dumps(safe), ex=120
            )
            return await self.pending(
                m.chat.id,
                uid,
                gid,
                "v2_importfbans",
                {"token": token, "fed": fed["fed_id"], "skipped": skipped},
                f"import {len(safe)} federation bans? skipped: {skipped}",
            )
        if cmd in {"permapin", "unpermapin"}:
            if args:
                raise ValueError("use this command without arguments ◡")
            await self.require_admin(gid, uid, "can_pin_messages")
            if cmd == "permapin" and (not m.reply_to_message or m.chat.id != gid):
                raise ValueError("reply to a message in the group ◡")
            return await self.pending(
                m.chat.id,
                uid,
                gid,
                "v2_permapin",
                {
                    "remove": cmd == "unpermapin",
                    "message": m.reply_to_message.message_id
                    if m.reply_to_message
                    else None,
                },
                "remove permanent pin?"
                if cmd == "unpermapin"
                else "keep this message pinned? I check periodically; Telegram sends no unpin event",
            )
        if cmd == "purgeto":
            await self.require_admin(gid, uid, "can_delete_messages")
            if (
                len(args) != 1
                or not args[0].isdigit()
                or not m.reply_to_message
                or m.chat.id != gid
            ):
                raise ValueError(
                    "reply to the first message: /purgeto last_message_id ◡"
                )
            first, last = m.reply_to_message.message_id, int(args[0])
            if not first <= last <= m.message_id or last - first >= 1000:
                raise ValueError("choose an existing range of at most 1000 ids ◡")
            return await self.pending(
                m.chat.id,
                uid,
                gid,
                "v2_purgeto",
                {"first": first, "last": last},
                f"delete ids {first}–{last}? unavailable messages are skipped; exact deletion counts are not returned",
            )
        if cmd == "delreactions":
            await self.require_admin(gid, uid, "can_delete_messages")
            if not m.reply_to_message or args:
                raise ValueError("reply to a message with /delreactions ◡")
            return await self.pending(
                m.chat.id,
                uid,
                gid,
                "v2_delreactions",
                {"message": m.reply_to_message.message_id},
                "remove reactions from reactors I have observed? anonymous and older unseen reactions cannot be enumerated",
            )

    async def tree_snapshot(
        self, m: T.Message, title: str, lines: list[str], page: int = 0
    ) -> None:
        token = secrets.token_urlsafe(8)
        await self.redis.set(
            f"{PREFIX}tree:{token}",
            json.dumps(
                {
                    "actor": m.from_user.id,
                    "chat": m.chat.id,
                    "gid": await self.context(m),
                    "title": title,
                    "lines": lines,
                }
            ),
            ex=300,
        )
        await self.show(
            m.chat.id,
            m.from_user.id,
            tree_screen(title, lines, page, f"v2:tree:{token}"),
        )

    async def cooldown_guard(self, m: T.Message, seconds: int) -> bool:
        if not seconds:
            return False
        gid, uid = m.chat.id, m.from_user.id
        strikes = int(
            await self.redis.eval(
                COOLDOWN_LUA,
                2,
                f"{PREFIX}last_message_time:{gid}:{uid}",
                f"{PREFIX}cooldown_strikes:{gid}:{uid}",
                now(),
                seconds,
                max(300, seconds * 10),
            )
        )
        if not strikes:
            return False
        await self.delete(m)
        if strikes == 3:
            await self.show(
                gid,
                uid,
                Screen(
                    "marie",
                    f"a little slower, please · wait {seconds}s between messages ◡",
                ),
            )
        elif strikes >= 5 and await self.redis.set(
            f"{PREFIX}cooldown-muted:{gid}:{uid}",
            "1",
            ex=max(60, seconds * 10),
            nx=True,
        ):
            await self.action(
                gid,
                self.me.id,
                uid,
                "mute",
                "cooldown limit",
                max(60, seconds * 10),
                automatic=True,
            )
        return True

    async def notify_duty(
        self, gid: int, reporter: int, target: int, report_id: int
    ) -> None:
        roster = await self.db.query(
            "SELECT * FROM duty_roster WHERE group_id=? AND is_active=1", [gid]
        )
        current = datetime.now(UTC).strftime("%H:%M")
        admins = await self.bot.get_chat_administrators(gid)
        active_ids = {
            r["admin_id"]
            for r in roster
            if duty_active(r["start_time"], r["end_time"], current)
        }
        eligible = [a.user for a in admins if not a.user.is_bot]
        chosen = [a for a in eligible if a.id in active_ids] or eligible
        for user in chosen:
            text = f'<b>「 report {report_id} 」</b>\n<blockquote><a href="tg://user?id={user.id}">{html.escape(user.full_name)}</a> · staff review requested\nmember: {target}\nreporter: {reporter}</blockquote>'
            try:
                await self.bot.send_message(
                    gid,
                    text,
                    parse_mode="HTML",
                    ephemeral_message_parameters=T.EphemeralMessageParameters(
                        receiver_user_id=user.id
                    ),
                )
            except TelegramAPIError:
                await self.show(
                    gid,
                    user.id,
                    Screen(
                        "report",
                        f"report {report_id} needs your review",
                        [f"member: {target}"],
                    ),
                )

    async def phase3_command(
        self, m: T.Message, cmd: str, text: str, args: list[str]
    ) -> None:
        uid = m.from_user.id
        if cmd == "setbirthday":
            if m.chat.type != "private" or not 1 <= len(args) <= 2:
                raise ValueError(
                    "set your own birthday in DM: /setbirthday MM-DD [group_id] ◡"
                )
            month, day = birthday_date(args[0])
            groups = await self.db.query(
                "SELECT m.group_id,g.title FROM group_members m JOIN groups g ON g.group_id=m.group_id WHERE m.user_id=? AND m.left_at IS NULL AND g.active=1 LIMIT 100",
                [uid],
            )
            if len(args) == 2:
                if not re.fullmatch(r"-[1-9]\d{0,18}", args[1]):
                    raise ValueError("give a negative group id ◡")
                groups = [g for g in groups if g["group_id"] == int(args[1])]
            if not groups:
                raise ValueError("I haven't seen you in an active group yet ◡")
            if len(groups) > 1:
                return await self.show(
                    uid,
                    uid,
                    Screen(
                        "birthday",
                        "choose where I should post your birthday greeting",
                        rows=[
                            [
                                button(
                                    g["title"] or str(g["group_id"]),
                                    f"v2:birthdayset:{month}:{day}:{g['group_id']}",
                                    "success",
                                )
                            ]
                            for g in groups
                        ]
                        + [[button("⌁ cancel", "v2:flowcancel")]],
                    ),
                )
            await self.save_birthday(uid, groups[0]["group_id"], month, day)
            return await self.say(
                m, "birthday saved · that group will see a greeting ˚"
            )
        gid = await self.context(m)
        await self.require_admin(gid, uid)
        if cmd == "setbirthdaymsg":
            if not 1 <= len(text) <= 1000 or "{user}" not in text:
                raise ValueError(
                    "give a birthday message containing {user}, up to 1000 characters ◡"
                )
            await self.db.execute(
                "INSERT INTO birthday_msg(group_id,template) VALUES (?,?) ON CONFLICT(group_id) DO UPDATE SET template=excluded.template",
                [gid, text],
            )
            return await self.say(m, "saved ˚")
        if cmd == "birthday":
            if args != ["list"]:
                raise ValueError("use /birthday list ◡")
            today = datetime.now(UTC)
            rows = await self.db.query(
                "SELECT b.*,u.first_name FROM birthdays b LEFT JOIN users u ON b.user_id=u.user_id WHERE b.group_id=?",
                [gid],
            )
            upcoming = []
            for r in rows:
                day = next_birthday(r["month"], r["day"], today)
                if (day.date() - today.date()).days <= 30:
                    upcoming.append(
                        {
                            "user": r["first_name"] or r["user_id"],
                            "date": day.strftime("%m-%d"),
                            "days": (day.date() - today.date()).days,
                        }
                    )
            upcoming.sort(key=lambda r: r["days"])
            return await self.show_snapshot(
                m, "birthdays · next 30 days", upcoming, ["user", "date", "days"]
            )
        if cmd == "shiftnotes":
            if args:
                raise ValueError("use /shiftnotes without arguments ◡")
            rows = await self.db.query(
                "SELECT admin_id,content,created_at FROM shift_notes WHERE group_id=? ORDER BY created_at DESC LIMIT 10",
                [gid],
            )
            return await self.tree_snapshot(
                m,
                "shift notes",
                [
                    f"[{r['admin_id']}] {datetime.fromtimestamp(r['created_at'], UTC):%H:%M} — {r['content']}"
                    for r in rows
                ],
            )
        if cmd in {"setdrip", "drip", "cleardrip"}:
            if cmd == "setdrip":
                if len(args) < 2 or not 1 <= len(" ".join(args[1:])) <= 3000:
                    raise ValueError("use /setdrip 24h your message ◡")
                seconds = duration(args[0])
                await self.db.execute(
                    "INSERT INTO drip_sequences(group_id,delay_seconds,content_json,position) SELECT ?,?,?,coalesce(max(position),0)+1 FROM drip_sequences WHERE group_id=?",
                    [gid, seconds, json.dumps({"text": " ".join(args[1:])}), gid],
                )
            elif cmd == "cleardrip":
                if not args:
                    return await self.pending(
                        m.chat.id,
                        uid,
                        gid,
                        "clear_table",
                        {"table": "drip_sequences"},
                        "clear every drip in this group?",
                    )
                if len(args) != 1 or not args[0].isdigit() or int(args[0]) < 1:
                    raise ValueError("use /cleardrip position, or /cleardrip for all ◡")
                rows = await self.db.query(
                    "SELECT seq_id FROM drip_sequences WHERE group_id=? ORDER BY delay_seconds,seq_id",
                    [gid],
                )
                index = int(args[0]) - 1
                if index >= len(rows):
                    raise ValueError("hmm, couldn't find that drip ◡")
                return await self.pending(
                    m.chat.id,
                    uid,
                    gid,
                    "v2_drip_remove",
                    {"seq_id": rows[index]["seq_id"]},
                    f"remove drip {args[0]}?",
                )
            elif args not in ([], ["list"]):
                raise ValueError("use /drip list ◡")
            rows = await self.db.query(
                "SELECT * FROM drip_sequences WHERE group_id=? ORDER BY delay_seconds,seq_id",
                [gid],
            )
            return await self.tree_snapshot(
                m,
                "drip sequence",
                [
                    f"{r['delay_seconds'] // 3600}h {r['delay_seconds'] % 3600 // 60}m — {json.loads(r['content_json'])['text']}"
                    for r in rows
                ],
            )
        if cmd == "adminperf":
            if args:
                raise ValueError("use /adminperf without arguments ◡")
            date = datetime.now(UTC)
            week = (
                (date - timedelta(days=date.weekday()))
                .replace(hour=0, minute=0, second=0, microsecond=0)
                .timestamp()
            )
            month = date.replace(
                day=1, hour=0, minute=0, second=0, microsecond=0
            ).timestamp()
            rows = await self.db.query(
                "SELECT admin_id,action,created_at FROM audit_log WHERE group_id=? AND created_at>=?",
                [gid, min(week, month)],
            )
            claim_admins = await self.db.query(
                "SELECT DISTINCT claimed_by FROM report_inbox WHERE group_id=? AND claimed_by IS NOT NULL",
                [gid],
            )
            admins = sorted(
                {r["admin_id"] for r in rows if r["admin_id"] != self.me.id}
                | {r["claimed_by"] for r in claim_admins}
            )
            values = []
            lines = []
            for admin in admins:
                own = [r for r in rows if r["admin_id"] == admin]
                week_n = sum(r["created_at"] >= week for r in own)
                month_n = sum(r["created_at"] >= month for r in own)
                action_counts = {
                    a: sum(r["action"] == a for r in own)
                    for a in {r["action"] for r in own}
                }
                common = (
                    max(action_counts, key=lambda a: (action_counts[a], a))
                    if action_counts
                    else "none"
                )
                claims = await self.db.one(
                    "SELECT count(*) n,avg(claimed_at-created_at) avg_wait FROM report_inbox WHERE group_id=? AND claimed_by=? AND claimed_at IS NOT NULL",
                    [gid, admin],
                )
                average = month_n / max(1, (date.timestamp() - month) / 86400)
                values.append(
                    [
                        str(admin),
                        str(week_n),
                        str(month_n),
                        common,
                        f"{average:.2f}",
                        str(claims["n"]),
                        f"{claims['avg_wait']:.1f}s"
                        if claims["avg_wait"] is not None
                        else "not recorded",
                    ]
                )
                lines += [
                    f"├ admin {admin}",
                    f"│  ├ week: {week_n} · month: {month_n}",
                    f"│  └ common: {common} · average/day: {average:.2f}",
                ]
            if lines:
                lines[-1] = lines[-1].replace("│  └", "   └", 1)
            return await self.show(
                m.chat.id,
                uid,
                Screen(
                    "admin performance",
                    "UTC calendar week/month · average/day uses elapsed month · claim delay uses recorded claims",
                    lines or ["nothing recorded yet ｡"],
                    table=(
                        [
                            "admin",
                            "week",
                            "month",
                            "common",
                            "avg/day",
                            "claimed",
                            "claim delay",
                        ],
                        values,
                    ),
                    expandable=True,
                ),
            )
        raise ValueError("hmm, check /help ◡")

    async def save_birthday(self, uid: int, gid: int, month: int, day: int) -> None:
        birthday_date(f"{month:02d}-{day:02d}")
        member = await self.bot.get_chat_member(gid, uid)
        if (
            member.status in {"left", "kicked"}
            or member.status == "restricted"
            and not member.is_member
        ):
            raise ValueError("you aren't a member of that group ◡")
        await self.db.execute(
            "INSERT INTO birthdays(user_id,group_id,month,day) VALUES (?,?,?,?) ON CONFLICT(user_id,group_id) DO UPDATE SET month=excluded.month,day=excluded.day",
            [uid, gid, month, day],
        )

    async def enhanced_panel(self, section: str, page: int = 0) -> Screen:
        back = [[button("← back", "panel:home")]]
        if section == "broadcast":
            return Screen(
                "broadcast",
                "choose an audience ⋆",
                rows=[
                    [
                        button("◈ all groups", "v2:bc:groups"),
                        button("◈ all users DM", "v2:bc:users"),
                    ],
                    [
                        button("◈ specific group", "v2:bc:group"),
                        button("◈ specific user", "v2:bc:user"),
                    ],
                ]
                + back,
            )
        if section == "warndefaults":
            cfg = {k: DEFAULTS[k] for k in ("warnlimit", "warnaction", "warntime")}
            cfg |= await self.db.bot_setting("warn_defaults", {})
            return Screen(
                "warn defaults",
                "new groups inherit these values; existing groups keep theirs",
                table=(["setting", "value"], [[k, str(v)] for k, v in cfg.items()]),
                rows=[
                    [
                        button("◈ edit limit", "v2:default:warnlimit"),
                        button("◈ edit action", "v2:default:warnaction"),
                        button("◈ edit expiry", "v2:default:warntime"),
                    ]
                ]
                + back,
            )
        if section == "appeals":
            rows = await self.db.query(
                "SELECT 'bans' kind,ban_id id,group_id,user_id,reason FROM bans WHERE appeal_requested=1 AND resolved=0 AND (expires_at IS NULL OR expires_at>?) UNION ALL SELECT 'mutes' kind,mute_id id,group_id,user_id,reason FROM mutes WHERE appeal_requested=1 AND resolved=0 AND (expires_at IS NULL OR expires_at>?)",
                [now(), now()],
            )
            screen = await self.list_screen(
                "appeals",
                rows,
                ["kind", "id", "group_id", "user_id", "reason"],
                page,
                "v2:panelpage:appeals",
                "panel:home",
            )
            controls = []
            for row in rows[page * 10 : page * 10 + 10]:
                controls.append(
                    [
                        button(
                            f"✦ approve {row['id']}",
                            f"v2:appeal:approve:{row['kind']}:{row['id']}",
                            "success",
                        ),
                        button(
                            f"⌦ reject {row['id']}",
                            f"v2:appeal:reject:{row['kind']}:{row['id']}",
                            "danger",
                        ),
                    ]
                )
            screen.rows = controls + (screen.rows or [])
            return screen
        if section == "database":
            counts = [
                [t, str((await self.db.one(f"SELECT count(*) n FROM {t}"))["n"])]
                for t in SCHEMA
            ]
            return Screen(
                "database",
                "backup contains operational rows and per-group settings exports",
                table=(["table", "rows"], counts),
                rows=[[button("⌦ export backup", "v2:backup")]] + back,
            )
        table, cols, where, order = {
            "groups": (
                "groups",
                ["group_id", "title", "member_count", "active"],
                "",
                "group_id",
            ),
            "users": ("users", ["user_id", "username", "message_count"], "", "user_id"),
            "audit": (
                "audit_log",
                ["log_id", "group_id", "admin_id", "action"],
                "",
                "log_id DESC",
            ),
            "bans": (
                "bans",
                ["group_id", "user_id", "reason"],
                "WHERE resolved=0 AND (expires_at IS NULL OR expires_at>strftime('%s','now'))",
                "ban_id DESC",
            ),
            "mutes": (
                "mutes",
                ["group_id", "user_id", "reason"],
                "WHERE resolved=0 AND (expires_at IS NULL OR expires_at>strftime('%s','now'))",
                "mute_id DESC",
            ),
        }[section]
        rows = await self.db.query(
            f"SELECT {','.join(cols)} FROM {table} {where} ORDER BY {order} LIMIT 1000"
        )
        page = max(0, min(page, max(0, (len(rows) - 1) // 10)))
        screen = await self.list_screen(
            section, rows, cols, page, f"v2:panelpage:{section}", "panel:home"
        )
        if section in {"groups", "users"}:
            key = "group_id" if section == "groups" else "user_id"
            screen.rows = [
                [
                    button(
                        f"◈ {r.get('title') or r.get('username') or r[key]}",
                        f"v2:{'group' if section == 'groups' else 'user'}:{r[key]}",
                    )
                ]
                for r in rows[page * 10 : page * 10 + 10]
            ] + (screen.rows or [])
        if section in {"audit", "bans", "mutes"}:
            screen.rows.insert(0, [button("⌦ export csv", f"v2:csv:{section}")])
        return screen

    async def group_detail(self, gid: int) -> Screen:
        row = await self.db.one("SELECT * FROM groups WHERE group_id=?", [gid])
        if not row:
            raise ValueError("hmm, couldn't find that group ◡")
        cfg = await self.db.settings(gid)
        data = [
            ["name", row["title"] or str(gid)],
            ["members", str(row["member_count"])],
            [
                "joined",
                datetime.fromtimestamp(row["joined_at"] or 0, UTC).strftime("%Y-%m-%d"),
            ],
        ]
        for label, table, where in [
            (
                "warns",
                "warnings",
                "AND is_expired=0 AND expires_at>strftime('%s','now')",
            ),
            (
                "active bans",
                "bans",
                "AND resolved=0 AND (expires_at IS NULL OR expires_at>strftime('%s','now'))",
            ),
            (
                "active mutes",
                "mutes",
                "AND resolved=0 AND (expires_at IS NULL OR expires_at>strftime('%s','now'))",
            ),
            ("filters", "filters", ""),
            ("notes", "notes", ""),
        ]:
            data.append(
                [
                    label,
                    str(
                        (
                            await self.db.one(
                                f"SELECT count(*) n FROM {table} WHERE group_id=? {where}",
                                [gid],
                            )
                        )["n"]
                    ),
                ]
            )
        fed = await self.db.one(
            "SELECT f.name FROM federations f JOIN fed_members m ON f.fed_id=m.fed_id WHERE m.group_id=?",
            [gid],
        )
        data += [
            ["captcha", str(cfg["captcha"])],
            ["flood", f"{cfg['flood']} / {cfg['floodtime']}s · {cfg['antiflood']}"],
            ["federation", fed["name"] if fed else "none"],
        ]
        return Screen(
            "group detail",
            table=(["setting", "value"], data),
            rows=[
                [
                    button("◈ view settings", f"v2:groupcfg:{gid}"),
                    button("◈ audit log", f"v2:groupaudit:{gid}"),
                ],
                [button("← back", "panel:groups")],
                [
                    button("⌦ leave group", f"v2:groupleave:{gid}", "danger"),
                    button("⌦ blacklist", f"v2:groupblacklist:{gid}", "danger"),
                ],
            ],
        )

    async def offer_appeals(self, m: T.Message) -> None:
        uid = m.from_user.id
        rows = await self.db.query(
            "SELECT 'bans' kind,b.ban_id id,b.group_id,g.title FROM bans b LEFT JOIN groups g ON b.group_id=g.group_id WHERE b.user_id=? AND b.resolved=0 AND b.appeal_requested=0 AND (b.expires_at IS NULL OR b.expires_at>?) UNION ALL SELECT 'mutes' kind,b.mute_id id,b.group_id,g.title FROM mutes b LEFT JOIN groups g ON b.group_id=g.group_id WHERE b.user_id=? AND b.resolved=0 AND b.appeal_requested=0 AND (b.expires_at IS NULL OR b.expires_at>?) LIMIT 10",
            [uid, now(), uid, now()],
        )
        for row in rows:
            if not await self.redis.set(
                f"{PREFIX}appeal-offer:{uid}:{row['kind']}:{row['id']}",
                "1",
                ex=3600,
                nx=True,
            ):
                continue
            await self.show(
                uid,
                uid,
                Screen(
                    "appeal",
                    f"you are {'banned' if row['kind'] == 'bans' else 'muted'} in {row['title'] or row['group_id']}",
                    ["would you like the owner to review it?"],
                    [
                        [
                            button(
                                "✦ appeal",
                                f"v2:appeal:request:{row['kind']}:{row['id']}",
                                "success",
                            )
                        ]
                    ],
                ),
            )

    async def broadcast_options(
        self, uid: int, state: dict[str, Any], callback: T.CallbackQuery | None = None
    ) -> None:
        await self.redis.set(f"{PREFIX}broadcast:{uid}", json.dumps(state), ex=600)
        await self.show(
            uid,
            uid,
            Screen(
                "broadcast options",
                f"audience: {state['target']}",
                rows=[
                    [
                        button(
                            f"◈ pin after: {'ON' if state.get('pin') else 'OFF'}",
                            "v2:bc:pin",
                        ),
                        button(
                            f"◈ forward mode: {'ON' if state.get('forward') else 'OFF'}",
                            "v2:bc:forward",
                        ),
                    ],
                    [
                        button("✦ send now", "v2:bc:preview", "success"),
                        button("⌁ cancel", "v2:bc:cancel"),
                    ],
                ],
            ),
            callback=callback,
        )

    async def panel_flow_input(self, m: T.Message, s: dict[str, Any]) -> None:
        uid = m.from_user.id
        kind = s["kind"]
        text = (m.text or "").strip()
        if kind != "setting":
            self.owner_only(m)
        if kind == "broadcast_target":
            if not re.fullmatch(r"-?[1-9]\d{0,18}", text) or (
                s["target_type"] == "group"
            ) != (int(text) < 0):
                raise ValueError(
                    "give a negative group id or positive user id for your chosen audience ◡"
                )
            s |= {"target": text, "kind": "broadcast_content"}
            return await self.flow(
                m, s, "reply with your broadcast text, photo, video or document"
            )
        if kind == "broadcast_content":
            if not (m.text or m.photo or m.video or m.document):
                raise ValueError("send text, a photo, a video or a document ◡")
            state = {
                "target": s["target"],
                "source": m.chat.id,
                "message": m.message_id,
                "pin": False,
                "forward": False,
            }
            await self.redis.delete(f"{PREFIX}flow:{uid}:{m.chat.id}")
            return await self.broadcast_options(uid, state)
        if kind == "global_target":
            if not re.fullmatch(r"[1-9]\d{0,15}", text) or int(text) >= 2**52:
                raise ValueError("give a positive Telegram user id ◡")
            await self.redis.delete(f"{PREFIX}flow:{uid}:{m.chat.id}")
            return await self.pending(
                uid,
                uid,
                0,
                "v2_global",
                {"target": int(text), "unban": s["unban"]},
                f"{'unban' if s['unban'] else 'ban'} {text} in every recorded group?",
            )
        if kind == "appeal_reason":
            if not 1 <= len(text) <= 1000:
                raise ValueError("give a short reason, up to 1000 characters ◡")
            await self.redis.delete(f"{PREFIX}flow:{uid}:{m.chat.id}")
            return await self.pending(
                uid,
                uid,
                0,
                "v2_appeal_reject",
                {"table": s["table"], "id": s["id"], "reason": text},
                "reject this appeal?\n" + text,
            )
        if kind == "warn_default":
            key = s["key"]
            value = (
                int(text)
                if key in {"warnlimit", "warntime"} and text.isdigit()
                else text
            )
            validate_setting(key, value)
            # Freeze current groups before changing defaults, preserving their existing behavior.
            for row in await self.db.query("SELECT group_id FROM groups"):
                await self.db.settings(row["group_id"])
            cfg = await self.db.bot_setting("warn_defaults", {})
            cfg[key] = value
            await self.db.set_bot_setting("warn_defaults", cfg)
            await self.redis.delete(f"{PREFIX}flow:{uid}:{m.chat.id}")
            return await self.show(uid, uid, await self.enhanced_panel("warndefaults"))
        if kind == "setting":
            await self.require_admin(s["gid"], uid)
            key = s["key"]
            value = int(text) if key in INT_SETTINGS and text.isdigit() else text
            validate_setting(key, value)
            await self.db.set_setting(s["gid"], key, value)
            await self.redis.delete(f"{PREFIX}flow:{uid}:{m.chat.id}")
            return await self.show(m.chat.id, uid, await self.setting_screen(s["gid"]))

    async def panel_callback(self, call: T.CallbackQuery) -> None:
        parts = call.data.split(":")
        uid = call.from_user.id
        chat = call.message.chat.id
        m = call.message.model_copy(update={"from_user": call.from_user})
        action = parts[1]
        if action == "birthdayset":
            if chat != uid:
                raise ValueError("set your birthday in my private chat ◡")
            month, day, gid = map(int, parts[2:])
            row = await self.db.one(
                "SELECT 1 FROM group_members WHERE group_id=? AND user_id=? AND left_at IS NULL",
                [gid, uid],
            )
            if not row:
                raise ValueError("I haven't seen you in that group ◡")
            await self.save_birthday(uid, gid, month, day)
            return await self.show(
                uid,
                uid,
                Screen("birthday", "saved · this group will see a greeting ˚"),
                callback=call,
            )
        if action == "appeal" and parts[2] == "request":
            if chat != uid:
                raise ValueError("open my private chat to appeal ◡")
            table = parts[3]
            ident = int(parts[4])
            col = appeal_column(table)
            result = await self.db.execute(
                f"UPDATE {table} SET appeal_requested=1 WHERE {col}=? AND user_id=? AND appeal_requested=0 AND resolved=0 AND (expires_at IS NULL OR expires_at>?)",
                [ident, uid, now()],
            )
            return await self.show(
                uid,
                uid,
                Screen(
                    "appeal",
                    "sent for review ✦"
                    if result.rows_affected
                    else "already reviewed or submitted ◡",
                ),
                callback=call,
            )
        if action == "setting":
            gid = int(parts[2])
            key = parts[3]
            await self.require_admin(gid, uid)
            if key not in BOOL_SETTINGS | set(INT_SETTINGS) | set(CHOICE_SETTINGS):
                raise ValueError("hmm, reopen settings ◡")
            cfg = await self.db.settings(gid)
            if key in BOOL_SETTINGS:
                if key == "readonly":
                    return await self.pending(
                        chat,
                        uid,
                        gid,
                        "readonly",
                        {"value": not cfg[key]},
                        "change group posting permissions?",
                        call,
                    )
                await self.db.set_setting(gid, key, not cfg[key])
                return await self.show(
                    chat, uid, await self.setting_screen(gid), callback=call
                )
            return await self.flow(
                m, {"kind": "setting", "gid": gid, "key": key}, f"new {key} value"
            )
        self.owner_only(call)
        if action == "panelpage":
            return await self.show(
                uid,
                uid,
                await self.enhanced_panel(parts[2], int(parts[3])),
                callback=call,
            )
        if action == "bc":
            op = parts[2]
            if op in {"groups", "users", "group", "user"}:
                await self.redis.delete(f"{PREFIX}broadcast:{uid}")
                if op in {"group", "user"}:
                    return await self.flow(
                        m,
                        {"kind": "broadcast_target", "target_type": op},
                        "reply with the group id"
                        if op == "group"
                        else "reply with the user id",
                    )
                return await self.flow(
                    m,
                    {"kind": "broadcast_content", "target": op},
                    "reply with your broadcast text, photo, video or document",
                )
            raw = await self.redis.get(f"{PREFIX}broadcast:{uid}")
            if not raw:
                raise ValueError("this broadcast draft expired ◡")
            state = json.loads(raw)
            if op == "cancel":
                await self.redis.delete(f"{PREFIX}broadcast:{uid}")
                return await self.show(
                    uid, uid, Screen("broadcast", "cancelled ˚"), callback=call
                )
            if op in {"pin", "forward"}:
                state[op] = not state.get(op, False)
                return await self.broadcast_options(uid, state, call)
            if op == "preview":
                await self.bot.copy_message(uid, state["source"], state["message"])
                return await self.pending(
                    uid,
                    uid,
                    0,
                    "v2_broadcast",
                    state,
                    f"send this preview to {state['target']}?\npin: {state.get('pin')} · forward attribution: {state.get('forward')}",
                    call,
                )
            raise ValueError("hmm, reopen the broadcast menu ◡")
        if action == "user":
            target = int(parts[2])
            return await self.show(
                uid,
                uid,
                Screen(
                    "user",
                    f"id: {target}",
                    rows=[
                        [
                            button(
                                "✦ global unban", f"v2:global:unban:{target}", "success"
                            )
                        ],
                        [button("← back", "panel:users")],
                        [button("⌦ global ban", f"v2:global:ban:{target}", "danger")],
                    ],
                ),
                callback=call,
            )
        if action == "global":
            if parts[2] not in {"ban", "unban"}:
                raise ValueError("choose ban or unban ◡")
            target = int(parts[3])
            if target in {self.owner, self.me.id} or target <= 0:
                raise ValueError("this member is protected ◡")
            return await self.pending(
                uid,
                uid,
                0,
                "v2_global",
                {"target": target, "unban": parts[2] == "unban"},
                f"{parts[2]} {target} in every recorded group?",
                call,
            )
        if action == "default":
            if parts[2] not in {"warnlimit", "warnaction", "warntime"}:
                raise ValueError("hmm, reopen warn defaults ◡")
            return await self.flow(
                m, {"kind": "warn_default", "key": parts[2]}, f"new {parts[2]} value"
            )
        if action == "appeal":
            op, table, ident = parts[2], parts[3], int(parts[4])
            col = appeal_column(table)
            row = await self.db.one(
                f"SELECT * FROM {table} WHERE {col}=? AND appeal_requested=1 AND resolved=0",
                [ident],
            )
            if not row:
                raise ValueError("that appeal has already been reviewed ◡")
            if op == "reject":
                return await self.flow(
                    m,
                    {"kind": "appeal_reason", "table": table, "id": ident},
                    "why wasn't this appeal approved?",
                )
            if op != "approve":
                raise ValueError("hmm, reopen appeals ◡")
            return await self.pending(
                uid,
                uid,
                0,
                "v2_appeal_approve",
                {"table": table, "id": ident},
                f"lift this {'ban' if table == 'bans' else 'mute'} in {row['group_id']}?",
                call,
            )
        if action in {"csv", "backup"}:
            return await self.owner_export(
                uid, parts[2] if action == "csv" else "backup"
            )
        if action in {
            "group",
            "groupcfg",
            "groupaudit",
            "groupleave",
            "groupblacklist",
        }:
            gid = int(parts[2])
            if action == "group":
                return await self.show(
                    uid, uid, await self.group_detail(gid), callback=call
                )
            if action == "groupcfg":
                cfg = await self.db.settings(gid)
                return await self.show(
                    uid,
                    uid,
                    Screen(
                        "group settings",
                        table=(
                            ["setting", "value"],
                            [[k, str(v)] for k, v in cfg.items()],
                        ),
                        rows=[[button("← back", f"v2:group:{gid}")]],
                    ),
                    callback=call,
                )
            if action == "groupaudit":
                rows = await self.db.query(
                    "SELECT log_id,admin_id,action,target_id FROM audit_log WHERE group_id=? ORDER BY log_id DESC LIMIT 1000",
                    [gid],
                )
                token = secrets.token_urlsafe(8)
                await self.redis.set(
                    f"{PREFIX}ownerlist:{token}",
                    json.dumps(
                        {
                            "title": "group audit",
                            "rows": rows,
                            "cols": ["log_id", "admin_id", "action", "target_id"],
                            "gid": gid,
                        }
                    ),
                    ex=300,
                )
                screen = await self.list_screen(
                    "group audit",
                    rows,
                    ["log_id", "admin_id", "action", "target_id"],
                    0,
                    f"v2:ownerlist:{token}",
                    f"v2:group:{gid}",
                )
                return await self.show(uid, uid, screen, callback=call)
            return await self.pending(
                uid,
                uid,
                0,
                "v2_group_exit",
                {"gid": gid, "blacklist": action == "groupblacklist"},
                f"{'blacklist and leave' if action == 'groupblacklist' else 'leave'} group {gid}?",
                call,
            )
        if action == "ownerlist":
            raw = await self.redis.get(f"{PREFIX}ownerlist:{parts[2]}")
            if not raw:
                raise ValueError("that list expired ◡")
            s = json.loads(raw)
            return await self.show(
                uid,
                uid,
                await self.list_screen(
                    s["title"],
                    s["rows"],
                    s["cols"],
                    int(parts[3]),
                    f"v2:ownerlist:{parts[2]}",
                    f"v2:group:{s['gid']}",
                ),
                callback=call,
            )
        raise ValueError("hmm, reopen the menu ◡")

    async def owner_export(self, uid: int, section: str) -> None:
        if uid != self.owner:
            raise ValueError("owner panel is private ᯓ")
        day = datetime.now(UTC).strftime("%Y-%m-%d")
        if section == "backup":
            data = {
                "format": "marie-backup-v2",
                "created_at": stamp(),
                "tables": {},
                "group_settings": {},
            }
            for table in SCHEMA:
                data["tables"][table] = await self.db.query(f"SELECT * FROM {table}")
            for row in await self.db.query("SELECT group_id FROM groups"):
                data["group_settings"][
                    str(row["group_id"])
                ] = await self.export_settings(row["group_id"])
            await self.bot.send_document(
                uid,
                T.BufferedInputFile(
                    json.dumps(data, ensure_ascii=False, indent=2).encode(),
                    filename=f"marie_backup_{day}.json",
                ),
            )
            return await self.show(
                uid,
                uid,
                Screen(
                    "backup",
                    "restore with /importsettings per group",
                    [
                        "extract the matching group_settings entry into its own JSON file",
                        "operational/history rows require a separate administrative restore; /importsettings does not restore the full backup",
                    ],
                ),
            )
        table = {"audit": "audit_log", "bans": "bans", "mutes": "mutes"}.get(section)
        if not table:
            raise ValueError("choose audit, bans or mutes ◡")
        rows = await self.db.query(f"SELECT * FROM {table} ORDER BY rowid")
        stream = io.StringIO()
        fields = (
            list(rows[0])
            if rows
            else [r["name"] for r in await self.db.query(f"PRAGMA table_info({table})")]
        )
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            # Protect spreadsheet users from formula injection in user-supplied reasons.
            writer.writerow(
                {
                    k: (
                        "'" + v
                        if isinstance(v, str)
                        and v[:1] in {"=", "+", "-", "@", "\t", "\r"}
                        else v
                    )
                    for k, v in row.items()
                }
            )
        await self.bot.send_document(
            uid,
            T.BufferedInputFile(
                stream.getvalue().encode("utf-8-sig"),
                filename=f"marie_{section}_{day}.csv",
            ),
        )

    async def panel_pending(
        self, r: dict[str, Any], call: T.CallbackQuery
    ) -> str | None:
        uid = r["actor"]
        p = r["payload"]
        op = r["op"]
        self.owner_only(call)
        if op == "v2_broadcast":
            job_id = secrets.token_urlsafe(10)
            p |= {"broadcast_id": job_id}
            sent = await self.show(
                uid,
                uid,
                Screen("broadcast", "sending... 0 / preparing audience"),
                callback=call,
            )
            p["progress"] = message_locator(sent, uid)
            await self.redis.enqueue_job(
                "broadcast_job", p, uid, _job_id=f"{PREFIX}broadcast-job:{job_id}"
            )
            await self.redis.delete(f"{PREFIX}broadcast:{uid}")
            return None
        if op == "v2_global":
            await self.redis.enqueue_job("global_moderation_job", p, uid)
            return "global action queued · progress will appear here ˚"
        if op in {"v2_appeal_approve", "v2_appeal_reject"}:
            table = p["table"]
            col = appeal_column(table)
            row = await self.db.one(
                f"SELECT * FROM {table} WHERE {col}=? AND appeal_requested=1 AND resolved=0 AND (expires_at IS NULL OR expires_at>?)",
                [p["id"], now()],
            )
            if not row:
                raise ValueError("that appeal was already reviewed or expired ◡")
            changed = await self.db.execute(
                f"UPDATE {table} SET appeal_requested=2 WHERE {col}=? AND appeal_requested=1",
                [p["id"]],
            )
            if not changed.rows_affected:
                raise ValueError("that appeal is already under review ◡")
            try:
                if op == "v2_appeal_approve":
                    action = (
                        "unban"
                        if table == "bans"
                        else "unshadowmute"
                        if row["is_shadow"]
                        else "unmute"
                    )
                    await self.action(
                        row["group_id"],
                        uid,
                        row["user_id"],
                        action,
                        "appeal approved",
                        automatic=True,
                    )
                    await self.db.execute(
                        f"UPDATE {table} SET appeal_requested=4 WHERE {col}=?",
                        [p["id"]],
                    )
                    notice = "appeal approved · your restriction has been lifted ✦"
                else:
                    await self.db.execute(
                        f"UPDATE {table} SET appeal_requested=3 WHERE {col}=?",
                        [p["id"]],
                    )
                    notice = "appeal not approved ◡\n" + p["reason"]
                try:
                    await self.show(
                        row["user_id"],
                        row["user_id"],
                        Screen("appeal", lines=notice.splitlines()),
                    )
                except TelegramAPIError:
                    await self.db.audit(
                        row["group_id"],
                        uid,
                        "appeal_notice_unavailable",
                        row["user_id"],
                    )
                await self.db.audit(
                    row["group_id"], uid, op, row["user_id"], {"record": p["id"]}
                )
            except Exception:
                await self.db.execute(
                    f"UPDATE {table} SET appeal_requested=1 WHERE {col}=? AND appeal_requested=2",
                    [p["id"]],
                )
                raise
            return "review saved ˚"
        if op == "v2_group_exit":
            gid = p["gid"]
            if p["blacklist"]:
                await self.db.execute(
                    "INSERT OR REPLACE INTO blacklisted_groups(group_id,reason,created_at) VALUES (?,'owner policy',?)",
                    [gid, now()],
                )
            await self.bot.leave_chat(gid)
            await self.db.execute("UPDATE groups SET active=0 WHERE group_id=?", [gid])
            await self.db.audit(
                gid, uid, "group_blacklist" if p["blacklist"] else "group_leave"
            )
            return "done ·͜·"
        raise ValueError("hmm, reopen the confirmation ◡")

    async def phase_callback(self, call: T.CallbackQuery) -> None:
        data = call.data.split(":")
        uid = call.from_user.id
        chat = call.message.chat.id
        m = call.message.model_copy(update={"from_user": call.from_user})
        if data[1] == "flowcancel":
            await self.redis.delete(f"{PREFIX}flow:{uid}:{chat}")
            return await self.show(
                chat, uid, Screen("marie", "cancelled ˚"), callback=call
            )
        if data[1] == "tree":
            raw = await self.redis.get(f"{PREFIX}tree:{data[2]}")
            if not raw:
                raise ValueError("that list expired ◡")
            s = json.loads(raw)
            if s["actor"] != uid or chat not in {s["chat"], uid}:
                raise ValueError("this list isn't yours ◡")
            await self.require_admin(s["gid"], uid)
            return await self.show(
                chat,
                uid,
                tree_screen(s["title"], s["lines"], int(data[3]), f"v2:tree:{data[2]}"),
                callback=call,
            )
        if data[1] == "proposal":
            op, gid, pid = data[2], int(data[3]), int(data[4])
            await self.require_admin(gid, uid, "can_promote_members")
            row = await self.db.one(
                "SELECT * FROM admin_proposals WHERE proposal_id=? AND group_id=? AND status='pending'",
                [pid, gid],
            )
            if not row:
                raise ValueError("that proposal has already been reviewed ◡")
            if op == "reject":
                return await self.flow(
                    m,
                    {
                        "kind": "proposal_reject",
                        "gid": gid,
                        "id": pid,
                        "proposer": row["proposer_id"],
                    },
                    "why wasn't this proposal approved?",
                )
            if op != "approve":
                raise ValueError("hmm, reopen the proposal ◡")
            return await self.pending(
                chat,
                uid,
                gid,
                "v2_proposal_approve",
                {"id": pid},
                f"approve {row['action']} for {row['target_id']}?\n{row['reason']}",
                call,
            )
        return await self.panel_callback(call)

    async def phase_pending(
        self, r: dict[str, Any], call: T.CallbackQuery
    ) -> str | None:
        op, p, gid, uid = r["op"], r["payload"], r["gid"], r["actor"]
        if op == "v2_drip_remove":
            await self.require_admin(gid, uid)
            await self.db.execute(
                "DELETE FROM drip_sequences WHERE group_id=? AND seq_id=?",
                [gid, p["seq_id"]],
            )
        elif op == "v2_ladder":
            await self.require_admin(gid, uid, "can_restrict_members")
            await self.db.set_setting(gid, "warn_ladder", validate_ladder(p["steps"]))
        elif op == "v2_template_save":
            await self.require_admin(gid, uid)
            await self.db.batch(
                [
                    (
                        "DELETE FROM mod_templates WHERE group_id=? AND name=?",
                        [gid, p["name"]],
                    ),
                    (
                        "INSERT INTO mod_templates(group_id,admin_id,name,action,reason,duration) VALUES (?,?,?,?,?,?)",
                        [gid, uid, p["name"], p["action"], p["reason"], p["seconds"]],
                    ),
                ]
            )
        elif op == "v2_proposal_reject":
            await self.require_admin(gid, uid, "can_promote_members")
            row = await self.db.one(
                "SELECT * FROM admin_proposals WHERE proposal_id=? AND group_id=? AND status='pending'",
                [p["id"], gid],
            )
            if not row:
                raise ValueError("that proposal was already reviewed ◡")
            changed = await self.db.execute(
                "UPDATE admin_proposals SET status='rejected',reject_reason=?,reviewed_by=? WHERE proposal_id=? AND group_id=? AND status='pending'",
                [p["reason"], uid, p["id"], gid],
            )
            if not changed.rows_affected:
                raise ValueError("that proposal was already reviewed ◡")
            await self.show(
                gid,
                row["proposer_id"],
                Screen("proposal", lines=["not approved", p["reason"]]),
            )
        elif op == "v2_proposal_approve":
            await self.require_admin(gid, uid, "can_promote_members")
            await self.require_admin(gid, uid, "can_restrict_members")
            row = await self.db.one(
                "SELECT * FROM admin_proposals WHERE proposal_id=? AND group_id=? AND status='pending'",
                [p["id"], gid],
            )
            if not row:
                raise ValueError("that proposal was already reviewed ◡")
            taken = await self.db.execute(
                "UPDATE admin_proposals SET status='reviewing',reviewed_by=? WHERE proposal_id=? AND group_id=? AND status='pending'",
                [uid, p["id"], gid],
            )
            if not taken.rows_affected:
                raise ValueError("another senior admin is reviewing this ◡")
            try:
                await self.action(
                    gid,
                    uid,
                    row["target_id"],
                    row["action"],
                    row["reason"],
                    row["duration"],
                )
            except Exception:
                await self.db.execute(
                    "UPDATE admin_proposals SET status='pending' WHERE proposal_id=? AND status='reviewing'",
                    [p["id"]],
                )
                raise
            await self.db.execute(
                "UPDATE admin_proposals SET status='approved' WHERE proposal_id=?",
                [p["id"]],
            )
            await self.show(gid, row["proposer_id"], Screen("proposal", "approved ✦"))
        elif op == "v2_bio_remove":
            await self.require_admin(gid, uid, "can_restrict_members")
            await self.db.execute(
                "DELETE FROM bioblacklist WHERE group_id=? AND pattern=?",
                [gid, p["pattern"]],
            )
        elif op == "v2_importfbans":
            await self.fed_authorize(p["fed"], uid)
            membership = await self.db.one(
                "SELECT fed_id FROM fed_members WHERE group_id=?", [gid]
            )
            if not membership or membership["fed_id"] != p["fed"]:
                raise ValueError("this group's federation changed ◡")
            raw = await self.redis.get(f"{PREFIX}fed-import:{p['token']}")
            if not raw:
                raise ValueError("that import expired; send the file again ◡")
            entries = json.loads(raw)
            for start in range(0, len(entries), 100):
                await self.db.batch(
                    [
                        (
                            "INSERT INTO fed_bans(fed_id,user_id,reason,banned_by,banned_at) VALUES (?,?,?,?,?) ON CONFLICT(fed_id,user_id) DO UPDATE SET reason=excluded.reason,banned_by=excluded.banned_by,banned_at=excluded.banned_at",
                            [p["fed"], x["user_id"], x["reason"], uid, now()],
                        )
                        for x in entries[start : start + 100]
                    ]
                )
            await self.redis.delete(f"{PREFIX}fed-import:{p['token']}")
            await self.audit(
                gid,
                uid,
                "fed_import",
                details={"imported": len(entries), "skipped": p["skipped"]},
            )
            for group in await self.fed_targets(p["fed"]):
                await self.redis.enqueue_job("sync_federation", group["group_id"])
            await self.show(
                call.message.chat.id,
                uid,
                Screen(
                    "import report",
                    table=(
                        ["imported", "skipped"],
                        [[str(len(entries)), str(p["skipped"])]],
                    ),
                ),
                callback=call,
            )
            return None
        elif op == "v2_permapin":
            await self.require_admin(gid, uid, "can_pin_messages")
            if p["remove"]:
                await self.db.execute("DELETE FROM permapin WHERE group_id=?", [gid])
            else:
                await self.bot.pin_chat_message(
                    gid, p["message"], disable_notification=True
                )
                await self.db.execute(
                    "INSERT INTO permapin(group_id,message_id) VALUES (?,?) ON CONFLICT(group_id) DO UPDATE SET message_id=excluded.message_id",
                    [gid, p["message"]],
                )
        elif op == "v2_purgeto":
            await self.require_admin(gid, uid, "can_delete_messages")
            submitted = 0
            for start in range(p["first"], p["last"] + 1, 100):
                ids = list(range(start, min(start + 100, p["last"] + 1)))
                await self.bot.delete_messages(gid, ids)
                submitted += len(ids)
            await self.audit(
                gid,
                uid,
                "purge",
                details={"submitted_ids": submitted, "exact_deleted_count": None},
            )
            return f"deletion requested for {submitted} ids · unavailable messages skipped ◡"
        elif op == "v2_delreactions":
            await self.require_admin(gid, uid, "can_delete_messages")
            rows = await self.db.query(
                "SELECT * FROM reaction_seen WHERE group_id=? AND message_id=?",
                [gid, p["message"]],
            )
            done = 0
            for row in rows:
                try:
                    await self.bot.delete_message_reaction(
                        gid,
                        p["message"],
                        user_id=row["actor_id"]
                        if row["actor_type"] == "user"
                        else None,
                        actor_chat_id=row["actor_id"]
                        if row["actor_type"] == "chat"
                        else None,
                    )
                    done += 1
                    await self.db.execute(
                        "DELETE FROM reaction_seen WHERE group_id=? AND message_id=? AND actor_id=? AND actor_type=?",
                        [gid, p["message"], row["actor_id"], row["actor_type"]],
                    )
                except TelegramAPIError:
                    LOG.warning(
                        "reaction could not be removed group=%s message=%s",
                        gid,
                        p["message"],
                    )
            return "✦ reactions cleared ·͜·" if done else "⌁ none to clear ◡"
        else:
            return await self.panel_pending(r, call)
        await self.audit(gid, uid, "settings_change", details={"operation": op})
        return "done ·͜·"

    async def command(self, m: T.Message, cmd: str, text: str) -> None:
        uid = m.from_user.id
        args = shlex.split(text)
        if cmd in UNSUPPORTED:
            raise ValueError(UNSUPPORTED[cmd])
        if cmd in PHASE1_COMMANDS:
            return await self.phase1_command(m, cmd, text, args)
        if cmd in PHASE3_COMMANDS:
            return await self.phase3_command(m, cmd, text, args)
        if cmd in DEFERRED:
            raise ValueError(DEFERRED[cmd] + " ◡")
        if cmd not in COMMANDS:
            raise ValueError("hmm, try /help ◡")
        if cmd == "start":
            return await self.home(m)
        if cmd in {"helpadmin", "helpuser"} and not args:
            lines = []
            sections = [
                (
                    name,
                    [c for c in names.split() if (c in PUBLIC) == (cmd == "helpuser")],
                )
                for name, names in SECTIONS.items()
                if name != "owner"
            ]
            sections = [(name, names) for name, names in sections if names]
            for i, (name, names) in enumerate(sections):
                lines.append(f"{'└' if i == len(sections) - 1 else '├'} {name}")
                for j, name in enumerate(names):
                    lines.append(
                        f"{'   ' if i == len(sections) - 1 else '│  '}{'└' if j == len(names) - 1 else '├'} /{name}"
                    )
            return await self.show(
                m.chat.id,
                uid,
                Screen(
                    "admin commands" if cmd == "helpadmin" else "member commands",
                    lines=lines,
                    rows=[[button("← back", "sections")]],
                    expandable=True,
                ),
            )
        if cmd in {"help", "helpadmin", "helpuser", "menu"}:
            return await self.show(
                m.chat.id,
                uid,
                self.help_screen(args[0])
                if args
                else await self.section_menu(m.chat.id),
            )
        if cmd == "about":
            return await self.say(
                m,
                await self.db.bot_setting(
                    "description", "your gentle group guardian ·͜·"
                ),
            )
        if cmd == "id":
            return await self.say(
                m,
                f"you: {uid} · chat: {m.chat.id}"
                + (
                    f" · reply: {m.reply_to_message.from_user.id}"
                    if m.reply_to_message and m.reply_to_message.from_user
                    else ""
                ),
            )
        if cmd == "ping":
            started = time.perf_counter()
            await self.redis.ping()
            await self.db.one("SELECT 1")
            return await self.say(
                m,
                f"{(time.perf_counter() - started) * 1000:.0f} ms · redis + database round trip ⋆",
            )
        if cmd in {"panel", "broadcast"}:
            self.owner_only(m)
            if cmd == "broadcast":
                if not m.reply_to_message:
                    raise ValueError(
                        "reply to content: /broadcast groups|users|chat_id ◡"
                    )
                target = args[0] if args else "groups"
                if target not in {"groups", "users"}:
                    int(target)
                return await self.pending(
                    m.chat.id,
                    uid,
                    0,
                    "broadcast",
                    {
                        "source": m.chat.id,
                        "message": m.reply_to_message.message_id,
                        "target": target,
                    },
                    f"send this content to {target}?",
                )
            await self.video(uid, "panel_menu_video")
            return await self.show(uid, uid, await self.panel_screen("home"))
        if cmd == "connect":
            gid = int(args[0])
            if m.chat.type != "private" or gid >= 0:
                raise ValueError("use /connect group_id in my private chat ◡")
            await self.require_admin(gid, uid)
            await self.redis.set(f"{PREFIX}connection:{uid}", str(gid), ex=86400)
            return await self.say(m, "connected ˚")
        if cmd == "disconnect":
            await self.redis.delete(f"{PREFIX}connection:{uid}")
            return await self.say(m, "disconnected ˚")
        if cmd == "connection":
            return await self.say(
                m,
                "group: "
                + str(await self.redis.get(f"{PREFIX}connection:{uid}") or "none"),
            )
        gid = await self.context(m)
        if cmd not in PUBLIC:
            await self.require_admin(gid, uid)
        cfg = await self.db.settings(gid)
        if cmd == "settings":
            return await self.show(m.chat.id, uid, await self.setting_screen(gid))
        if cmd in BOOL_SETTINGS | set(INT_SETTINGS) | set(CHOICE_SETTINGS) | {
            "setflood",
            "captcharules",
            "setblocklistreason",
        }:
            key = "flood" if cmd == "setflood" else cmd
            if not args:
                return await self.say(m, f"{key}: {cfg[key]}")
            if key in BOOL_SETTINGS:
                if args[0] not in {"on", "off"}:
                    raise ValueError("use on or off ◡")
                value: Any = args[0] == "on"
            elif key in INT_SETTINGS:
                value = int(args[0])
            else:
                value = (
                    text if key in {"captcharules", "setblocklistreason"} else args[0]
                )
            validate_setting(key, value)
            if key == "readonly":
                return await self.pending(
                    m.chat.id,
                    uid,
                    gid,
                    "readonly",
                    {"value": value},
                    f"set read-only {args[0]}? affects all regular members",
                )
            await self.db.set_setting(gid, key, value)
            await self.audit(gid, uid, "settings_change", details={key: value})
            return await self.say(m, "saved ˚")
        if cmd in {
            "aispam",
            "aitoxic",
            "aipii",
            "aimode",
            "aithreshold",
            "aisettings",
            "ailog",
        }:
            return await self.ai_command(m, gid, cmd, args, cfg)
        moderation = set(
            "ban tban unban kick mute tmute unmute warn softban shadowmute unshadowmute strike anonban anonmute anonkick anonwarn".split()
        )
        if cmd in moderation:
            target, remaining = await self.resolve(m, args)
            kind = cmd.removeprefix("anon")
            seconds = 0
            if kind in {"tban", "tmute"}:
                if not remaining:
                    raise ValueError("add a duration, like 1h ◡")
                seconds, remaining = duration(remaining[0]), remaining[1:]
            elif (
                kind == "mute"
                and remaining
                and re.fullmatch(r"\d+[smhdw]", remaining[0])
            ):
                seconds, remaining = duration(remaining[0]), remaining[1:]
            reason = " ".join(remaining) or "no reason given"
            await self.require_admin(gid, uid, "can_restrict_members")
            await self.safe_target(gid, target)
            return await self.pending(
                m.chat.id,
                uid,
                gid,
                "moderate",
                {"target": target, "kind": kind, "reason": reason, "seconds": seconds},
                f"{kind} {target}? reason: {reason}",
            )
        if cmd in {"bulkban", "bulkmute", "bulkwarn"}:
            targets = []
            while args and (args[0].startswith("@") or args[0].isdigit()):
                target, args = await self.resolve(m, args)
                targets.append(target)
                if len(targets) > 20:
                    raise ValueError("20 members per batch maximum ◡")
            if not targets:
                raise ValueError("give member ids or usernames ◡")
            seconds = duration(args.pop(0)) if cmd == "bulkmute" and args else 0
            return await self.pending(
                m.chat.id,
                uid,
                gid,
                "bulk",
                {
                    "targets": targets,
                    "kind": cmd[4:],
                    "reason": " ".join(args),
                    "seconds": seconds,
                },
                f"{cmd[4:]} {len(targets)} members? ids: {', '.join(map(str, targets))}",
            )
        if cmd in {"warns", "strikes", "unwarn", "resetwarns"}:
            target, _ = await self.resolve(m, args)
            if cmd in {"unwarn", "resetwarns"}:
                return await self.pending(
                    m.chat.id,
                    uid,
                    gid,
                    "clearwarn",
                    {"target": target, "all": cmd == "resetwarns"},
                    f"clear {'all active warnings' if cmd == 'resetwarns' else 'latest warning'} for {target}?",
                )
            rows = await self.db.query(
                "SELECT warn_id,reason,expires_at FROM warnings WHERE group_id=? AND user_id=? AND is_expired=0 AND expires_at>? ORDER BY created_at DESC LIMIT 100",
                [gid, target, now()],
            )
            if cmd == "strikes":
                rows = [r for r in rows if r["reason"].startswith("strike · ")]
            return await self.show(
                m.chat.id, uid, await self.list_screen(cmd, rows, ["warn_id", "reason"])
            )
        if cmd == "undo":
            row = await self.db.one(
                "SELECT * FROM cases WHERE group_id=? AND admin_id=? AND created_at>? AND status='applied' AND resolved=0 ORDER BY case_id DESC LIMIT 1",
                [gid, uid, now() - 60],
            )
            if not row or row["action"] not in {
                "ban",
                "tban",
                "mute",
                "tmute",
                "shadowmute",
                "warn",
                "strike",
            }:
                raise ValueError("nothing reversible in the last 60s ◡")
            return await self.pending(
                m.chat.id,
                uid,
                gid,
                "undo",
                {"case": row},
                f"reverse case {row['case_id']}? deleted messages cannot be restored",
            )
        if cmd in {
            "case",
            "cases",
            "casefilter",
            "modstats",
            "adminperf",
            "casetag",
            "mynote",
        }:
            if cmd in {"casetag", "mynote"}:
                case_id = int(args[0])
                row = await self.db.one(
                    "SELECT * FROM cases WHERE case_id=? AND group_id=?", [case_id, gid]
                )
                if not row:
                    raise ValueError("couldn't find that case ◡")
                field = "tags_json" if cmd == "casetag" else "notes_json"
                items = json.loads(row[field])
                items.append(" ".join(args[1:])[:1000])
                await self.db.execute(
                    f"UPDATE cases SET {field}=? WHERE case_id=? AND group_id=?",
                    [json.dumps(items), case_id, gid],
                )
                return await self.say(m, "noted ✦")
            if cmd == "case":
                rows = await self.db.query(
                    "SELECT * FROM cases WHERE case_id=? AND group_id=?",
                    [int(args[0]), gid],
                )
                cols = ["case_id", "user_id", "action", "reason", "status"]
            elif cmd == "cases":
                target, _ = await self.resolve(m, args)
                rows = await self.db.query(
                    "SELECT * FROM cases WHERE group_id=? AND user_id=? ORDER BY case_id DESC LIMIT 1000",
                    [gid, target],
                )
                cols = ["case_id", "action", "reason", "status"]
            elif cmd == "casefilter":
                rows = await self.db.query(
                    "SELECT * FROM cases WHERE group_id=? AND EXISTS (SELECT 1 FROM json_each(cases.tags_json) WHERE value=?) ORDER BY case_id DESC LIMIT 1000",
                    [gid, args[0]],
                )
                cols = ["case_id", "user_id", "action", "reason"]
            else:
                rows = await self.db.query(
                    "SELECT admin_id,action,count(*) total FROM cases WHERE group_id=? AND status='applied' GROUP BY admin_id,action ORDER BY total DESC LIMIT 1000",
                    [gid],
                )
                cols = ["admin_id", "action", "total"]
            return await self.show_snapshot(m, cmd, rows, cols)
        if cmd == "info":
            target, _ = (
                await self.resolve(m, args) if args or m.reply_to_message else (uid, [])
            )
            member = await self.bot.get_chat_member(gid, target)
            row = await self.db.one(
                "SELECT xp,joined_at FROM group_members WHERE group_id=? AND user_id=?",
                [gid, target],
            )
            return await self.show(
                m.chat.id,
                uid,
                Screen(
                    "member",
                    member.user.full_name,
                    [
                        f"id: {target}",
                        f"status: {member.status}",
                        f"xp: {row['xp'] if row else 0}",
                    ],
                ),
            )
        if cmd == "groupinfo":
            chat = await self.bot.get_chat(gid)
            total = await self.bot.get_chat_member_count(gid)
            return await self.show(
                m.chat.id,
                uid,
                Screen(
                    "group",
                    chat.title or str(gid),
                    [
                        f"id: {gid}",
                        f"members: {total}",
                        f"forum: {chat.is_forum or False}",
                    ],
                ),
            )
        if cmd in {"lock", "unlock", "locks"}:
            if cmd != "locks":
                if not args or args[0] not in LOCK_TYPES:
                    raise ValueError("types: " + " · ".join(sorted(LOCK_TYPES)))
                if args[0] in {"topic_create", "topic_delete"}:
                    raise ValueError(
                        "topic permissions must be managed in Telegram; deletion events cannot be reversed ◡"
                    )
                await self.db.execute(
                    "INSERT INTO locks(group_id,lock_type,is_locked) VALUES (?,?,?) ON CONFLICT(group_id,lock_type) DO UPDATE SET is_locked=excluded.is_locked",
                    [gid, args[0], int(cmd == "lock")],
                )
                await self.audit(gid, uid, "settings_change", details={cmd: args[0]})
            rows = await self.db.query(
                "SELECT lock_type,is_locked FROM locks WHERE group_id=?", [gid]
            )
            return await self.show_snapshot(
                m, "locks", rows, ["lock_type", "is_locked"]
            )
        if cmd in {"allowlist", "rmallowlist"}:
            items = cfg["allowlist"]
            if not args or args[0] == "list":
                return await self.show(
                    m.chat.id, uid, Screen("allowlist", lines=items or ["empty ｡"])
                )
            phrase = " ".join(args[1:] if args[0] == "add" else args)
            if cmd == "rmallowlist":
                items = [x for x in items if x != phrase]
            elif phrase not in items:
                items.append(phrase)
            await self.db.set_setting(gid, "allowlist", items)
            return await self.say(m, "saved ˚")
        if cmd in {
            "addblocklist",
            "rmblocklist",
            "blocklist",
            "rmblocklistall",
            "filter",
            "filters",
            "rmfilter",
            "rmfilters",
            "save",
            "get",
            "notes",
            "rmnote",
            "rmnotes",
            "clearallnotes",
        }:
            return await self.content_command(m, gid, cmd, args, text)
        if cmd in {
            "setwelcome",
            "setgoodbye",
            "setrules",
            "welcome",
            "goodbye",
            "rules",
            "clearwelcome",
            "cleargoodbye",
            "clearrules",
            "welcomebutton",
            "clearwelcomebuttons",
            "setdrip",
            "drip",
            "cleardrip",
        }:
            return await self.greeting_command(m, gid, cmd, args, text)
        if cmd in {
            "tag",
            "untag",
            "tags",
            "approve",
            "unapprove",
            "approved",
            "unapproveall",
            "addxp",
            "removexp",
            "xp",
            "xpleaderboard",
            "setrank",
            "ranks",
            "rank",
        }:
            return await self.member_command(m, gid, cmd, args)
        if cmd in {
            "banpack",
            "unbanpack",
            "bannedpacks",
            "banforward",
            "unbanforward",
            "bannedforwards",
        }:
            packs = "pack" in cmd
            table, col = (
                ("sticker_pack_bans", "pack_name")
                if packs
                else ("forward_source_bans", "source_chat_id")
            )
            if cmd.startswith("banned"):
                rows = await self.db.query(
                    f"SELECT {col},banned_by FROM {table} WHERE group_id=?", [gid]
                )
                return await self.show_snapshot(m, cmd, rows, [col, "banned_by"])
            if cmd == "banpack":
                sticker = m.reply_to_message.sticker if m.reply_to_message else None
                if not sticker or not sticker.set_name:
                    raise ValueError("reply to a sticker from a pack ◡")
                value = sticker.set_name
            else:
                value = args[0] if packs else int(args[0])
            if cmd.startswith("unban"):
                await self.db.execute(
                    f"DELETE FROM {table} WHERE group_id=? AND {col}=?", [gid, value]
                )
            else:
                await self.db.execute(
                    f"INSERT OR REPLACE INTO {table}(group_id,{col},banned_by,banned_at) VALUES (?,?,?,?)",
                    [gid, value, uid, now()],
                )
            return await self.say(m, "saved ˚")
        if cmd in {"raidmode", "lockreactions", "allowreactions"}:
            if cmd == "raidmode":
                await self.redis.set(f"{PREFIX}raid:{gid}", "1", ex=cfg["raidtime"])
                return await self.say(m, "raid protection active ✦")
            return await self.pending(
                m.chat.id,
                uid,
                gid,
                "reaction_permissions",
                {"value": cmd == "allowreactions"},
                f"{'allow' if cmd == 'allowreactions' else 'restrict'} member reactions?",
            )
        if cmd in {
            "schedule",
            "schedulerecur",
            "schedules",
            "cancelschedule",
            "countdown",
            "poll",
            "quiz",
            "closepoll",
            "checklist",
            "checklists",
            "pin",
            "unpin",
            "unpinall",
        }:
            return await self.schedule_content(m, gid, cmd, args, text)
        if cmd in {
            "newfed",
            "renamefed",
            "delfed",
            "fedpromote",
            "feddemote",
            "joinfed",
            "leavefed",
            "fban",
            "unfban",
            "fedstat",
            "fbanlist",
            "subfed",
            "unsubfed",
            "quietfed",
            "fedinfo",
            "fedadmins",
            "myfed",
        }:
            return await self.federation_command(m, gid, cmd, args, text)
        if cmd in {
            "report",
            "reports",
            "claim",
            "unclaim",
            "duty",
            "shiftnote",
            "shiftnotes",
        }:
            return await self.workflow_command(m, gid, cmd, args, text)
        if cmd in {"purge", "spurge", "purgefrom", "del"}:
            await self.require_admin(gid, uid, "can_delete_messages")
            start = (
                int(args[0])
                if cmd == "purgefrom"
                else (m.reply_to_message.message_id if m.reply_to_message else 0)
            )
            if not start or m.chat.id != gid:
                raise ValueError("reply to the first message in the group ◡")
            end = start if cmd == "del" else m.message_id
            if not 0 <= end - start < 1000:
                raise ValueError("maximum 1000 messages per purge ◡")
            return await self.pending(
                m.chat.id,
                uid,
                gid,
                "purge",
                {"start": start, "end": end},
                f"delete message ids {start}–{end}? deletion cannot be undone",
            )
        if cmd in {
            "newtopic",
            "renametopic",
            "closetopic",
            "reopentopic",
            "deletetopic",
            "settopiclog",
        }:
            await self.require_admin(gid, uid, "can_manage_topics")
            topic = m.message_thread_id
            if cmd == "newtopic":
                await self.bot.create_forum_topic(gid, text[:128])
            elif cmd == "settopiclog":
                topic = int(args[0])
                await self.db.execute(
                    "UPDATE groups SET log_topics_json=? WHERE group_id=?",
                    [json.dumps({"ban": topic, "warn": topic, "mute": topic}), gid],
                )
            elif not topic:
                raise ValueError("use this inside a topic ◡")
            elif cmd == "deletetopic":
                return await self.pending(
                    m.chat.id,
                    uid,
                    gid,
                    "delete_topic",
                    {"topic": topic},
                    "delete this topic and all its messages?",
                )
            elif cmd == "renametopic":
                await self.bot.edit_forum_topic(gid, topic, name=text[:128])
            else:
                await (
                    self.bot.close_forum_topic
                    if cmd == "closetopic"
                    else self.bot.reopen_forum_topic
                )(gid, topic)
            return await self.say(m, "done ·͜·")
        if cmd in {"setlog", "unsetlog", "logchannel", "logcategories"}:
            if cmd == "setlog":
                channel = int(args[0]) if args else gid
                await self.require_admin(channel, uid)
                await self.db.execute(
                    "UPDATE groups SET log_channel_id=? WHERE group_id=?",
                    [channel, gid],
                )
            elif cmd == "unsetlog":
                await self.db.execute(
                    "UPDATE groups SET log_channel_id=NULL WHERE group_id=?", [gid]
                )
            elif cmd == "logcategories" and args:
                await self.db.set_setting(gid, "log_categories", args)
            row = await self.db.one(
                "SELECT log_channel_id FROM groups WHERE group_id=?", [gid]
            )
            return await self.say(m, f"log: {row['log_channel_id'] if row else 'none'}")
        if cmd in {"cleancommandtypes", "cleanservicetypes"}:
            await self.db.set_setting(gid, cmd, args)
            return await self.say(m, "saved ˚")
        if cmd == "keepcommand":
            await self.redis.set(f"{PREFIX}keep:{gid}:{uid}", "1", ex=300)
            return await self.say(m, "I'll keep your next command ˚")
        if cmd == "exportsettings":
            data = await self.export_settings(gid)
            return await self.bot.send_document(
                uid,
                T.BufferedInputFile(
                    json.dumps(data, indent=2).encode(), filename="marie-settings.json"
                ),
            )
        if cmd == "importsettings":
            reply = m.reply_to_message
            if (
                not reply
                or not reply.document
                or (reply.document.file_size or 0) > MAX_FILE
            ):
                raise ValueError("reply to a settings JSON file under 1 MB ◡")
            stream = io.BytesIO()
            await self.bot.download(reply.document, destination=stream)
            data = self.validate_import(json.loads(stream.getvalue()))
            changes = [k for k, v in data.settings.items() if cfg.get(k) != v]
            return await self.pending(
                m.chat.id,
                uid,
                gid,
                "import",
                {"data": data.model_dump()},
                "replace settings and content? changed: "
                + (", ".join(changes) or "content tables only"),
            )
        if cmd == "reset":
            return await self.pending(
                m.chat.id,
                uid,
                gid,
                "reset_first",
                {},
                "reset settings, filters, notes, blocklist, locks, greetings, rules, and AI settings? cases and audit history stay",
            )
        raise ValueError("this command is not available yet ◡")

    async def show_snapshot(
        self, m: T.Message, title: str, rows: list[dict[str, Any]], columns: list[str]
    ) -> None:
        token = secrets.token_urlsafe(8)
        await self.redis.set(
            f"{PREFIX}list:{token}",
            json.dumps(
                {
                    "actor": m.from_user.id,
                    "chat": m.chat.id,
                    "scope": await self.context(m),
                    "title": title,
                    "rows": rows[:1000],
                    "columns": columns,
                }
            ),
            ex=300,
        )
        await self.show(
            m.chat.id,
            m.from_user.id,
            await self.list_screen(
                title, rows, columns, callback_prefix=f"page:{token}"
            ),
        )

    async def content_command(
        self, m: T.Message, gid: int, cmd: str, args: list[str], text: str
    ) -> None:
        uid = m.from_user.id
        if cmd == "get":
            row = await self.db.one(
                "SELECT * FROM notes WHERE group_id=? AND name=?", [gid, args[0]]
            )
            if not row:
                raise ValueError("couldn't find that note ◡")
            await self.show(
                m.chat.id,
                uid,
                Screen(row["name"], lines=[row["content"] or ""]),
                private=False,
            )
            if row["media_id"] and row["media_type"] in {
                "photo",
                "video",
                "document",
                "animation",
            }:
                await getattr(self.bot, f"send_{row['media_type']}")(
                    chat_id=m.chat.id, **{row["media_type"]: row["media_id"]}
                )
            return
        if cmd in {"filters", "notes", "blocklist"}:
            table = cmd
            cols = {
                "filters": ["keyword", "action", "reply_text"],
                "notes": ["name", "content"],
                "blocklist": ["phrase", "match_type"],
            }[cmd]
            rows = await self.db.query(
                f"SELECT * FROM {table} WHERE group_id=? LIMIT 1000", [gid]
            )
            return await self.show_snapshot(m, cmd, rows, cols)
        if cmd in {"rmfilters", "rmnotes", "clearallnotes", "rmblocklistall"}:
            table = (
                "filters"
                if cmd == "rmfilters"
                else "blocklist"
                if cmd == "rmblocklistall"
                else "notes"
            )
            return await self.pending(
                m.chat.id,
                uid,
                gid,
                "clear_table",
                {"table": table},
                f"delete all {table} in this group?",
            )
        if cmd in {"rmfilter", "rmnote", "rmblocklist"}:
            table, col = {
                "rmfilter": ("filters", "keyword"),
                "rmnote": ("notes", "name"),
                "rmblocklist": ("blocklist", "phrase"),
            }[cmd]
            return await self.pending(
                m.chat.id,
                uid,
                gid,
                "delete_entry",
                {"table": table, "column": col, "value": text},
                f"delete {text} from {table}?",
            )
        if not args:
            raise ValueError("add a name or phrase ◡")
        if cmd == "addblocklist":
            mode = "substring"
            if (
                args[0] in {"exact", "substring", "wildcard", "prefix"}
                and len(args) > 1
            ):
                mode, args = args[0], args[1:]
            phrase = " ".join(args)
            if len(phrase) > 200:
                raise ValueError("200 characters maximum ◡")
            await self.db.execute(
                "INSERT INTO blocklist(group_id,phrase,match_type,created_at) VALUES (?,?,?,?) ON CONFLICT(group_id,phrase) DO UPDATE SET match_type=excluded.match_type",
                [gid, phrase, mode, now()],
            )
        elif cmd == "filter":
            keyword, response = args[0], " ".join(args[1:])
            allow = "{allow_bot}" in response
            response = response.replace("{allow_bot}", "").strip()
            await self.db.execute(
                "INSERT INTO filters(group_id,keyword,action,reply_text,allow_bots,created_at) VALUES (?,?,?,?,?,?) ON CONFLICT(group_id,keyword) DO UPDATE SET action=excluded.action,reply_text=excluded.reply_text,allow_bots=excluded.allow_bots",
                [
                    gid,
                    keyword,
                    "reply" if response else "delete",
                    response,
                    int(allow),
                    now(),
                ],
            )
        elif cmd == "save":
            name, content = args[0], " ".join(args[1:])
            media_type, media_id = media_ref(m.reply_to_message)
            if not content and m.reply_to_message:
                content = m.reply_to_message.text or m.reply_to_message.caption or ""
            if not re.fullmatch(r"[\w-]{1,40}", name) or len(content) > 3000:
                raise ValueError(
                    "use a short note name and content under 3000 characters ◡"
                )
            await self.db.execute(
                "INSERT INTO notes(group_id,name,content,media_id,media_type,created_at) VALUES (?,?,?,?,?,?) ON CONFLICT(group_id,name) DO UPDATE SET content=excluded.content,media_id=excluded.media_id,media_type=excluded.media_type",
                [gid, name, content, media_id, media_type, now()],
            )
        await self.audit(gid, uid, "settings_change", details={"command": cmd})
        await self.say(m, "saved ˚")

    async def greeting_command(
        self, m: T.Message, gid: int, cmd: str, args: list[str], text: str
    ) -> None:
        if cmd in {"setdrip", "drip", "cleardrip"}:
            if cmd == "setdrip":
                seconds = duration(args[0])
                await self.db.execute(
                    "INSERT INTO drip_sequences(group_id,delay_seconds,content_json,position) VALUES (?,?,?,0)",
                    [gid, seconds, json.dumps({"text": " ".join(args[1:])[:3000]})],
                )
            elif cmd == "cleardrip":
                return await self.pending(
                    m.chat.id,
                    m.from_user.id,
                    gid,
                    "clear_table",
                    {"table": "drip_sequences"},
                    "remove every drip sequence?",
                )
            rows = await self.db.query(
                "SELECT seq_id,delay_seconds,content_json FROM drip_sequences WHERE group_id=?",
                [gid],
            )
            return await self.show_snapshot(
                m, "drip", rows, ["seq_id", "delay_seconds", "content_json"]
            )
        if cmd in {"welcomebutton", "clearwelcomebuttons"}:
            row = await self.db.one(
                "SELECT buttons_json FROM welcomes WHERE group_id=?", [gid]
            )
            items = json.loads(row["buttons_json"]) if row else []
            if cmd == "clearwelcomebuttons":
                items = []
            else:
                if len(args) < 2 or not valid_url(args[-1]):
                    raise ValueError("use /welcomebutton label https://link ◡")
                items.append({"text": " ".join(args[:-1]), "url": args[-1]})
                if len(items) > 6:
                    raise ValueError("six welcome buttons maximum ◡")
            await self.db.execute(
                "INSERT INTO welcomes(group_id,welcome_text,buttons_json) VALUES (?,'welcome {first} ·͜·',?) ON CONFLICT(group_id) DO UPDATE SET buttons_json=excluded.buttons_json",
                [gid, json.dumps(items)],
            )
            return await self.say(m, "saved ˚")
        base = cmd.removeprefix("set").removeprefix("clear")
        table, column = {
            "welcome": ("welcomes", "welcome_text"),
            "goodbye": ("goodbyes", "goodbye_text"),
            "rules": ("rules", "rules_text"),
        }[base]
        if cmd.startswith("clear"):
            return await self.pending(
                m.chat.id,
                m.from_user.id,
                gid,
                "clear_table",
                {"table": table},
                f"clear {base}?",
            )
        if cmd.startswith("set"):
            if not text or len(text) > 3000:
                raise ValueError("use 1–3000 characters ◡")
            if base == "rules":
                await self.db.execute(
                    "INSERT INTO rules(group_id,rules_text) VALUES (?,?) ON CONFLICT(group_id) DO UPDATE SET rules_text=excluded.rules_text",
                    [gid, text],
                )
            else:
                kind, file_id = media_ref(m.reply_to_message)
                await self.db.execute(
                    f"INSERT INTO {table}(group_id,{column},media_id,media_type) VALUES (?,?,?,?) ON CONFLICT(group_id) DO UPDATE SET {column}=excluded.{column},media_id=excluded.media_id,media_type=excluded.media_type",
                    [gid, text, file_id, kind],
                )
            return await self.say(m, "saved ˚")
        row = await self.db.one(f"SELECT * FROM {table} WHERE group_id=?", [gid])
        content = (
            render_template(row[column], m.from_user, m.chat)
            if row
            else "nothing set yet ｡"
        )
        await self.show(
            m.chat.id,
            m.from_user.id,
            Screen(base, lines=content.splitlines()),
            private=base != "rules",
        )

    async def member_command(
        self, m: T.Message, gid: int, cmd: str, args: list[str]
    ) -> None:
        uid = m.from_user.id
        if cmd in {"tags", "approved", "xpleaderboard", "ranks"}:
            if cmd == "ranks":
                rows, cols = (
                    await self.db.query(
                        "SELECT min_xp,rank_name FROM xp_ranks WHERE group_id=? ORDER BY min_xp",
                        [gid],
                    ),
                    ["min_xp", "rank_name"],
                )
            else:
                where = (
                    "AND is_tagged=1"
                    if cmd == "tags"
                    else "AND is_approved=1"
                    if cmd == "approved"
                    else ""
                )
                rows = await self.db.query(
                    f"SELECT user_id,tag_label,xp FROM group_members WHERE group_id=? {where} ORDER BY xp DESC LIMIT 1000",
                    [gid],
                )
                cols = ["user_id", "tag_label"] if cmd == "tags" else ["user_id", "xp"]
            return await self.show_snapshot(m, cmd, rows, cols)
        if cmd == "unapproveall":
            return await self.pending(
                m.chat.id,
                uid,
                gid,
                "unapprove_all",
                {},
                "remove all approved-member exemptions?",
            )
        if cmd == "setrank":
            xp = int(args[0])
            if xp < 0:
                raise ValueError("xp cannot be negative ◡")
            await self.db.execute(
                "INSERT INTO xp_ranks(group_id,min_xp,rank_name) VALUES (?,?,?) ON CONFLICT(group_id,min_xp) DO UPDATE SET rank_name=excluded.rank_name",
                [gid, xp, " ".join(args[1:])[:30]],
            )
            return await self.say(m, "saved ˚")
        target, remaining = (
            await self.resolve(m, args) if args or m.reply_to_message else (uid, [])
        )
        await self.db.execute(
            "INSERT OR IGNORE INTO group_members(group_id,user_id,joined_at) VALUES (?,?,?)",
            [gid, target, now()],
        )
        if cmd in {"tag", "untag"}:
            label = " ".join(remaining) if cmd == "tag" else ""
            if len(label) > 16 or not re.fullmatch(r"[\w ·-]{0,16}", label):
                raise ValueError("tag must be at most 16 characters, without emoji ◡")
            await self.require_admin(gid, uid, "can_manage_tags")
            await self.bot.set_chat_member_tag(gid, target, label)
            await self.db.execute(
                "UPDATE group_members SET is_tagged=?,tag_label=? WHERE group_id=? AND user_id=?",
                [int(bool(label)), label, gid, target],
            )
        elif cmd in {"approve", "unapprove"}:
            if cmd == "unapprove":
                return await self.pending(
                    m.chat.id,
                    uid,
                    gid,
                    "unapprove",
                    {"target": target},
                    f"remove protection exemption for {target}?",
                )
            await self.db.execute(
                "UPDATE group_members SET is_approved=1 WHERE group_id=? AND user_id=?",
                [gid, target],
            )
        elif cmd in {"addxp", "removexp"}:
            amount = int(remaining[0])
            if not 1 <= amount <= 1_000_000:
                raise ValueError("xp amount must be 1–1000000 ◡")
            await self.db.execute(
                "UPDATE group_members SET xp=MAX(0,xp+?) WHERE group_id=? AND user_id=?",
                [amount if cmd == "addxp" else -amount, gid, target],
            )
        elif cmd in {"xp", "rank"}:
            row = await self.db.one(
                "SELECT xp FROM group_members WHERE group_id=? AND user_id=?",
                [gid, target],
            )
            rank = await self.db.one(
                "SELECT rank_name FROM xp_ranks WHERE group_id=? AND min_xp<=? ORDER BY min_xp DESC LIMIT 1",
                [gid, row["xp"]],
            )
            return await self.say(
                m,
                f"{target} · {row['xp']} xp · {rank['rank_name'] if rank else 'unranked'}",
            )
        await self.say(m, "done ·͜·")

    async def ai_command(
        self, m: T.Message, gid: int, cmd: str, args: list[str], cfg: dict[str, Any]
    ) -> None:
        uid = m.from_user.id
        if cmd == "ailog":
            rows = await self.db.query(
                "SELECT user_id,capability,score,action_taken FROM ai_log WHERE group_id=? ORDER BY log_id DESC LIMIT 1000",
                [gid],
            )
            return await self.show_snapshot(
                m,
                "guard history",
                rows,
                ["user_id", "capability", "score", "action_taken"],
            )
        if cmd == "aisettings":
            return await self.show(m.chat.id, uid, self.ai_screen(cfg, gid))
        if cmd == "aithreshold":
            if len(args) != 2 or args[0] not in {"spam", "toxic", "pii"}:
                raise ValueError("use /aithreshold spam|toxic|pii 0.85 ◡")
            await self.db.set_setting(gid, args[0] + "_threshold", float(args[1]))
            return await self.say(m, "saved ˚")
        if not args or args[0] not in {"on", "off"}:
            raise ValueError("use on or off ◡")
        caps = (
            ["spam", "toxic", "pii"]
            if cmd == "aimode"
            else [{"aispam": "spam", "aitoxic": "toxic", "aipii": "pii"}[cmd]]
        )
        enabled = args[0] == "on"
        if enabled:
            if any(cap not in self.guards.models for cap in caps):
                raise ValueError(
                    "local capability files are not configured; nothing enabled ◡"
                )
            descriptions = {
                "spam": "spam guard: scores text for spam; deletes above your threshold, flags scores from 0.60 for admin review.",
                "toxic": "toxicity guard: warns above your threshold; mutes for 1h at 0.95 or higher.",
                "pii": "content guard: scores text for personal information; deletes above your threshold. Image and video analysis are not included.",
            }
            return await self.pending(
                m.chat.id,
                uid,
                gid,
                "ai_enable",
                {"caps": caps},
                "\n".join(descriptions[c] for c in caps)
                + "\nLocal classification can be wrong. Enable these automatic actions?",
            )
        for cap in caps:
            await self.db.set_setting(gid, cap + "_enabled", False)
        await self.say(m, "disabled ˚")

    def ai_screen(self, cfg: dict[str, Any], gid: int) -> Screen:
        rows, lines = [], []
        for cap, name in [
            ("spam", "spam guard"),
            ("toxic", "toxicity guard"),
            ("pii", "content guard"),
        ]:
            ready = cap in self.guards.models
            enabled = cfg[cap + "_enabled"]
            lines.append(
                f"{name}: {'on' if enabled else 'off'} · threshold {cfg[cap + '_threshold']:.2f}"
            )
            rows.append(
                [
                    button(
                        f"{name} · {'disable' if enabled else 'enable'}"
                        if ready
                        else f"{name} · needs local files",
                        f"cfg:{gid}:ai:{cap}" if ready else None,
                        "danger" if enabled else "success",
                    )
                ]
            )
        rows.append([button("← back", f"cfg:{gid}:settings")])
        return Screen("capabilities", "all local · all off by default", lines, rows)

    async def schedule_content(
        self, m: T.Message, gid: int, cmd: str, args: list[str], text: str
    ) -> None:
        uid = m.from_user.id
        if cmd in {"poll", "quiz"}:
            parts = [x.strip() for x in text.split("|")]
            if not 3 <= len(parts) <= 13 or any(not x for x in parts):
                raise ValueError("use question | option | option ◡")
            options = [T.InputPollOption(text=x[:100]) for x in parts[1:]]
            await self.bot.send_poll(
                gid,
                parts[0][:300],
                options=options,
                type="quiz" if cmd == "quiz" else "regular",
                correct_option_ids=[0] if cmd == "quiz" else None,
            )
            return
        if cmd == "closepoll":
            if not m.reply_to_message or not m.reply_to_message.poll:
                raise ValueError("reply to a poll I created ◡")
            await self.bot.stop_poll(gid, m.reply_to_message.message_id)
            return await self.say(m, "closed ˚")
        if cmd in {"pin", "unpin", "unpinall"}:
            await self.require_admin(gid, uid, "can_pin_messages")
            if cmd == "unpinall":
                return await self.pending(
                    m.chat.id, uid, gid, "unpin_all", {}, "unpin every message?"
                )
            if not m.reply_to_message or m.chat.id != gid:
                raise ValueError("reply to a message in the group ◡")
            fn = (
                self.bot.pin_chat_message
                if cmd == "pin"
                else self.bot.unpin_chat_message
            )
            await fn(gid, m.reply_to_message.message_id)
            return await self.say(m, "done ·͜·")
        if cmd in {"schedule", "schedulerecur", "countdown"}:
            recurrence = None
            if cmd == "schedulerecur":
                if len(args) < 6:
                    raise ValueError(
                        "use five cron fields followed by text; times are UTC ◡"
                    )
                recurrence = " ".join(args[:5])
                when = croniter(recurrence, datetime.now(UTC)).get_next(datetime)
                payload = " ".join(args[5:])
                second = croniter(recurrence, when).get_next(datetime)
                if (second - when).total_seconds() < 60:
                    raise ValueError("one post per minute maximum ◡")
            else:
                if len(args) < 2:
                    raise ValueError("use YYYY-MM-DD HH:MM text; times are UTC ◡")
                when = datetime.strptime(" ".join(args[:2]), "%Y-%m-%d %H:%M").replace(
                    tzinfo=UTC
                )
                payload = " ".join(args[2:])
            if cmd == "countdown":
                seconds = int((when - datetime.now(UTC)).total_seconds())
                return await self.say(
                    m,
                    f"{payload} · {'ended' if seconds < 0 else str(seconds // 86400) + 'd ' + str(seconds % 86400 // 3600) + 'h ' + str(seconds % 3600 // 60) + 'm'}",
                )
            if when.timestamp() <= now() or not payload or len(payload) > 3000:
                raise ValueError("choose a future time and 1–3000 characters ◡")
            post_id = secrets.token_urlsafe(9)
            await self.db.execute(
                "INSERT INTO scheduled_posts(post_id,group_id,topic_id,content_json,run_at,recur_cron) VALUES (?,?,?,?,?,?)",
                [
                    post_id,
                    gid,
                    m.message_thread_id,
                    json.dumps({"text": payload, "creator": uid}),
                    when.timestamp(),
                    recurrence,
                ],
            )
            await self.enqueue_post(post_id, when.timestamp())
            return await self.say(
                m, f"scheduled ˚ {post_id} · {when:%Y-%m-%d %H:%M} UTC"
            )
        if cmd == "schedules":
            rows = await self.db.query(
                "SELECT post_id,run_at,recur_cron FROM scheduled_posts WHERE group_id=? AND is_active=1 ORDER BY run_at LIMIT 1000",
                [gid],
            )
            return await self.show_snapshot(
                m, "schedules · UTC", rows, ["post_id", "run_at", "recur_cron"]
            )
        if cmd == "cancelschedule":
            return await self.pending(
                m.chat.id,
                uid,
                gid,
                "cancel_schedule",
                {"id": args[0]},
                f"cancel scheduled post {args[0]}?",
            )
        if cmd in {"checklist", "checklists"}:
            if cmd == "checklists":
                rows = await self.db.query(
                    "SELECT list_id,title,items_json FROM checklists WHERE group_id=? LIMIT 1000",
                    [gid],
                )
                return await self.show_snapshot(
                    m, "checklists", rows, ["list_id", "title", "items_json"]
                )
            if not args:
                raise ValueError(
                    "use create title · add item · done n · undone n · delete ◡"
                )
            operation = args[0]
            if operation == "create":
                title = " ".join(args[1:])[:100]
                if not title:
                    raise ValueError("add a title ◡")
                await self.db.execute(
                    "INSERT INTO checklists(group_id,title,created_by,created_at) VALUES (?,?,?,?)",
                    [gid, title, uid, now()],
                )
            else:
                row = await self.db.one(
                    "SELECT * FROM checklists WHERE group_id=? ORDER BY list_id DESC LIMIT 1",
                    [gid],
                )
                if not row:
                    raise ValueError("create a checklist first ◡")
                if operation == "delete":
                    return await self.pending(
                        m.chat.id,
                        uid,
                        gid,
                        "delete_checklist",
                        {"id": row["list_id"]},
                        f"delete {row['title']}?",
                    )
                items = json.loads(row["items_json"])
                if operation == "add":
                    if len(items) >= 50:
                        raise ValueError("50 items maximum ◡")
                    items.append({"text": " ".join(args[1:])[:150], "done": False})
                elif operation in {"done", "undone"}:
                    index = int(args[1]) - 1
                    if not 0 <= index < len(items):
                        raise ValueError("couldn't find that item ◡")
                    items[index]["done"] = operation == "done"
                else:
                    raise ValueError("unknown checklist action ◡")
                await self.db.execute(
                    "UPDATE checklists SET items_json=? WHERE group_id=? AND list_id=?",
                    [json.dumps(items), gid, row["list_id"]],
                )
            row = await self.db.one(
                "SELECT * FROM checklists WHERE group_id=? ORDER BY list_id DESC LIMIT 1",
                [gid],
            )
            lines = [
                f"{i + 1}. {'✦' if x['done'] else '◇'} {x['text']}"
                for i, x in enumerate(json.loads(row["items_json"]))
            ]
            # Native sendChecklist requires a business connection; this is an honest rich-message checklist.
            return await self.show(
                m.chat.id,
                uid,
                Screen(
                    row["title"],
                    "managed with /checklist · not a native business checklist",
                    lines or ["add your first item ｡"],
                ),
                private=False,
            )

    async def enqueue_post(self, post_id: str, run_at: float) -> None:
        await self.redis.enqueue_job(
            "deliver_post",
            post_id,
            _defer_until=datetime.fromtimestamp(run_at, UTC),
            _job_id=f"{PREFIX}post:{post_id}:{int(run_at)}",
        )

    async def workflow_command(
        self, m: T.Message, gid: int, cmd: str, args: list[str], text: str
    ) -> None:
        uid = m.from_user.id
        if cmd == "report":
            if not m.reply_to_message or not m.reply_to_message.from_user:
                raise ValueError("reply to the message you want to report ◡")
            if not await self.rate(f"report:{gid}:{uid}", 3, 600):
                raise ValueError("please give the team a moment ◡")
            result = await self.db.execute(
                "INSERT INTO report_inbox(group_id,reporter_id,target_id,message_id,created_at) VALUES (?,?,?,?,?)",
                [
                    gid,
                    uid,
                    m.reply_to_message.from_user.id,
                    m.reply_to_message.message_id,
                    now(),
                ],
            )
            await self.audit(
                gid,
                uid,
                "report",
                m.reply_to_message.from_user.id,
                {"id": result.last_insert_rowid},
            )
            await self.notify_duty(
                gid, uid, m.reply_to_message.from_user.id, result.last_insert_rowid
            )
            return await self.say(m, "reported to the staff inbox ✦")
        if cmd in {"claim", "unclaim"}:
            report_id = int(args[0])
            if cmd == "claim":
                result = await self.db.execute(
                    "UPDATE report_inbox SET claimed_by=?,claimed_at=? WHERE report_id=? AND group_id=? AND claimed_by IS NULL",
                    [uid, now(), report_id, gid],
                )
            else:
                result = await self.db.execute(
                    "UPDATE report_inbox SET claimed_by=NULL,claimed_at=NULL WHERE report_id=? AND group_id=? AND claimed_by=?",
                    [report_id, gid, uid],
                )
            return await self.say(
                m,
                "done ·͜·"
                if result.rows_affected
                else "already claimed, or not yours ◡",
            )
        if cmd == "reports":
            rows = await self.db.query(
                "SELECT report_id,target_id,status,claimed_by FROM report_inbox WHERE group_id=? ORDER BY report_id DESC LIMIT 1000",
                [gid],
            )
            return await self.show_snapshot(
                m, "reports", rows, ["report_id", "target_id", "status", "claimed_by"]
            )
        if cmd == "shiftnote":
            if not text or len(text) > 2000:
                raise ValueError("note must be 1–2000 characters ◡")
            await self.db.execute(
                "INSERT INTO shift_notes(group_id,admin_id,content,created_at) VALUES (?,?,?,?)",
                [gid, uid, text, now()],
            )
            return await self.say(m, "noted ✦")
        if cmd == "shiftnotes":
            rows = await self.db.query(
                "SELECT admin_id,content,created_at FROM shift_notes WHERE group_id=? ORDER BY note_id DESC LIMIT 1000",
                [gid],
            )
            return await self.show_snapshot(
                m, "shift notes", rows, ["admin_id", "content", "created_at"]
            )
        if cmd == "duty":
            if args and args[0] in {"on", "off"}:
                await self.db.execute(
                    "DELETE FROM duty_roster WHERE group_id=? AND admin_id=?",
                    [gid, uid],
                )
                await self.db.execute(
                    "INSERT INTO duty_roster(group_id,admin_id,start_time,end_time,is_active) VALUES (?,?,'00:00','23:59',?)",
                    [gid, uid, int(args[0] == "on")],
                )
            elif args and args[0] == "set":
                target, remaining = await self.resolve(m, args[1:])
                await self.require_admin(gid, target)
                for value in remaining[:2]:
                    datetime.strptime(value, "%H:%M")
                if len(remaining) != 2:
                    raise ValueError("use /duty set @admin 09:00 17:00 · UTC ◡")
                await self.db.execute(
                    "DELETE FROM duty_roster WHERE group_id=? AND admin_id=?",
                    [gid, target],
                )
                await self.db.execute(
                    "INSERT INTO duty_roster(group_id,admin_id,start_time,end_time) VALUES (?,?,?,?)",
                    [gid, target, *remaining],
                )
            rows = await self.db.query(
                "SELECT admin_id,start_time,end_time,is_active FROM duty_roster WHERE group_id=?",
                [gid],
            )
            return await self.show_snapshot(
                m,
                "duty roster · UTC",
                rows,
                ["admin_id", "start_time", "end_time", "is_active"],
            )

    async def federation_command(
        self, m: T.Message, gid: int, cmd: str, args: list[str], text: str
    ) -> None:
        uid = m.from_user.id
        if cmd == "newfed":
            if not text or len(text) > 100:
                raise ValueError("use a federation name under 100 characters ◡")
            fed_id = secrets.token_urlsafe(12)
            await self.db.execute(
                "INSERT INTO federations(fed_id,name,owner_id,created_at) VALUES (?,?,?,?)",
                [fed_id, text, uid, now()],
            )
            return await self.say(m, f"created ˚ {fed_id} · /joinfed {fed_id}")
        if cmd == "myfed":
            rows = await self.db.query(
                "SELECT fed_id,name FROM federations WHERE owner_id=?", [uid]
            )
            return await self.show_snapshot(
                m, "your federations", rows, ["fed_id", "name"]
            )
        if cmd == "joinfed":
            member = await self.bot.get_chat_member(gid, uid)
            if member.status != "creator":
                raise ValueError("only the group owner may join a federation ◡")
            fed = await self.db.one(
                "SELECT * FROM federations WHERE fed_id=?", [args[0]]
            )
            if not fed:
                raise ValueError("couldn't find that federation ◡")
            return await self.pending(
                m.chat.id,
                uid,
                gid,
                "join_fed",
                {"fed": fed["fed_id"]},
                f"join {fed['name']}? its admins can ban members here; existing bans will be enforced",
            )
        membership = await self.db.one(
            "SELECT f.* FROM federations f JOIN fed_members m ON f.fed_id=m.fed_id WHERE m.group_id=?",
            [gid],
        )
        if not membership:
            raise ValueError("this group has no federation ◡")
        fed, owner = membership["fed_id"], membership["owner_id"] == uid
        fed_admin = owner or bool(
            await self.db.one(
                "SELECT 1 FROM fed_admins WHERE fed_id=? AND user_id=?", [fed, uid]
            )
        )
        if cmd in {"fedinfo", "fedadmins", "fbanlist", "fedstat"}:
            if cmd == "fedinfo":
                return await self.show(
                    m.chat.id,
                    uid,
                    Screen(
                        "federation",
                        membership["name"],
                        [f"id: {fed}", f"owner: {membership['owner_id']}"],
                    ),
                )
            if cmd == "fedadmins":
                rows, cols = (
                    await self.db.query(
                        "SELECT user_id FROM fed_admins WHERE fed_id=?", [fed]
                    ),
                    ["user_id"],
                )
            elif cmd == "fedstat":
                target, _ = await self.resolve(m, args)
                rows, cols = (
                    await self.db.query(
                        "SELECT user_id,reason,banned_by FROM fed_bans WHERE fed_id=? AND user_id=?",
                        [fed, target],
                    ),
                    ["user_id", "reason", "banned_by"],
                )
            else:
                rows, cols = (
                    await self.db.query(
                        "SELECT user_id,reason,banned_by FROM fed_bans WHERE fed_id=? LIMIT 1000",
                        [fed],
                    ),
                    ["user_id", "reason", "banned_by"],
                )
            return await self.show_snapshot(m, cmd, rows, cols)
        if cmd == "leavefed":
            if (await self.bot.get_chat_member(gid, uid)).status != "creator":
                raise ValueError("only the group owner may leave the federation ◡")
            return await self.pending(
                m.chat.id,
                uid,
                gid,
                "leave_fed",
                {},
                "leave this federation? existing Telegram bans remain",
            )
        if not fed_admin:
            raise ValueError("federation admin rights needed ᯓ")
        if cmd in {"fban", "unfban"}:
            target, rest = await self.resolve(m, args)
            return await self.pending(
                m.chat.id,
                uid,
                gid,
                "fed_ban",
                {
                    "fed": fed,
                    "target": target,
                    "reason": " ".join(rest),
                    "unban": cmd == "unfban",
                },
                f"{cmd} {target} across this federation and subscribers?",
            )
        if not owner:
            raise ValueError("federation owner only ᯓ")
        if cmd == "renamefed":
            await self.db.execute(
                "UPDATE federations SET name=? WHERE fed_id=?", [text[:100], fed]
            )
        elif cmd == "quietfed":
            if args[0] not in {"on", "off"}:
                raise ValueError("use on or off ◡")
            await self.db.execute(
                "UPDATE federations SET quiet_mode=? WHERE fed_id=?",
                [int(args[0] == "on"), fed],
            )
        elif cmd in {"fedpromote", "feddemote"}:
            target, _ = await self.resolve(m, args)
            if cmd == "fedpromote":
                await self.db.execute(
                    "INSERT OR IGNORE INTO fed_admins(fed_id,user_id,added_at) VALUES (?,?,?)",
                    [fed, target, now()],
                )
            else:
                await self.db.execute(
                    "DELETE FROM fed_admins WHERE fed_id=? AND user_id=?", [fed, target]
                )
        elif cmd == "delfed":
            return await self.pending(
                m.chat.id,
                uid,
                gid,
                "delete_fed",
                {"fed": fed},
                "delete this federation? existing group bans remain",
            )
        elif cmd in {"subfed", "unsubfed"}:
            target = args[0]
            if target == fed or not await self.db.one(
                "SELECT 1 FROM federations WHERE fed_id=?", [target]
            ):
                raise ValueError("choose another existing federation ◡")
            if cmd == "subfed":
                return await self.pending(
                    m.chat.id,
                    uid,
                    gid,
                    "subscribe_fed",
                    {"fed": fed, "target": target},
                    "subscribe to this federation's bans? existing bans sync on new joins; new bans propagate",
                )
            await self.db.execute(
                "DELETE FROM fed_subscriptions WHERE fed_id=? AND subscribed_to_fed_id=?",
                [fed, target],
            )
        await self.say(m, "saved ˚")

    async def fed_targets(self, fed: str) -> list[dict[str, Any]]:
        # UNION (not UNION ALL) makes cycles finite.
        return await self.db.query(
            "WITH RECURSIVE followers(id) AS (SELECT ? UNION SELECT s.fed_id FROM fed_subscriptions s JOIN followers x ON s.subscribed_to_fed_id=x.id) SELECT DISTINCT group_id FROM fed_members WHERE fed_id IN (SELECT id FROM followers)",
            [fed],
        )

    async def fed_authorize(self, fed: str, uid: int, owner_only: bool = False) -> None:
        row = await self.db.one(
            "SELECT owner_id FROM federations WHERE fed_id=?", [fed]
        )
        if not row:
            raise ValueError("that federation no longer exists ◡")
        if row["owner_id"] == uid:
            return
        if not owner_only and await self.db.one(
            "SELECT 1 FROM fed_admins WHERE fed_id=? AND user_id=?", [fed, uid]
        ):
            return
        raise ValueError("federation permissions changed ᯓ")

    def owner_only(self, m: T.Message | T.CallbackQuery) -> None:
        chat = (
            m.chat
            if isinstance(m, T.Message)
            else (m.message.chat if m.message else None)
        )
        if m.from_user.id != self.owner or not chat or chat.type != "private":
            raise ValueError("owner panel is private ᯓ")

    async def panel_screen(self, section: str) -> Screen:
        if section in {
            "broadcast",
            "warndefaults",
            "appeals",
            "groups",
            "users",
            "database",
            "audit",
            "bans",
            "mutes",
        }:
            return await self.enhanced_panel(section)
        labels = {
            "overview": "overview",
            "broadcast": "broadcast",
            "mainvideo": "main menu video",
            "panelvideo": "panel video",
            "updates": "update channel",
            "groups": "groups",
            "users": "users",
            "federations": "federations",
            "audit": "moderation log",
            "bans": "active bans",
            "mutes": "active mutes",
            "scheduled": "scheduled posts",
            "blacklist": "blacklisted groups",
            "botsettings": "bot settings",
            "warndefaults": "warn defaults",
            "capabilities": "ai capabilities",
            "reports": "reports inbox",
            "appeals": "appeal queue",
            "history": "broadcast history",
            "database": "database",
            "redis": "redis",
            "system": "system",
        }
        if section == "home":
            buttons = [
                button(f"◈ {label}", f"panel:{key}") for key, label in labels.items()
            ]
            return Screen(
                "owner panel",
                "private to you ·͜·",
                rows=[buttons[i : i + 2] for i in range(0, len(buttons), 2)]
                + [[button("← back", "home")]],
            )
        if section not in labels:
            raise ValueError("unknown panel section ◡")
        back = [[button("← back", "panel:home")]]
        if section == "overview":
            counts = {
                table: (await self.db.one(f"SELECT count(*) n FROM {table}"))["n"]
                for table in ("groups", "users", "cases", "warnings", "fed_bans")
            }
            active = await self.db.one(
                "SELECT count(*) n FROM users WHERE last_seen>?", [now() - 604800]
            )
            lines = [f"{k}: {v}" for k, v in counts.items()] + [
                f"active users 7d: {active['n']}",
                f"uptime: {int(now() - self.started)}s",
            ]
            return Screen(
                labels[section],
                "observed records · not historical data from before installation",
                lines,
                [[button("◈ refresh", "panel:overview")]] + back,
            )
        if section in {"mainvideo", "panelvideo", "updates"}:
            table = {
                "mainvideo": "main_menu_video",
                "panelvideo": "panel_menu_video",
                "updates": "update_channel",
            }[section]
            exists = bool(await self.db.one(f"SELECT * FROM {table} LIMIT 1"))
            return Screen(
                labels[section],
                "send a video/GIF or Telegram post link"
                if section != "updates"
                else "send an https://t.me/ channel link",
                ["configured" if exists else "not set ｡"],
                [
                    [
                        button("◈ set", f"input:{section}", "success"),
                        button("◇ preview", f"preview:{section}" if exists else None),
                    ],
                    [
                        button(
                            "⌦ remove",
                            f"remove:{section}" if exists else None,
                            "danger",
                        )
                    ],
                ]
                + back,
            )
        queries = {
            "groups": (
                "SELECT group_id,title,member_count,active FROM groups ORDER BY joined_at DESC LIMIT 1000",
                ["group_id", "title", "member_count", "active"],
            ),
            "users": (
                "SELECT user_id,username,message_count FROM users ORDER BY last_seen DESC LIMIT 1000",
                ["user_id", "username", "message_count"],
            ),
            "federations": (
                "SELECT fed_id,name,owner_id FROM federations LIMIT 1000",
                ["fed_id", "name", "owner_id"],
            ),
            "audit": (
                "SELECT log_id,group_id,admin_id,action FROM audit_log ORDER BY log_id DESC LIMIT 1000",
                ["log_id", "group_id", "admin_id", "action"],
            ),
            "bans": (
                "SELECT group_id,user_id,reason,expires_at FROM bans WHERE resolved=0 AND (expires_at IS NULL OR expires_at>strftime('%s','now')) LIMIT 1000",
                ["group_id", "user_id", "reason"],
            ),
            "mutes": (
                "SELECT group_id,user_id,reason,expires_at FROM mutes WHERE resolved=0 AND (expires_at IS NULL OR expires_at>strftime('%s','now')) LIMIT 1000",
                ["group_id", "user_id", "reason"],
            ),
            "scheduled": (
                "SELECT post_id,group_id,run_at FROM scheduled_posts WHERE is_active=1 ORDER BY run_at LIMIT 1000",
                ["post_id", "group_id", "run_at"],
            ),
            "blacklist": (
                "SELECT group_id,reason FROM blacklisted_groups LIMIT 1000",
                ["group_id", "reason"],
            ),
            "reports": (
                "SELECT report_id,group_id,target_id,claimed_by FROM report_inbox WHERE status='pending' ORDER BY report_id DESC LIMIT 1000",
                ["report_id", "group_id", "target_id", "claimed_by"],
            ),
            "history": (
                "SELECT broadcast_id,target_type,sent_count,failed_count FROM broadcast_history ORDER BY broadcast_id DESC LIMIT 20",
                ["broadcast_id", "target_type", "sent_count", "failed_count"],
            ),
        }
        if section in queries:
            query, cols = queries[section]
            rows = await self.db.query(query)
            screen = await self.list_screen(
                labels[section], rows, cols, back="panel:home"
            )
            if len(rows) > 10:
                # Panel callback pagination re-queries live data, unlike per-user list snapshots.
                screen.rows.insert(
                    0, [button("←"), button("1"), button("→", f"panelpage:{section}:1")]
                )
            return screen
        if section == "capabilities":
            lines = [
                f"{name}: {'ready' if cap in self.guards.models else 'needs local files'}"
                for cap, name in [
                    ("spam", "spam guard"),
                    ("toxic", "toxicity guard"),
                    ("pii", "content guard"),
                ]
            ]
            return Screen(
                labels[section],
                "startup loading only · no runtime model replacement",
                lines,
                back,
            )
        if section == "botsettings":
            maintenance = await self.db.bot_setting("maintenance", False)
            joins = await self.db.bot_setting("new_joins", True)
            return Screen(
                labels[section],
                lines=[f"maintenance: {maintenance}", f"new joins: {joins}"],
                rows=[
                    [
                        button("◈ maintenance", "ownertoggle:maintenance"),
                        button("◈ new joins", "ownertoggle:new_joins"),
                    ],
                    [
                        button("◈ support link", "input:support"),
                        button("◈ description", "input:description"),
                    ],
                ]
                + back,
            )
        if section == "redis":
            info = await self.redis.info("memory")
            return Screen(
                "redis",
                lines=[
                    f"memory: {info.get('used_memory_human')}",
                    "rate-limit cleanup affects only Marie's rate keys",
                ],
                rows=[[button("⌦ flush rate limits", "ownerflush", "danger")]] + back,
            )
        if section == "system":
            return Screen(
                "system",
                lines=[
                    f"Python: {sys.version.split()[0]}",
                    f"aiogram: {aiogram.__version__}",
                    f"uptime: {int(now() - self.started)}s",
                    "restart through your container manager; errors go to stderr",
                    "health: /health · readiness: /ready",
                ],
                rows=back,
            )
        raise ValueError("unknown panel section ◡")

    async def callback(self, call: T.CallbackQuery) -> None:
        if not isinstance(call.message, T.Message) or not call.data:
            return
        uid, chat, data = call.from_user.id, call.message.chat.id, call.data
        explicit_gid = None
        if data.startswith("cfg:"):
            _, group_str, data = data.split(":", 2)
            explicit_gid = int(group_str)
            if explicit_gid >= 0 or data not in {
                "settings",
                "ai:show",
                "ai:spam",
                "ai:toxic",
                "ai:pii",
            }:
                raise ValueError("invalid settings scope ◡")
            await self.require_admin(explicit_gid, uid)
        with contextlib.suppress(TelegramAPIError):
            await call.answer()
        if data.startswith("v2:"):
            return await self.phase_callback(call)
        if data.startswith("captcha:"):
            return await self.solve_captcha(call)
        if data.startswith(("confirm:", "cancel:")):
            token = data.split(":", 1)[1]
            raw = await self.redis.get(f"{PREFIX}confirm:{token}")
            if not raw:
                raise ValueError("that confirmation expired ◡")
            record = json.loads(raw)
            if record["actor"] != uid or (chat != record["chat"] and chat != uid):
                raise ValueError("this confirmation isn't yours ◡")
            raw = await self.redis.eval(
                CONSUME, 1, f"{PREFIX}confirm:{token}", str(uid), str(record["chat"])
            )
            if not raw or record["expires"] <= now():
                raise ValueError("that confirmation expired ◡")
            await self.redis.delete(f"{PREFIX}countdown:{token}")
            job = await self.redis.get(f"{PREFIX}countdown-active:{token}")
            if job:
                await self.redis.delete(f"arq:job:{job}")
                await self.redis.zrem(PREFIX + "jobs", job)
            await self.redis.delete(f"{PREFIX}countdown-active:{token}")
            if data.startswith("cancel:"):
                return await self.show(
                    chat, uid, Screen("marie", "cancelled ˚"), callback=call
                )
            if record["gid"]:
                await self.require_admin(record["gid"], uid)
            else:
                self.owner_only(call)
            result = await self.execute_pending(record, call)
            if result is not None:
                await self.show(chat, uid, Screen("marie", result), callback=call)
            return
        if data.startswith("panel:"):
            self.owner_only(call)
            return await self.show(
                chat, uid, await self.panel_screen(data.split(":", 1)[1]), callback=call
            )
        if data.startswith("panelpage:"):
            self.owner_only(call)
            _, section, page_str = data.split(":")
            page = int(page_str)
            screen = await self.panel_screen(section)
            # Owner tables are intentionally bounded to the most recent 1000 records.
            table_map = {
                "groups": ("groups", "group_id,title,member_count,active"),
                "users": ("users", "user_id,username,message_count"),
                "federations": ("federations", "fed_id,name,owner_id"),
                "audit": ("audit_log", "log_id,group_id,admin_id,action"),
                "bans": ("bans", "group_id,user_id,reason"),
                "mutes": ("mutes", "group_id,user_id,reason"),
                "scheduled": ("scheduled_posts", "post_id,group_id,run_at"),
                "blacklist": ("blacklisted_groups", "group_id,reason"),
                "reports": ("report_inbox", "report_id,group_id,target_id,claimed_by"),
                "history": (
                    "broadcast_history",
                    "broadcast_id,target_type,sent_count,failed_count",
                ),
            }
            table, cols = table_map[section]
            where = (
                " WHERE resolved=0 AND (expires_at IS NULL OR expires_at>strftime('%s','now'))"
                if section in {"bans", "mutes"}
                else " WHERE is_active=1"
                if section == "scheduled"
                else " WHERE status='pending'"
                if section == "reports"
                else ""
            )
            rows = await self.db.query(f"SELECT {cols} FROM {table}{where} LIMIT 1000")
            screen = await self.list_screen(
                screen.title,
                rows,
                cols.split(","),
                page,
                f"panelpage:{section}",
                "panel:home",
            )
            return await self.show(chat, uid, screen, callback=call)
        if (
            data.startswith(("input:", "preview:", "remove:", "ownertoggle:"))
            or data == "ownerflush"
        ):
            self.owner_only(call)
            if data == "ownerflush":
                return await self.pending(
                    chat,
                    uid,
                    0,
                    "flush_rates",
                    {},
                    "clear Marie's command, report, and flood counters? no other Redis data changes",
                    call,
                )
            kind, section = data.split(":", 1)
            if kind == "ownertoggle":
                if section not in {"maintenance", "new_joins"}:
                    raise ValueError("unknown setting ◡")
                await self.db.set_bot_setting(
                    section,
                    not await self.db.bot_setting(section, section == "new_joins"),
                )
                return await self.show(
                    chat, uid, await self.panel_screen("botsettings"), callback=call
                )
            if section not in {
                "mainvideo",
                "panelvideo",
                "updates",
                "support",
                "description",
            }:
                raise ValueError("unknown input ◡")
            if kind == "input":
                await self.redis.delete(f"{PREFIX}flow:{uid}:{chat}")
                await self.redis.set(f"{PREFIX}owner_input:{uid}", section, ex=300)
                return await self.show(
                    chat,
                    uid,
                    Screen(
                        "send content",
                        "send your new value within 5 minutes · /cancel to stop",
                        rows=[[button("← back", "panel:home")]],
                    ),
                    callback=call,
                )
            if kind == "preview":
                if section in {"mainvideo", "panelvideo"}:
                    await self.video(
                        uid,
                        "main_menu_video"
                        if section == "mainvideo"
                        else "panel_menu_video",
                    )
                else:
                    row = await self.db.one("SELECT url FROM update_channel LIMIT 1")
                    await self.show(
                        chat, uid, Screen("updates", row["url"] if row else "not set ｡")
                    )
                return
            return await self.pending(
                chat,
                uid,
                0,
                "remove_owner_media",
                {"section": section},
                f"remove {section}?",
                call,
            )
        if data.startswith("page:"):
            _, token, page_str = data.split(":")
            raw = await self.redis.get(f"{PREFIX}list:{token}")
            if not raw:
                raise ValueError("this list expired; run the command again ◡")
            record = json.loads(raw)
            if record["actor"] != uid or chat not in {record["chat"], uid}:
                raise ValueError("this list isn't yours ◡")
            if record.get("scope") and record["title"] not in {
                "xpleaderboard",
                "ranks",
                "warns",
                "strikes",
            }:
                await self.require_admin(record["scope"], uid)
            screen = await self.list_screen(
                record["title"],
                record["rows"],
                record["columns"],
                int(page_str),
                f"page:{token}",
            )
            return await self.show(chat, uid, screen, callback=call)
        if data == "home":
            if chat == uid:
                return await self.home(
                    call.message.model_copy(update={"from_user": call.from_user})
                )
            data = "sections"
        if data == "about":
            return await self.show(
                chat,
                uid,
                Screen(
                    "marie",
                    "your gentle group guardian ·͜·",
                    rows=[[button("← back", "home")]],
                ),
                callback=call,
            )
        if data == "sections":
            return await self.show(
                chat, uid, await self.section_menu(chat), callback=call
            )
        if data.startswith("help:"):
            return await self.show(chat, uid, self.help_screen(data[5:]), callback=call)
        if data in {"settings", "ai:show"} or data.startswith("ai:"):
            m = call.message.model_copy(update={"from_user": call.from_user})
            gid = explicit_gid if explicit_gid is not None else await self.context(m)
            await self.require_admin(gid, uid)
            cfg = await self.db.settings(gid)
            if data == "settings":
                return await self.show(
                    chat, uid, await self.setting_screen(gid), callback=call
                )
            if data == "ai:show":
                return await self.show(
                    chat, uid, self.ai_screen(cfg, gid), callback=call
                )
            cap = data.split(":")[1]
            if cap not in {"spam", "toxic", "pii"}:
                raise ValueError("unknown capability ◡")
            return await self.ai_command(
                m,
                gid,
                {"spam": "aispam", "toxic": "aitoxic", "pii": "aipii"}[cap],
                ["off" if cfg[cap + "_enabled"] else "on"],
                cfg,
            )
        raise ValueError("this menu has expired ◡")

    async def execute_pending(
        self, r: dict[str, Any], call: T.CallbackQuery
    ) -> str | None:
        op, p, gid, uid = r["op"], r["payload"], r["gid"], r["actor"]
        if op.startswith("v2_"):
            return await self.phase_pending(r, call)
        if op == "moderate":
            case_id = await self.action(
                gid, uid, p["target"], p["kind"], p["reason"], p["seconds"]
            )
            return f"done ·͜· case {case_id}"
        if op == "bulk":
            done, failed = 0, 0
            for target in p["targets"]:
                try:
                    await self.action(
                        gid, uid, target, p["kind"], p["reason"], p["seconds"]
                    )
                    done += 1
                except (ValueError, TelegramAPIError):
                    failed += 1
                    LOG.exception(
                        "bulk action failed gid=%s actor=%s target=%s", gid, uid, target
                    )
            return f"done: {done} · not applied: {failed}"
        if op in {"clear_table", "delete_entry"}:
            allowed = {
                "filters",
                "notes",
                "blocklist",
                "welcomes",
                "goodbyes",
                "rules",
                "drip_sequences",
            }
            if p["table"] not in allowed:
                raise ValueError("invalid table")
            if op == "delete_entry":
                if (p["table"], p["column"]) not in {
                    ("filters", "keyword"),
                    ("notes", "name"),
                    ("blocklist", "phrase"),
                }:
                    raise ValueError("invalid column")
                await self.db.execute(
                    f"DELETE FROM {p['table']} WHERE group_id=? AND {p['column']}=?",
                    [gid, p["value"]],
                )
            else:
                await self.db.execute(
                    f"DELETE FROM {p['table']} WHERE group_id=?", [gid]
                )
        elif op == "clearwarn":
            await self.require_admin(gid, uid, "can_restrict_members")
            if p["all"]:
                await self.db.execute(
                    "UPDATE warnings SET is_expired=1 WHERE group_id=? AND user_id=?",
                    [gid, p["target"]],
                )
            else:
                await self.db.execute(
                    "UPDATE warnings SET is_expired=1 WHERE warn_id=(SELECT warn_id FROM warnings WHERE group_id=? AND user_id=? AND is_expired=0 ORDER BY warn_id DESC LIMIT 1)",
                    [gid, p["target"]],
                )
        elif op == "undo":
            row = p["case"]
            live = await self.db.one(
                "SELECT * FROM cases WHERE case_id=? AND group_id=? AND admin_id=? AND resolved=0 AND created_at>?",
                [row["case_id"], gid, uid, now() - 60],
            )
            if not live:
                raise ValueError("undo window expired ◡")
            kind = row["action"]
            if kind in {"warn", "strike"}:
                await self.db.execute(
                    "UPDATE warnings SET is_expired=1 WHERE warn_id=(SELECT warn_id FROM warnings WHERE group_id=? AND user_id=? AND admin_id=? AND created_at>=? ORDER BY warn_id DESC LIMIT 1)",
                    [gid, row["user_id"], uid, row["created_at"]],
                )
            else:
                reverse = (
                    "unban"
                    if kind in {"ban", "tban"}
                    else "unshadowmute"
                    if kind == "shadowmute"
                    else "unmute"
                )
                await self.action(gid, uid, row["user_id"], reverse, "undo")
            await self.db.execute(
                "UPDATE cases SET resolved=1 WHERE case_id=?", [row["case_id"]]
            )
        elif op in {"readonly", "reaction_permissions"}:
            await self.require_admin(gid, uid, "can_restrict_members")
            chat = await self.bot.get_chat(gid)
            perms = (
                chat.permissions or T.ChatPermissions(can_send_messages=True)
            ).model_dump(exclude_none=True)
            if op == "readonly":
                if p["value"]:
                    cfg = await self.db.settings(gid)
                    if cfg["readonly"]:
                        return "already read-only ˚"
                    await self.db.set_bot_setting(f"permissions:{gid}", perms)
                    for key in list(perms):
                        if (
                            key.startswith("can_send")
                            or key == "can_add_web_page_previews"
                        ):
                            perms[key] = False
                    perms["can_send_messages"] = False
                else:
                    old = await self.db.bot_setting(f"permissions:{gid}")
                    if not old:
                        raise ValueError(
                            "original permissions not saved; restore in Telegram settings ◡"
                        )
                    perms = old
            else:
                perms["can_react_to_messages"] = p["value"]
            await self.bot.set_chat_permissions(
                gid, T.ChatPermissions(**perms), use_independent_chat_permissions=True
            )
            if op == "readonly":
                await self.db.set_setting(gid, "readonly", p["value"])
        elif op == "ai_enable":
            for cap in p["caps"]:
                if cap not in self.guards.models:
                    raise ValueError("capability is not ready ◡")
                await self.db.set_setting(gid, cap + "_enabled", True)
        elif op == "purge":
            await self.require_admin(gid, uid, "can_delete_messages")
            done, failed = 0, 0
            for mid in range(p["start"], p["end"] + 1):
                try:
                    await self.bot.delete_message(gid, mid)
                    done += 1
                except TelegramBadRequest:
                    failed += 1
                except TelegramRetryAfter as exc:
                    await asyncio.sleep(exc.retry_after)
                    try:
                        await self.bot.delete_message(gid, mid)
                        done += 1
                    except TelegramAPIError:
                        failed += 1
            await self.audit(
                gid, uid, "purge", details={"deleted": done, "not_deleted": failed}
            )
            return f"deleted: {done} · not deleted: {failed}"
        elif op == "unpin_all":
            await self.require_admin(gid, uid, "can_pin_messages")
            await self.bot.unpin_all_chat_messages(gid)
        elif op == "delete_topic":
            await self.require_admin(gid, uid, "can_delete_messages")
            await self.bot.delete_forum_topic(gid, p["topic"])
        elif op == "unapprove_all":
            await self.db.execute(
                "UPDATE group_members SET is_approved=0 WHERE group_id=?", [gid]
            )
        elif op == "unapprove":
            await self.db.execute(
                "UPDATE group_members SET is_approved=0 WHERE group_id=? AND user_id=?",
                [gid, p["target"]],
            )
        elif op == "cancel_schedule":
            await self.db.execute(
                "UPDATE scheduled_posts SET is_active=0 WHERE post_id=? AND group_id=?",
                [p["id"], gid],
            )
        elif op == "delete_checklist":
            await self.db.execute(
                "DELETE FROM checklists WHERE list_id=? AND group_id=?", [p["id"], gid]
            )
        elif op == "reset_first":
            await self.pending(
                call.message.chat.id,
                uid,
                gid,
                "reset_final",
                {},
                "final step: reset all group configuration and content? cases, warnings, ban history and audit stay",
                call,
            )
            return None
        elif op == "reset_final":
            statements = [
                (f"DELETE FROM {table} WHERE group_id=?", [gid])
                for table in EXPORT_FIELDS
            ]
            statements += [
                (
                    "UPDATE groups SET settings_json='{}',log_channel_id=NULL,log_topics_json='{}' WHERE group_id=?",
                    [gid],
                )
            ]
            await self.db.batch(statements)
            await self.audit(gid, uid, "settings_reset")
            return "cleared: settings, locks, blocklist, filters, notes, greetings, rules · history kept ˚"
        elif op == "import":
            data = self.validate_import(p["data"])
            statements = [
                (
                    "UPDATE groups SET settings_json=? WHERE group_id=?",
                    [json.dumps(data.settings), gid],
                )
            ]
            for table, columns in EXPORT_FIELDS.items():
                statements.append((f"DELETE FROM {table} WHERE group_id=?", [gid]))
                for row in getattr(data, table):
                    fields = list(row)
                    statements.append(
                        (
                            f"INSERT INTO {table}(group_id,{','.join(fields)}) VALUES ({','.join('?' for _ in range(len(fields) + 1))})",
                            [gid] + [row[c] for c in fields],
                        )
                    )
            await self.db.batch(statements)
        elif op == "join_fed":
            if (await self.bot.get_chat_member(gid, uid)).status != "creator":
                raise ValueError("group owner rights required ᯓ")
            await self.db.execute(
                "INSERT INTO fed_members(fed_id,group_id,joined_at) VALUES (?,?,?) ON CONFLICT(group_id) DO UPDATE SET fed_id=excluded.fed_id,joined_at=excluded.joined_at",
                [p["fed"], gid, now()],
            )
            # Enforce bans through a durable job; failed members remain retryable.
            await self.redis.enqueue_job(
                "sync_federation",
                gid,
                _job_id=f"{PREFIX}fedsync:{gid}:{secrets.token_hex(4)}",
            )
        elif op == "leave_fed":
            if (await self.bot.get_chat_member(gid, uid)).status != "creator":
                raise ValueError("group owner rights required ᯓ")
            await self.db.execute("DELETE FROM fed_members WHERE group_id=?", [gid])
        elif op == "fed_ban":
            await self.fed_authorize(p["fed"], uid)
            target = p["target"]
            if target == self.owner or target == self.me.id:
                raise ValueError("this member is protected ◡")
            if p["unban"]:
                await self.db.execute(
                    "DELETE FROM fed_bans WHERE fed_id=? AND user_id=?",
                    [p["fed"], target],
                )
            else:
                await self.db.execute(
                    "INSERT OR REPLACE INTO fed_bans(fed_id,user_id,reason,banned_by,banned_at) VALUES (?,?,?,?,?)",
                    [p["fed"], target, p["reason"][:1000], uid, now()],
                )
            groups = await self.fed_targets(p["fed"])
            success, failed = 0, 0
            for group in groups:
                try:
                    await self.action(
                        group["group_id"],
                        self.me.id,
                        target,
                        "unban" if p["unban"] else "ban",
                        "federation · " + p["reason"],
                        automatic=True,
                    )
                    success += 1
                except (ValueError, TelegramAPIError):
                    failed += 1
                    LOG.exception(
                        "federation propagation failed group=%s user=%s",
                        group["group_id"],
                        target,
                    )
            return f"groups updated: {success} · not applied: {failed}"
        elif op == "delete_fed":
            await self.fed_authorize(p["fed"], uid, owner_only=True)
            tables = ["fed_members", "fed_admins", "fed_bans"]
            statements = [
                (f"DELETE FROM {table} WHERE fed_id=?", [p["fed"]]) for table in tables
            ]
            statements += [
                (
                    "DELETE FROM fed_subscriptions WHERE fed_id=? OR subscribed_to_fed_id=?",
                    [p["fed"], p["fed"]],
                ),
                ("DELETE FROM federations WHERE fed_id=?", [p["fed"]]),
            ]
            await self.db.batch(statements)
        elif op == "subscribe_fed":
            await self.fed_authorize(p["fed"], uid, owner_only=True)
            await self.db.execute(
                "INSERT OR IGNORE INTO fed_subscriptions(fed_id,subscribed_to_fed_id) VALUES (?,?)",
                [p["fed"], p["target"]],
            )
        elif op == "broadcast":
            await self.redis.enqueue_job("broadcast_job", p, uid)
            return "broadcast queued · delivery report will arrive here ˚"
        elif op == "remove_owner_media":
            table = {
                "mainvideo": "main_menu_video",
                "panelvideo": "panel_menu_video",
                "updates": "update_channel",
            }[p["section"]]
            await self.db.execute(f"DELETE FROM {table}")
        elif op == "flush_rates":
            for pattern in ("cmd:*", "report:*", "flood:*", "dup:*", "cooldown:*"):
                async for key in self.redis.scan_iter(
                    match=PREFIX + pattern, count=100
                ):
                    await self.redis.delete(key)
        else:
            raise ValueError("unknown operation ◡")
        if gid:
            await self.audit(gid, uid, "settings_change", details={"operation": op})
        return "done ·͜·"

    async def export_settings(self, gid: int) -> dict[str, Any]:
        data: dict[str, Any] = {"version": 1, "settings": await self.db.settings(gid)}
        for table, cols in EXPORT_FIELDS.items():
            data[table] = await self.db.query(
                f"SELECT {','.join(cols)} FROM {table} WHERE group_id=? LIMIT 1000",
                [gid],
            )
        return data

    @staticmethod
    def validate_import(raw: Any) -> Export:
        data = Export.model_validate(raw)
        if len(data.settings) > 100:
            raise ValueError("too many settings ◡")
        for key, value in data.settings.items():
            validate_setting(key, value)
        # Import cannot silently enable automatic classifiers without an informed confirmation.
        if any(data.settings.get(c + "_enabled") for c in ("spam", "toxic", "pii")):
            raise ValueError(
                "import AI settings with guards off, then enable them separately ◡"
            )
        for table, allowed in EXPORT_FIELDS.items():
            for row in getattr(data, table):
                if not row or any(c not in allowed for c in row):
                    raise ValueError(f"invalid columns in {table}")
                for value in row.values():
                    if (
                        not isinstance(value, (str, int, float, type(None)))
                        or isinstance(value, str)
                        and len(value) > 5000
                    ):
                        raise ValueError("invalid imported cell ◡")
                required = {
                    "locks": {"lock_type", "is_locked"},
                    "blocklist": {"phrase", "match_type"},
                    "filters": {"keyword", "action"},
                    "notes": {"name", "content"},
                    "welcomes": {"welcome_text"},
                    "goodbyes": {"goodbye_text"},
                    "rules": {"rules_text"},
                }[table]
                if not required <= set(row):
                    raise ValueError(f"missing fields in {table}")
                if table == "locks" and (
                    row["lock_type"] not in LOCK_TYPES or row["is_locked"] not in (0, 1)
                ):
                    raise ValueError("invalid lock ◡")
                if "match_type" in row and row["match_type"] not in {
                    "exact",
                    "substring",
                    "wildcard",
                    "prefix",
                }:
                    raise ValueError("invalid match type ◡")
                if table == "filters" and row["action"] not in {"delete", "reply"}:
                    raise ValueError("invalid filter action ◡")
                if "buttons_json" in row:
                    buttons = json.loads(row["buttons_json"] or "[]")
                    if (
                        not isinstance(buttons, list)
                        or len(buttons) > 6
                        or any(
                            not isinstance(b, dict)
                            or set(b) != {"text", "url"}
                            or not isinstance(b["text"], str)
                            or not valid_url(b["url"])
                            for b in buttons
                        )
                    ):
                        raise ValueError("invalid imported buttons ◡")
        return data

    async def owner_input(self, m: T.Message, section: str) -> None:
        self.owner_only(m)
        text = m.text or ""
        if text == "/cancel":
            await self.redis.delete(f"{PREFIX}owner_input:{m.from_user.id}")
            return await self.say(m, "cancelled ˚")
        if section in {"updates", "support"}:
            if not valid_url(text) or not text.startswith("https://t.me/"):
                raise ValueError("send an https://t.me/ link ◡")
            if section == "updates":
                await self.db.batch(
                    [
                        ("DELETE FROM update_channel", []),
                        ("INSERT INTO update_channel(url) VALUES (?)", [text]),
                    ]
                )
            else:
                await self.db.set_bot_setting("support", text)
        elif section == "description":
            if not 1 <= len(text) <= 1000:
                raise ValueError("use 1–1000 characters ◡")
            await self.db.set_bot_setting("description", text)
        else:
            table = "main_menu_video" if section == "mainvideo" else "panel_menu_video"
            media_type, media_id = media_ref(m)
            source = None
            if text:
                telegram_link(text)
                source = text
            elif media_type not in {"video", "animation"}:
                raise ValueError("send a video, GIF, or Telegram post link ◡")
            await self.db.batch(
                [
                    (f"DELETE FROM {table}", []),
                    (
                        f"INSERT INTO {table}(file_id,source_link) VALUES (?,?)",
                        [
                            json.dumps({"type": media_type, "id": media_id})
                            if media_id
                            else None,
                            source,
                        ],
                    ),
                ]
            )
        await self.redis.delete(f"{PREFIX}owner_input:{m.from_user.id}")
        await self.say(m, "saved ˚")

    async def delete(self, m: T.Message) -> bool:
        try:
            await self.bot.delete_message(m.chat.id, m.message_id)
            return True
        except TelegramBadRequest:
            LOG.warning(
                "message deletion rejected group=%s message=%s", m.chat.id, m.message_id
            )
            return False

    async def protection(self, m: T.Message, cfg: dict[str, Any]) -> bool:
        gid, uid = m.chat.id, m.from_user.id
        if await self.is_admin(gid, uid):
            return False
        # Fast local cache; Telegram has already accepted delivery before bots see updates.
        shadow = await self.redis.sismember(
            f"{PREFIX}shadowmuted_users:{gid}", str(uid)
        )
        if shadow:
            await self.delete(m)
            return True
        pending = await self.db.one(
            "SELECT 1 FROM captcha_pending WHERE group_id=? AND user_id=?", [gid, uid]
        )
        if pending:
            await self.delete(m)
            return True
        approved = await self.db.one(
            "SELECT is_approved FROM group_members WHERE group_id=? AND user_id=?",
            [gid, uid],
        )
        if approved and approved["is_approved"]:
            return False
        text = m.text or m.caption or ""
        if any(matches(text, p, "wildcard") for p in cfg["allowlist"]):
            return False
        action, cause = None, None
        if cfg["antiflood"] and not await self.rate(
            f"flood:{gid}:{uid}", cfg["flood"], cfg["floodtime"]
        ):
            action, cause = cfg["floodaction"], "flood"
        if cfg["cooldown"] and await self.cooldown_guard(m, cfg["cooldown"]):
            return True
        if text and cfg["antiflood"]:
            digest = hashlib.sha256(normalize(text).encode()).hexdigest()[:24]
            if not await self.rate(f"dup:{gid}:{uid}:{digest}", 3, cfg["dupetime"]):
                action, cause = cfg["setdupemode"], "duplicate messages"
        locks = await self.db.query(
            "SELECT lock_type FROM locks WHERE group_id=? AND is_locked=1", [gid]
        )
        kinds = message_kinds(m)
        if any(row["lock_type"] in kinds for row in locks):
            action, cause = "delete", "locked content"
        if (
            cfg["nopreview"]
            and m.link_preview_options
            and not m.link_preview_options.is_disabled
        ):
            action, cause = "delete", "link preview"
        mentions = sum(
            e.type in {"mention", "text_mention"}
            for e in (m.entities or m.caption_entities or [])
        )
        if mentions > cfg["setmentionlimit"]:
            action, cause = "delete", "too many mentions"
        letters = [c for c in text if c.isalpha()]
        if (
            cfg["setcapslimit"]
            and len(letters) >= 10
            and sum(c.isupper() for c in letters) * 100 / len(letters)
            > cfg["setcapslimit"]
        ):
            action, cause = cfg["capsmode"], "too many capitals"
        if (
            cfg["setemojilimit"]
            and len(regex.findall(r"\p{Extended_Pictographic}", text))
            > cfg["setemojilimit"]
        ):
            action, cause = cfg["emojimode"], "too many emoji"
        if (
            m.sticker
            and m.sticker.set_name
            and await self.db.one(
                "SELECT 1 FROM sticker_pack_bans WHERE group_id=? AND pack_name=?",
                [gid, m.sticker.set_name],
            )
        ):
            action, cause = "delete", "blocked sticker pack"
        origin = m.forward_origin
        source = getattr(origin, "chat", None) or getattr(origin, "sender_chat", None)
        if source and await self.db.one(
            "SELECT 1 FROM forward_source_bans WHERE group_id=? AND source_chat_id=?",
            [gid, source.id],
        ):
            action, cause = "delete", "blocked forward source"
        blocked = await self.db.query("SELECT * FROM blocklist WHERE group_id=?", [gid])
        for entry in blocked:
            if matches(text, entry["phrase"], entry["match_type"]):
                action, cause = cfg["blocklistmode"], cfg["setblocklistreason"]
                break
        if action:
            await self.delete(m)
            # Penalize at most once per 30s despite concurrent updates/flood bursts.
            if action != "delete" and await self.redis.set(
                f"{PREFIX}punish:{gid}:{uid}", "1", ex=30, nx=True
            ):
                await self.action(
                    gid,
                    self.me.id,
                    uid,
                    action,
                    cause,
                    3600 if action == "mute" else 0,
                    automatic=True,
                )
            await self.audit(
                gid, self.me.id, "protection", uid, {"reason": cause, "action": action}
            )
            return True
        for row in await self.db.query("SELECT * FROM filters WHERE group_id=?", [gid]):
            if m.from_user.is_bot and not row["allow_bots"]:
                continue
            if matches(text, row["keyword"], row["match_type"]):
                if row["action"] == "delete":
                    await self.delete(m)
                    return True
                if await self.rate(f"filter:{gid}:{row['filter_id']}", 3, 30):
                    await self.show(
                        gid, uid, Screen("marie", row["reply_text"]), private=False
                    )
        for cap, label in [
            ("spam", "spam guard"),
            ("toxic", "toxicity guard"),
            ("pii", "content guard"),
        ]:
            if not cfg[cap + "_enabled"] or not text or cap not in self.guards.models:
                continue
            if not await self.rate(f"ai:{gid}", 60, 60):
                LOG.warning("guard capacity reached group=%s", gid)
                break
            score = await self.guards.score(cap, text)
            threshold = cfg[cap + "_threshold"]
            if score >= threshold:
                if cap in {"spam", "pii"}:
                    await self.delete(m)
                    taken = "delete"
                else:
                    taken = "mute" if score >= 0.95 else "warn"
                    if await self.redis.set(
                        f"{PREFIX}punish:{gid}:{uid}", "1", ex=30, nx=True
                    ):
                        await self.action(
                            gid,
                            self.me.id,
                            uid,
                            taken,
                            label,
                            3600 if taken == "mute" else 0,
                            automatic=True,
                        )
                # Never persist PII/plaintext by default; hash is enough for correlation.
                await self.db.execute(
                    "INSERT INTO ai_log(group_id,user_id,message_text,capability,score,action_taken,created_at) VALUES (?,?,?,?,?,?,?)",
                    [
                        gid,
                        uid,
                        hashlib.sha256(text.encode()).hexdigest(),
                        label,
                        score,
                        taken,
                        now(),
                    ],
                )
                await self.audit(
                    gid,
                    self.me.id,
                    "ai_action",
                    uid,
                    {"capability": label, "score": score, "action": taken},
                )
                return True
            if (
                cap == "spam"
                and score >= 0.60
                and await self.rate(f"aiflag:{gid}:{uid}", 1, 60)
            ):
                for admin in await self.bot.get_chat_administrators(gid):
                    if not admin.user.is_bot:
                        await self.show(
                            gid,
                            admin.user.id,
                            Screen(
                                "spam guard",
                                f"member {uid} · message {m.message_id} · confidence {score:.2f}",
                                [
                                    "review the message with /del; no automatic action below threshold"
                                ],
                            ),
                        )
        if (
            cfg["setxp"]
            and not m.from_user.is_bot
            and await self.redis.set(f"{PREFIX}xp:{gid}:{uid}", "1", ex=60, nx=True)
        ):
            await self.db.execute(
                "UPDATE group_members SET xp=xp+? WHERE group_id=? AND user_id=?",
                [cfg["setxprate"], gid, uid],
            )
        return False

    async def on_join(self, chat: T.Chat, user: T.User) -> None:
        gid, uid = chat.id, user.id
        if user.is_bot:
            return
        cfg = await self.db.settings(gid)
        await self.db.execute(
            "INSERT INTO group_members(group_id,user_id,joined_at,left_at) VALUES (?,?,?,NULL) ON CONFLICT(group_id,user_id) DO UPDATE SET joined_at=excluded.joined_at,left_at=NULL",
            [gid, uid, now()],
        )
        await self.db.execute(
            "INSERT INTO users(user_id,username,first_name,last_name,is_bot,first_seen,last_seen) VALUES (?,?,?,?,0,?,?) ON CONFLICT(user_id) DO UPDATE SET username=excluded.username,last_seen=excluded.last_seen",
            [uid, user.username, user.first_name, user.last_name, now(), now()],
        )
        if await self.is_admin(gid, uid):
            return
        approved = await self.db.one(
            "SELECT is_approved FROM group_members WHERE group_id=? AND user_id=?",
            [gid, uid],
        )
        if not approved or not approved["is_approved"]:
            patterns = await self.db.query(
                "SELECT pattern FROM bioblacklist WHERE group_id=?", [gid]
            )
            if patterns:
                try:
                    profile = await self.bot.get_chat(uid)
                    bio = getattr(profile, "bio", None) or ""
                except TelegramAPIError:
                    bio = ""
                    await self.db.audit(gid, self.me.id, "bio_unavailable", uid)
                if any(matches(bio, p["pattern"]) for p in patterns):
                    await self.action(
                        gid, self.me.id, uid, "kick", "biography policy", automatic=True
                    )
                    return
        fed = await self.db.one(
            "WITH RECURSIVE sources(id) AS (SELECT fed_id FROM fed_members WHERE group_id=? UNION SELECT s.subscribed_to_fed_id FROM fed_subscriptions s JOIN sources x ON s.fed_id=x.id) SELECT b.* FROM fed_bans b JOIN sources s ON b.fed_id=s.id WHERE user_id=? LIMIT 1",
            [gid, uid],
        )
        if fed:
            await self.action(
                gid,
                self.me.id,
                uid,
                "ban",
                "federation · " + (fed["reason"] or ""),
                automatic=True,
            )
            return
        count_ok = await self.rate(f"joins:{gid}", 10, 30)
        if cfg["antiraid"] and not count_ok:
            await self.redis.set(f"{PREFIX}raid:{gid}", "1", ex=cfg["raidtime"])
        if (
            await self.redis.exists(f"{PREFIX}raid:{gid}")
            or cfg["nousername"]
            and not user.username
        ):
            await self.action(
                gid,
                self.me.id,
                uid,
                cfg["raidaction"],
                "join protection",
                3600 if cfg["raidaction"] == "mute" else 0,
                automatic=True,
            )
            return
        if cfg["captcha"]:
            await self.start_captcha(chat, user, cfg)
        else:
            await self.greet(chat, user)
        for row in await self.db.query(
            "SELECT * FROM drip_sequences WHERE group_id=?", [gid]
        ):
            await self.redis.enqueue_job(
                "deliver_drip",
                gid,
                uid,
                row["seq_id"],
                _defer_by=row["delay_seconds"],
                _job_id=f"{PREFIX}drip:{gid}:{uid}:{row['seq_id']}:{int(now())}",
            )
        await self.audit(gid, self.me.id, "join", uid)

    async def greet(self, chat: T.Chat, user: T.User, goodbye: bool = False) -> None:
        table = "goodbyes" if goodbye else "welcomes"
        row = await self.db.one(f"SELECT * FROM {table} WHERE group_id=?", [chat.id])
        if not row:
            return
        content = render_template(
            row["goodbye_text" if goodbye else "welcome_text"], user, chat
        )
        links = json.loads(row["buttons_json"] or "[]")
        buttons = [button(x["text"], url=x["url"]) for x in links]
        if row["media_id"] and row["media_type"] in {
            "photo",
            "video",
            "animation",
            "document",
        }:
            await getattr(self.bot, "send_" + row["media_type"])(
                chat_id=chat.id, **{row["media_type"]: row["media_id"]}
            )
        await self.show(
            chat.id,
            user.id,
            Screen(
                "goodbye" if goodbye else "welcome",
                lines=content.splitlines(),
                rows=[buttons[i : i + 3] for i in range(0, len(buttons), 3)] or None,
            ),
            private=False,
        )

    async def start_captcha(
        self, chat: T.Chat, user: T.User, cfg: dict[str, Any]
    ) -> None:
        token = secrets.token_urlsafe(9)
        a, b = secrets.randbelow(9) + 1, secrets.randbelow(9) + 1
        mode = cfg["captchamode"]
        if mode == "button":
            question, answer, choices = "tap verify to enter ·͜·", "verify", ["verify"]
        elif mode == "math":
            answer = str(a + b)
            choices = [str(a + b), str(a + b + 1), str(max(0, a + b - 1))]
            secrets.SystemRandom().shuffle(choices)
            question = f"{a} + {b} = ?"
        else:
            choices = ["moon", "star", "cloud"]
            answer = secrets.choice(choices)
            question = f"tap the word: {answer}"
        member = await self.bot.get_chat_member(chat.id, user.id)
        current_chat = await self.bot.get_chat(chat.id)
        perms = {
            k: getattr(member, k)
            for k in T.ChatPermissions.model_fields
            if hasattr(member, k)
        }
        if member.status != "restricted":
            perms = (
                current_chat.permissions or T.ChatPermissions(can_send_messages=True)
            ).model_dump(exclude_none=True)
        data = {
            "user": user.id,
            "group": chat.id,
            "answer": answer,
            "permissions": perms,
            "expires": now() + cfg["captchakicktime"],
            "action": cfg["captchaaction"],
        }
        await self.redis.set(
            f"{PREFIX}captcha:{token}",
            json.dumps(data),
            ex=cfg["captchakicktime"] + 300,
        )
        await self.bot.restrict_chat_member(
            chat.id,
            user.id,
            T.ChatPermissions(can_send_messages=False),
            until_date=datetime.now(UTC)
            + timedelta(
                seconds=max(cfg["captchamutetime"], cfg["captchakicktime"] + 60)
            ),
            use_independent_chat_permissions=True,
        )
        await self.db.execute(
            "INSERT OR REPLACE INTO captcha_pending(user_id,group_id,challenge,answer,expires_at) VALUES (?,?,?,?,?)",
            [user.id, chat.id, token, answer, data["expires"]],
        )
        await self.show(
            chat.id,
            user.id,
            Screen(
                "verify",
                cfg["captcharules"],
                [question, f"expires in {cfg['captchakicktime']}s"],
                [[button(x, f"captcha:{token}:{x}", "success") for x in choices]],
            ),
        )
        await self.redis.enqueue_job(
            "captcha_expire",
            token,
            _defer_by=cfg["captchakicktime"],
            _job_id=f"{PREFIX}captcha-expire:{token}",
        )

    async def solve_captcha(self, call: T.CallbackQuery) -> None:
        _, token, selected = call.data.split(":", 2)
        key = f"{PREFIX}captcha:{token}"
        raw = await self.redis.get(key)
        if not raw:
            raise ValueError("this challenge expired ◡")
        data = json.loads(raw)
        if data["user"] != call.from_user.id or call.message.chat.id not in {
            data["group"],
            data["user"],
        }:
            raise ValueError("this challenge isn't yours ◡")
        if data["expires"] <= now():
            raise ValueError("this challenge expired ◡")
        if selected != data["answer"]:
            await self.redis.set(key, json.dumps(data | {"expires": now()}), ex=300)
            await self.redis.enqueue_job("captcha_expire", token)
            raise ValueError("not quite; verification failed ◡")
        # Atomic lock avoids simultaneous approve/expire and repeated callbacks.
        lock = f"{PREFIX}captcha-lock:{token}"
        if not await self.redis.set(lock, "1", ex=60, nx=True):
            raise ValueError("verification is already being checked ◡")
        try:
            if await self.redis.get(key) != raw:
                raise ValueError("this challenge changed ◡")
            await self.bot.restrict_chat_member(
                data["group"],
                data["user"],
                T.ChatPermissions(**data["permissions"]),
                use_independent_chat_permissions=True,
            )
            await self.redis.delete(key)
            await self.db.execute(
                "DELETE FROM captcha_pending WHERE group_id=? AND user_id=?",
                [data["group"], data["user"]],
            )
            await self.show(
                call.message.chat.id,
                call.from_user.id,
                Screen("marie", "verified ✦"),
                callback=call,
            )
            chat = await self.bot.get_chat(data["group"])
            await self.greet(chat, call.from_user)
        finally:
            await self.redis.delete(lock)

    async def handle_message(self, m: T.Message) -> None:
        if not m.from_user:
            return
        uid, gid = m.from_user.id, m.chat.id
        if gid < 0 and await self.redis.sismember(
            f"{PREFIX}shadowmuted_users:{gid}", str(uid)
        ):
            await self.delete(m)
            return
        text = m.text or ""
        if m.sender_chat and text.startswith("/"):
            return  # Anonymous administrators cannot be securely identified; use DM connection.
        if m.chat.type in {"group", "supergroup"} and await self.db.one(
            "SELECT 1 FROM blacklisted_groups WHERE group_id=?", [gid]
        ):
            await self.bot.leave_chat(gid)
            return
        if uid != self.owner and await self.db.bot_setting("maintenance", False):
            if text.startswith("/"):
                await self.say(m, "resting for maintenance · back soon ☽")
            return
        if uid == self.owner and m.chat.type == "private":
            pending = await self.redis.get(f"{PREFIX}owner_input:{uid}")
            if pending:
                await self.owner_input(m, pending)
                return
        if await self.phase_input(m):
            return
        await self.remember(m)
        if m.chat.type == "private":
            await self.offer_appeals(m)
        if m.chat.type in {"group", "supergroup"}:
            cfg = await self.db.settings(gid)
            if m.left_chat_member:
                await self.db.execute(
                    "UPDATE group_members SET left_at=? WHERE group_id=? AND user_id=?",
                    [now(), gid, m.left_chat_member.id],
                )
                await self.greet(m.chat, m.left_chat_member, goodbye=True)
            # Join processing uses chat_member updates only to avoid duplicate greetings/jobs.
            service = bool(
                m.new_chat_members
                or m.left_chat_member
                or m.pinned_message
                or m.forum_topic_created
                or m.forum_topic_closed
                or m.forum_topic_reopened
            )
            if service:
                types = set(cfg["cleanservicetypes"])
                if cfg["cleanservice"] and (not types or m.content_type in types):
                    await self.delete(m)
                return
            if m.from_user.is_bot and not cfg["bot2bot"]:
                return
            if await self.protection(m, cfg):
                return
        if text.startswith("/"):
            first, _, argtext = text.partition(" ")
            token, _, username = first[1:].partition("@")
            if (
                username
                and self.me
                and username.casefold() != self.me.username.casefold()
            ):
                return
            cmd = token.lower()
            if cmd != "start" and not await self.rate(f"cmd:{uid}", 10, 10):
                raise ValueError("a little slower, please ◡")
            await self.command(m, cmd, argtext.strip())
            if m.chat.type in {"group", "supergroup"}:
                cfg = await self.db.settings(gid)
                keep = await self.redis.getdel(f"{PREFIX}keep:{gid}:{uid}")
                if (
                    cfg["cleancommand"]
                    and not keep
                    and (
                        not cfg["cleancommandtypes"] or cmd in cfg["cleancommandtypes"]
                    )
                ):
                    await self.delete(m)
        elif re.fullmatch(r"#[\w-]{1,40}", text) and m.chat.type in {
            "group",
            "supergroup",
        }:
            await self.content_command(m, gid, "get", [text[1:]], text[1:])


EXPORT_FIELDS = {
    "locks": ["lock_type", "is_locked", "allowlist_json"],
    "blocklist": ["phrase", "match_type", "action", "reason"],
    "filters": [
        "keyword",
        "action",
        "reply_text",
        "reply_media_id",
        "match_type",
        "allow_bots",
    ],
    "notes": ["name", "content", "media_id", "media_type", "buttons_json"],
    "welcomes": [
        "welcome_text",
        "media_id",
        "media_type",
        "buttons_json",
        "is_rich",
        "delete_after_seconds",
    ],
    "goodbyes": ["goodbye_text", "media_id", "media_type", "buttons_json"],
    "rules": ["rules_text", "is_rich", "buttons_json"],
}


def telegram_link(value: str) -> tuple[int | str, int]:
    from urllib.parse import urlsplit

    parsed = urlsplit(value.strip())
    if parsed.scheme != "https" or parsed.netloc != "t.me":
        raise ValueError("use an https://t.me/ post link ◡")
    parts = parsed.path.strip("/").split("/")
    if parts[0] == "c" and len(parts) in (3, 4) and all(x.isdigit() for x in parts[1:]):
        return int("-100" + parts[1]), int(parts[-1])
    if (
        len(parts) in (2, 3)
        and re.fullmatch(r"[A-Za-z][\w]{3,31}", parts[0])
        and all(x.isdigit() for x in parts[1:])
    ):
        return "@" + parts[0], int(parts[-1])
    raise ValueError("that link needs a post number; I also need access to its chat ◡")


def valid_url(value: Any) -> bool:
    from urllib.parse import urlsplit

    if (
        not isinstance(value, str)
        or len(value) > 1000
        or any(c.isspace() for c in value)
    ):
        return False
    url = urlsplit(value)
    return (
        url.scheme == "https"
        and bool(url.netloc)
        and not url.username
        and not url.password
    )


def media_ref(m: T.Message | None) -> tuple[str | None, str | None]:
    if not m:
        return None, None
    if m.photo:
        return "photo", m.photo[-1].file_id
    for name in ("animation", "video", "document"):
        media = getattr(m, name)
        if media:
            return name, media.file_id
    return None, None


def render_template(text: str, user: T.User, chat: T.Chat) -> str:
    # Plain text passed to rich blocks: no Markdown/HTML or format-string execution.
    for key, value in {
        "first": user.first_name,
        "last": user.last_name or "",
        "fullname": user.full_name,
        "username": "@" + user.username if user.username else user.first_name,
        "id": str(user.id),
        "chatname": chat.title or "this group",
    }.items():
        text = text.replace("{" + key + "}", value)
    return text


def message_kinds(m: T.Message) -> set[str]:
    kinds: set[str] = set()
    attrs = {
        "sticker": "sticker",
        "animation": "gif",
        "game": "game",
        "poll": "poll",
        "location": "location",
        "contact": "contact",
        "audio": "audio",
        "video": "video",
        "document": "document",
        "voice": "voice",
        "video_note": "videonote",
    }
    for attr, name in attrs.items():
        if getattr(m, attr, None):
            kinds.add(name)
    if m.text:
        kinds.add("text")
    if m.photo or kinds & {
        "video",
        "document",
        "voice",
        "videonote",
        "audio",
        "gif",
        "sticker",
    }:
        kinds.add("media")
    if m.via_bot:
        kinds.add("inline")
    if m.forward_origin:
        kinds.add("forward")
    if m.reply_markup:
        kinds.add("button")
    text = m.text or m.caption or ""
    if text.startswith("/"):
        kinds.add("command")
    entities = m.entities or m.caption_entities or []
    if any(e.type in {"url", "text_link"} for e in entities):
        kinds.add("url")
    if any(e.type == "spoiler" for e in entities) or m.has_media_spoiler:
        kinds.add("spoiler")
    if any(unicodedata.bidirectional(c) in {"R", "AL", "RLE", "RLO"} for c in text):
        kinds.add("rtl")
    if regex.search(r"\p{Extended_Pictographic}", text):
        kinds.add("emoji")
    return kinds


# ARQ job functions live in this file and run on the same event loop.
async def deliver_post(ctx: dict[str, Any], post_id: str) -> None:
    app: Marie = ctx["app"]
    row = await app.db.one(
        "SELECT * FROM scheduled_posts WHERE post_id=? AND is_active=1", [post_id]
    )
    if not row or row["run_at"] > now() + 1:
        return
    lock = f"{PREFIX}post-lock:{post_id}"
    if not await app.redis.set(lock, "1", ex=120, nx=True):
        return
    try:
        payload = json.loads(row["content_json"])
        # Re-check the creator; revoked administrators cannot retain scheduled posting rights.
        if not await app.is_admin(row["group_id"], payload["creator"]):
            await app.db.execute(
                "UPDATE scheduled_posts SET is_active=0 WHERE post_id=?", [post_id]
            )
            return
        await app.bot.send_rich_message(
            row["group_id"],
            Screen("scheduled", lines=[payload["text"]]).rich(),
            message_thread_id=row["topic_id"],
        )
        if row["recur_cron"]:
            next_run = (
                croniter(row["recur_cron"], datetime.now(UTC))
                .get_next(datetime)
                .timestamp()
            )
            await app.db.execute(
                "UPDATE scheduled_posts SET run_at=? WHERE post_id=? AND is_active=1",
                [next_run, post_id],
            )
            await app.enqueue_post(post_id, next_run)
        else:
            await app.db.execute(
                "UPDATE scheduled_posts SET is_active=0 WHERE post_id=?", [post_id]
            )
    finally:
        await app.redis.delete(lock)


async def deliver_drip(ctx: dict[str, Any], gid: int, uid: int, seq_id: int) -> None:
    app: Marie = ctx["app"]
    if await app.db.one(
        "SELECT 1 FROM drip_sent WHERE user_id=? AND group_id=? AND seq_id=?",
        [uid, gid, seq_id],
    ):
        return
    row = await app.db.one(
        "SELECT * FROM drip_sequences WHERE group_id=? AND seq_id=?", [gid, seq_id]
    )
    member = await app.bot.get_chat_member(gid, uid)
    if not row or member.status in {"left", "kicked"}:
        return
    if await app.db.one(
        "SELECT 1 FROM captcha_pending WHERE group_id=? AND user_id=?", [gid, uid]
    ):
        return
    await app.show(
        gid,
        uid,
        Screen("a little note", lines=[json.loads(row["content_json"])["text"]]),
    )
    await app.db.execute(
        "INSERT OR IGNORE INTO drip_sent(user_id,group_id,seq_id,sent_at) VALUES (?,?,?,?)",
        [uid, gid, seq_id, now()],
    )


async def captcha_expire(ctx: dict[str, Any], token: str) -> None:
    app: Marie = ctx["app"]
    key, lock = f"{PREFIX}captcha:{token}", f"{PREFIX}captcha-lock:{token}"
    if not await app.redis.set(lock, "1", ex=60, nx=True):
        return
    try:
        raw = await app.redis.get(key)
        if not raw:
            return
        data = json.loads(raw)
        if data["expires"] > now():
            return
        await app.action(
            data["group"],
            app.me.id,
            data["user"],
            data["action"],
            "verification expired",
            3600 if data["action"] == "mute" else 0,
            automatic=True,
        )
        await app.redis.delete(key)
        await app.db.execute(
            "DELETE FROM captcha_pending WHERE group_id=? AND user_id=?",
            [data["group"], data["user"]],
        )
    finally:
        await app.redis.delete(lock)


async def sync_federation(ctx: dict[str, Any], gid: int) -> None:
    app: Marie = ctx["app"]
    rows = await app.db.query(
        "WITH RECURSIVE sources(id) AS (SELECT fed_id FROM fed_members WHERE group_id=? UNION SELECT s.subscribed_to_fed_id FROM fed_subscriptions s JOIN sources x ON s.fed_id=x.id) SELECT DISTINCT b.user_id,b.reason FROM fed_bans b JOIN sources s ON b.fed_id=s.id",
        [gid],
    )
    for row in rows:
        try:
            await app.action(
                gid,
                app.me.id,
                row["user_id"],
                "ban",
                "federation sync · " + (row["reason"] or ""),
                automatic=True,
            )
            await asyncio.sleep(0.05)
        except ValueError:
            LOG.warning(
                "federation sync skipped protected member group=%s user=%s",
                gid,
                row["user_id"],
            )


async def broadcast_job(
    ctx: dict[str, Any], payload: dict[str, Any], actor: int
) -> None:
    app: Marie = ctx["app"]
    if actor != app.owner:
        return
    target = payload["target"]
    if target == "groups":
        rows = await app.db.query("SELECT group_id id FROM groups WHERE active=1")
    elif target == "users":
        rows = await app.db.query(
            "SELECT user_id id FROM users WHERE dm_opt_in=1 AND is_bot=0"
        )
    else:
        rows = [{"id": int(target)}]
    started = time.monotonic()
    last_update = 0.0
    done = failed = skipped = pin_unavailable = 0
    loc = payload.get("progress", {"chat": actor, "message": None})
    key = (
        payload.get("broadcast_id")
        or hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:24]
    )
    for index, row in enumerate(rows):
        tid = row["id"]
        existing = await app.db.one(
            "SELECT status FROM broadcast_delivery WHERE broadcast_key=? AND target_id=?",
            [key, tid],
        )
        if existing:
            if existing["status"] == "sent":
                done += 1
            elif existing["status"] == "skipped":
                skipped += 1
            else:
                failed += 1
            continue
        status = "failed"
        try:
            fn = (
                app.bot.forward_message
                if payload.get("forward")
                else app.bot.copy_message
            )
            try:
                sent = await fn(tid, payload["source"], payload["message"])
            except TelegramRetryAfter as exc:
                await asyncio.sleep(exc.retry_after + 1)
                sent = await fn(tid, payload["source"], payload["message"])
            status = "sent"
            done += 1
            if payload.get("pin"):
                try:
                    await app.bot.pin_chat_message(
                        tid, sent.message_id, disable_notification=True
                    )
                except TelegramAPIError:
                    pin_unavailable += 1
        except TelegramForbiddenError:
            status = "skipped"
            skipped += 1
            if tid > 0:
                await app.db.execute(
                    "UPDATE users SET dm_opt_in=0 WHERE user_id=?", [tid]
                )
        except TelegramAPIError:
            failed += 1
            LOG.warning("broadcast delivery unavailable target=%s", tid)
        await app.db.execute(
            "INSERT OR REPLACE INTO broadcast_delivery(broadcast_key,target_id,status) VALUES (?,?,?)",
            [key, tid, status],
        )
        if time.monotonic() - last_update >= 5:
            await progress_screen(
                app,
                loc,
                Screen("broadcast", f"sending... {index + 1} / {len(rows)} {target}"),
                actor,
            )
            last_update = time.monotonic()
        await asyncio.sleep(0.06)
    elapsed = time.monotonic() - started
    await app.db.execute(
        "INSERT INTO broadcast_history(admin_id,content_json,target_type,sent_count,failed_count,skipped_count,time_seconds,created_at) VALUES (?,?,?,?,?,?,?,?)",
        [actor, json.dumps(payload), target, done, failed, skipped, elapsed, now()],
    )
    await progress_screen(
        app,
        loc,
        Screen(
            "delivery report",
            table=(
                [
                    "sent",
                    "not delivered",
                    "skipped · blocked",
                    "seconds",
                    "pin unavailable",
                ],
                [
                    [
                        str(done),
                        str(failed),
                        str(skipped),
                        f"{elapsed:.1f}",
                        str(pin_unavailable),
                    ]
                ],
            ),
        ),
        actor,
    )


async def confirmation_tick(ctx: dict[str, Any], token: str) -> None:
    app: Marie = ctx["app"]
    raw = await app.redis.get(f"{PREFIX}countdown:{token}")
    if not raw:
        return
    state = json.loads(raw)
    confirmation = await app.redis.get(f"{PREFIX}confirm:{token}")
    remaining = max(0, math.ceil(state["expires"] - now())) if confirmation else 0
    loc = state["locator"]
    actor = state["actor"]
    screen = Screen(
        "confirm",
        state["description"],
        [f"expires in {remaining}s · only you can confirm"]
        if remaining
        else ["this confirmation expired ◡"],
        [
            [
                button(
                    f"✦ confirm ({remaining}s)"
                    if remaining
                    else "⌁ confirmation expired",
                    f"confirm:{token}" if remaining else None,
                    "success",
                ),
                button("⌁ cancel", f"cancel:{token}"),
            ]
        ]
        if remaining
        else [[button("⌁ confirmation expired")]],
    )
    if not await app.redis.get(f"{PREFIX}countdown:{token}"):
        return
    await progress_screen(app, loc, screen, actor)
    if not remaining:
        await app.redis.delete(f"{PREFIX}countdown:{token}")
        return
    job_id = f"{PREFIX}countdown-job:{token}:{secrets.token_hex(4)}"
    await app.redis.enqueue_job(
        "confirmation_tick", token, _defer_by=min(5, remaining), _job_id=job_id
    )
    await app.redis.set(f"{PREFIX}countdown-active:{token}", job_id, ex=60)


async def warm_shadow_cache(app: Marie) -> None:
    # Startup happens under the single-instance lease before polling begins.
    async for key in app.redis.scan_iter(
        match=PREFIX + "shadowmuted_users:*", count=100
    ):
        await app.redis.delete(key)
    rows = await app.db.query(
        "SELECT group_id,user_id FROM mutes WHERE is_shadow=1 AND resolved=0 AND (expires_at IS NULL OR expires_at>?)",
        [now()],
    )
    for row in rows:
        await app.redis.sadd(
            f"{PREFIX}shadowmuted_users:{row['group_id']}", str(row["user_id"])
        )


async def birthday_daily(ctx: dict[str, Any]) -> None:
    app: Marie = ctx["app"]
    day = datetime.now(UTC)
    label = day.strftime("%Y-%m-%d")
    rows = await app.db.query(
        "SELECT b.*,coalesce(t.template,'happy birthday ·͜· {user}') template FROM birthdays b JOIN groups g ON b.group_id=g.group_id LEFT JOIN birthday_msg t ON b.group_id=t.group_id WHERE b.month=? AND b.day=? AND g.active=1",
        [day.month, day.day],
    )
    for r in rows:
        try:
            member = await app.bot.get_chat_member(r["group_id"], r["user_id"])
        except TelegramAPIError:
            LOG.warning(
                "birthday membership unavailable group=%s user=%s",
                r["group_id"],
                r["user_id"],
            )
            continue
        if (
            member.status in {"left", "kicked"}
            or member.status == "restricted"
            and not member.is_member
        ):
            continue
        done = await app.db.one(
            "SELECT status FROM birthday_delivery WHERE group_id=? AND user_id=? AND delivery_date=?",
            [r["group_id"], r["user_id"], label],
        )
        if done and done["status"] == "sent":
            continue
        token = secrets.token_hex(8)
        lock = f"{PREFIX}birthday-lock:{r['group_id']}:{r['user_id']}:{label}"
        if not await app.redis.set(lock, token, ex=120, nx=True):
            continue
        try:
            text = r["template"].replace("{user}", member.user.full_name)
            delivered = await app.show(
                r["group_id"], r["user_id"], Screen("birthday", text), private=False
            )
            if delivered is None:
                raise RuntimeError("birthday delivery unavailable")
            await app.db.execute(
                "INSERT OR REPLACE INTO birthday_delivery(group_id,user_id,delivery_date,status) VALUES (?,?,?,'sent')",
                [r["group_id"], r["user_id"], label],
            )
        except (TelegramAPIError, RuntimeError):
            LOG.warning(
                "birthday greeting unavailable group=%s user=%s",
                r["group_id"],
                r["user_id"],
            )
        finally:
            await app.redis.eval(
                "if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) else return 0 end",
                1,
                lock,
                token,
            )


async def global_moderation_job(
    ctx: dict[str, Any], payload: dict[str, Any], actor: int
) -> None:
    app: Marie = ctx["app"]
    if actor != app.owner or payload["target"] in {app.owner, app.me.id}:
        return
    rows = await app.db.query("SELECT group_id FROM groups")
    done = failed = 0
    last = 0.0
    loc = {"chat": actor, "message": None}
    for index, row in enumerate(rows):
        try:
            await app.action(
                row["group_id"],
                actor,
                payload["target"],
                "unban" if payload["unban"] else "ban",
                "owner global action",
                automatic=True,
            )
            done += 1
        except (ValueError, TelegramAPIError):
            failed += 1
        if time.monotonic() - last >= 5:
            await progress_screen(
                app,
                loc,
                Screen(
                    "global action",
                    f"{'unbanning' if payload['unban'] else 'banning'} in {index + 1} / {len(rows)} groups...",
                ),
                actor,
            )
            last = time.monotonic()
        await asyncio.sleep(0.06)
    await progress_screen(
        app,
        loc,
        Screen(
            "global action",
            table=(["updated", "not updated"], [[str(done), str(failed)]]),
        ),
        actor,
    )


async def enforce_permapins(app: Marie, gid: int | None = None) -> None:
    rows = await app.db.query(
        "SELECT group_id,message_id FROM permapin"
        + (" WHERE group_id=?" if gid else ""),
        [gid] if gid else [],
    )
    for row in rows:
        try:
            await app.bot.pin_chat_message(
                row["group_id"], row["message_id"], disable_notification=True
            )
        except TelegramAPIError:
            LOG.warning("permanent pin needs attention group=%s", row["group_id"])


async def reconcile(app: Marie) -> None:
    """Requeue durable jobs after restarts or Redis loss. At-least-once delivery."""
    while not app.stopping.is_set():
        try:
            await enforce_permapins(app)
            rows = await app.db.query(
                "SELECT post_id,run_at FROM scheduled_posts WHERE is_active=1 AND run_at<?",
                [now() + 86400],
            )
            for row in rows:
                await app.enqueue_post(row["post_id"], max(now(), row["run_at"]))
            for row in await app.db.query(
                "SELECT * FROM captcha_pending WHERE expires_at<?", [now()]
            ):
                if not await app.redis.exists(f"{PREFIX}captcha:{row['challenge']}"):
                    # Missing volatile challenge state must not override a later moderator's
                    # restriction. The finite Telegram CAPTCHA restriction expires naturally.
                    await app.db.execute(
                        "DELETE FROM captcha_pending WHERE group_id=? AND user_id=?",
                        [row["group_id"], row["user_id"]],
                    )
                    await app.db.audit(
                        row["group_id"], app.me.id, "captcha_state_lost", row["user_id"]
                    )

        except Exception:
            LOG.exception("job reconciliation failed")
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(app.stopping.wait(), 30)


async def run() -> None:
    load_dotenv()
    required = ["BOT_TOKEN", "OWNER_ID", "TURSO_URL", "TURSO_AUTH_TOKEN", "REDIS_URL"]
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise SystemExit("Missing configuration: " + ", ".join(missing))
    owner = int(os.environ["OWNER_ID"])
    if owner <= 0:
        raise SystemExit("OWNER_ID must be a positive Telegram user id")
    import ssl, certifi, aiohttp
    _sslctx = ssl.create_default_context(cafile=certifi.where())
    from aiogram.client.session.aiohttp import AiohttpSession

    class _Session(AiohttpSession):
        def __init__(self, *a, limit=100, **k):
            super().__init__(*a, **k)
            self._limit = limit

        async def create_session(self):
            # Test env may intercept TLS; allow opt-out via TELEGRAM_SSL_VERIFY=false.
            verify = os.getenv("TELEGRAM_SSL_VERIFY", "true").lower() not in {"0","false","no"}
            ssl_arg = _sslctx if verify else False
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(
                    connector=aiohttp.TCPConnector(
                        ssl=ssl_arg, limit=getattr(self, "_limit", 100)
                    ),
                    json_serialize=self.json_dumps,
                )
            return self._session

    bot = Bot(
        os.environ["BOT_TOKEN"],
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
        session=_Session(),
    )
    db = DB(os.environ["TURSO_URL"], os.environ["TURSO_AUTH_TOKEN"])
    redis = await create_pool(RedisSettings.from_dsn(os.environ["REDIS_URL"]))
    # Application Redis strings are decoded explicitly; ARQ needs binary responses.
    redis_state = DecodedRedis(redis)
    guards = LocalGuards()
    app = Marie(bot, db, redis_state, owner, guards)
    dispatcher = Dispatcher()
    worker = None
    runner = None
    tasks: list[asyncio.Task] = []
    lease = secrets.token_hex(16)
    acquired = False
    try:
        acquired = bool(
            await redis.set(PREFIX + "runtime-lease", lease, nx=True, ex=60)
        )
        if not acquired:
            raise RuntimeError(
                "Only one long-polling Marie instance is allowed per Redis namespace"
            )
        await redis.ping()
        await db.initialize()
        await asyncio.get_running_loop().run_in_executor(guards.pool, guards.load_sync)
        app.me = await bot.get_me()
        await warm_shadow_cache(app)

        async def track_inflight(handler: Any, event: Any, data: dict[str, Any]) -> Any:
            task = asyncio.current_task()
            if task:
                app.inflight.add(task)
            try:
                return await handler(event, data)
            finally:
                if task:
                    app.inflight.discard(task)

        dispatcher.update.outer_middleware(track_inflight)
        dispatcher.message.register(app.handle_message)
        dispatcher.callback_query.register(app.callback)
        dispatcher.message_reaction.register(app.record_reaction)

        async def member_update(event: T.ChatMemberUpdated) -> None:
            await enforce_permapins(app, event.chat.id)
            old = event.old_chat_member
            new = event.new_chat_member
            if new.status in {"administrator", "creator"}:
                await app.redis.srem(
                    f"{PREFIX}shadowmuted_users:{event.chat.id}", str(new.user.id)
                )
                await db.execute(
                    "UPDATE mutes SET resolved=1 WHERE group_id=? AND user_id=? AND is_shadow=1",
                    [event.chat.id, new.user.id],
                )
            was_in = (
                old.status in {"member", "administrator", "creator"}
                or old.status == "restricted"
                and old.is_member
            )
            is_in = (
                new.status in {"member", "administrator", "creator"}
                or new.status == "restricted"
                and new.is_member
            )
            if not was_in and is_in:
                # Groups may deliver member updates before their first ordinary message.
                await db.execute(
                    "INSERT INTO groups(group_id,title,joined_at) VALUES (?,?,?) ON CONFLICT(group_id) DO UPDATE SET title=excluded.title",
                    [event.chat.id, event.chat.title, now()],
                )
                await app.on_join(event.chat, new.user)

        async def self_update(event: T.ChatMemberUpdated) -> None:
            gid = event.chat.id
            blocked = await db.one(
                "SELECT 1 FROM blacklisted_groups WHERE group_id=?", [gid]
            )
            joining = event.old_chat_member.status in {"left", "kicked"}
            if blocked or joining and not await db.bot_setting("new_joins", True):
                if event.new_chat_member.status not in {"left", "kicked"}:
                    await bot.leave_chat(gid)
                return
            active = event.new_chat_member.status not in {"left", "kicked"}
            await db.execute(
                "INSERT INTO groups(group_id,title,joined_at,active) VALUES (?,?,?,?) ON CONFLICT(group_id) DO UPDATE SET title=excluded.title,active=excluded.active",
                [gid, event.chat.title, now(), int(active)],
            )
            if active:
                count = await bot.get_chat_member_count(gid)
                await db.execute(
                    "UPDATE groups SET member_count=? WHERE group_id=?", [count, gid]
                )

        async def errors(event: T.ErrorEvent) -> bool:
            update = event.update
            call = update.callback_query
            m = update.message or (
                call.message if call and isinstance(call.message, T.Message) else None
            )
            u = call.from_user if call else (m.from_user if m else None)
            err = event.exception
            LOG.error(
                "handler failed group=%s user=%s command=%s",
                m.chat.id if m else None,
                u.id if u else None,
                (m.text or "").split(" ")[0] if m else "callback",
                exc_info=(type(err), err, err.__traceback__),
            )
            if isinstance(err, (ValueError, IndexError, KeyError)):
                message = friendly_error(err)
            elif isinstance(err, TelegramForbiddenError):
                message = "I need permission to do that ᯓ"
            elif isinstance(err, TelegramBadRequest):
                message = (
                    "Telegram couldn't apply that; check my rights and the target ◡"
                )
            else:
                message = "hmm, something needs attention; no success recorded ◡"
            if m and u:
                with contextlib.suppress(TelegramAPIError):
                    await app.show(
                        m.chat.id, u.id, Screen("marie", message), callback=call
                    )
            return True

        dispatcher.chat_member.register(member_update)
        dispatcher.my_chat_member.register(self_update)
        dispatcher.errors.register(errors)
        # Join requests are intentionally not auto-approved; owner policy must opt in.
        await bot.set_my_commands(
            [
                T.BotCommand(command=c, description=d, is_ephemeral=True)
                for c, d in [
                    ("help", "command guide"),
                    ("menu", "group menu"),
                    ("settings", "group settings"),
                    ("report", "report a message"),
                ]
            ]
        )
        http = web.Application(client_max_size=MAX_FILE)

        async def health(_: web.Request) -> web.Response:
            return web.json_response(
                {
                    "status": "ok",
                    "uptime": int(now() - app.started),
                    "capabilities": {
                        cap: cap in guards.models for cap in ("spam", "toxic", "pii")
                    },
                }
            )

        async def ready(_: web.Request) -> web.Response:
            try:
                await asyncio.wait_for(redis.ping(), 3)
                await asyncio.wait_for(db.one("SELECT 1"), 3)
                if worker and worker.main_task and worker.main_task.done():
                    raise RuntimeError("worker stopped")
                return web.json_response({"ready": True})
            except Exception:
                return web.json_response({"ready": False}, status=503)

        http.router.add_get("/health", health)
        http.router.add_get("/ready", ready)
        runner = web.AppRunner(http, access_log=None)
        await runner.setup()
        await web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", "8080"))).start()
        worker = Worker(
            functions=[
                deliver_post,
                deliver_drip,
                captcha_expire,
                sync_federation,
                broadcast_job,
                global_moderation_job,
                confirmation_tick,
            ],
            queue_name=PREFIX + "jobs",
            cron_jobs=[
                cron(
                    birthday_daily,
                    hour=0,
                    minute=1,
                    second=0,
                    microsecond=0,
                    unique=True,
                )
            ],
            timezone=UTC,
            redis_pool=redis,
            ctx={"app": app},
            handle_signals=False,
            max_jobs=5,
            job_timeout=600,
            job_completion_wait=30,
            max_tries=3,
            keep_result=0,
        )
        tasks.append(asyncio.create_task(worker.async_run(), name="arq"))
        tasks.append(asyncio.create_task(reconcile(app), name="reconcile"))

        async def renew_lease() -> None:
            script = "if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('EXPIRE',KEYS[1],60) else return 0 end"
            while not app.stopping.is_set():
                await asyncio.sleep(20)
                if not await redis.eval(script, 1, PREFIX + "runtime-lease", lease):
                    LOG.critical("runtime lease lost; stopping polling")
                    app.stopping.set()
                    await dispatcher.stop_polling()
                    return

        tasks.append(asyncio.create_task(renew_lease(), name="lease"))
        watched = tuple(tasks)

        async def supervise() -> None:
            done, _ = await asyncio.wait(watched, return_when=asyncio.FIRST_COMPLETED)
            if not app.stopping.is_set():
                for task in done:
                    error = None if task.cancelled() else task.exception()
                    LOG.critical(
                        "background service stopped task=%s error=%r",
                        task.get_name(),
                        error,
                    )
                app.stopping.set()
                await dispatcher.stop_polling()

        tasks.append(asyncio.create_task(supervise(), name="supervisor"))
        LOG.info("Marie started; local capabilities ready=%s", sorted(guards.models))
        await dispatcher.start_polling(
            bot,
            allowed_updates=dispatcher.resolve_used_update_types(),
            close_bot_session=False,
            tasks_concurrency_limit=50,
        )
    finally:
        app.stopping.set()
        if app.inflight:
            done, pending = await asyncio.wait(app.inflight, timeout=30)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        if worker:
            worker.handle_sig_wait_for_completion(signal.SIGTERM)
        for task in tasks:
            if task.get_name() == "arq":
                with contextlib.suppress(asyncio.TimeoutError, asyncio.CancelledError):
                    await asyncio.wait_for(asyncio.shield(task), 35)
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if runner:
            await runner.cleanup()
        if acquired:
            with contextlib.suppress(Exception):
                await redis.eval(
                    "if redis.call('GET',KEYS[1])==ARGV[1] then return redis.call('DEL',KEYS[1]) else return 0 end",
                    1,
                    PREFIX + "runtime-lease",
                    lease,
                )
        guards.pool.shutdown(wait=True, cancel_futures=True)
        await db.client.close()
        await redis.aclose()
        await bot.session.close()


class DecodedRedis:
    """Decode only application GETs, leaving ARQ's binary job protocol intact."""

    def __init__(self, raw: Any):
        self.raw = raw

    async def get(self, key: str) -> str | None:
        value = await self.raw.get(key)
        return value.decode() if isinstance(value, bytes) else value

    async def getdel(self, key: str) -> str | None:
        value = await self.raw.getdel(key)
        return value.decode() if isinstance(value, bytes) else value

    async def enqueue_job(self, function: str, *args: Any, **kwargs: Any) -> Any:
        kwargs.setdefault("_queue_name", PREFIX + "jobs")
        return await self.raw.enqueue_job(function, *args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.raw, name)


def self_test() -> None:
    """Offline contracts and mocked Telegram/DB operations, not live integration."""
    import unittest
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, MagicMock

    class CoreTests(unittest.TestCase):
        def test_duration_valid(self):
            for text, expected in [
                ("60s", 60),
                ("1m", 60),
                ("2h", 7200),
                ("7d", 604800),
                ("2w", 1209600),
            ]:
                with self.subTest(text=text):
                    self.assertEqual(duration(text), expected)

        def test_duration_rejects_dangerous_values(self):
            for text in [
                "0s",
                "1s",
                "30s",
                "-1h",
                "1 hour",
                "400d",
                "1h;drop",
                "",
                "99999999w",
            ]:
                with self.subTest(text=text), self.assertRaises(ValueError):
                    duration(text)

        def test_confusables(self):
            self.assertEqual(normalize("ＳРАМ\u200b"), "spam")

        def test_matching(self):
            self.assertTrue(matches("Hello World", "world"))
            self.assertFalse(matches("Hello World", "world", "exact"))
            self.assertTrue(matches("hello there", "hello", "prefix"))
            self.assertTrue(matches("buy coins now", "buy * now", "wildcard"))

        def test_buttons_styled(self):
            for style in ["primary", "success", "danger"]:
                self.assertEqual(button("label", "action", style).style, style)

        def test_disabled_has_no_action(self):
            b = button("needs permissions")
            self.assertIsNotNone(b.disabled)
            self.assertIsNone(b.callback_data)

        def test_callback_limit(self):
            with self.assertRaises(ValueError):
                button("test", "x" * 65)

        def test_three_buttons_max(self):
            with self.assertRaises(ValueError):
                keyboard([button("x") for _ in range(4)])

        def test_rich_quote_contract(self):
            for count in [1, 4, 7, 40]:
                with self.subTest(count=count):
                    screen = Screen("help", lines=["├ /ban"] * count)
                    blocks = screen.rich().blocks
                    self.assertEqual(
                        blocks[-1].type,
                        "expandable_blockquote" if count > 6 else "blockquote",
                    )

        def test_fallback_escapes_html(self):
            value = Screen("<title>", "<script>", ["<b>unsafe</b>"]).fallback()
            self.assertNotIn("<script>", value)
            self.assertIn("&lt;script&gt;", value)

        def test_fallback_never_truncates_tags(self):
            value = Screen("x", "<" * 5000, ["&" * 5000]).fallback()
            self.assertTrue(value.endswith("</blockquote>"))

        def test_rich_native_table(self):
            screen = Screen("stats", table=(["key", "value"], [["users", "5"]]))
            table = screen.rich().blocks[-1].blocks[0]
            self.assertEqual(table.type, "table")
            self.assertTrue(table.cells[0][0].is_header)
            self.assertEqual(table.cells[1][1].text, "5")

        def test_setting_ranges(self):
            for key, bounds in INT_SETTINGS.items():
                lo, hi = bounds
                self.assertEqual(validate_setting(key, lo), lo)
                with self.assertRaises(ValueError):
                    validate_setting(key, hi + 1)
                with self.assertRaises(ValueError):
                    validate_setting(key, True)

        def test_setting_boolean(self):
            for key in BOOL_SETTINGS:
                self.assertTrue(validate_setting(key, True))
                with self.assertRaises(ValueError):
                    validate_setting(key, "on")

        def test_setting_choices(self):
            for key, values in CHOICE_SETTINGS.items():
                for value in values:
                    self.assertEqual(validate_setting(key, value), value)
                with self.assertRaises(ValueError):
                    validate_setting(key, "erase")

        def test_threshold_nan_rejected(self):
            for value in [float("nan"), float("inf"), -0.1, 1.1, True]:
                with self.subTest(value=value), self.assertRaises(ValueError):
                    validate_setting("spam_threshold", value)

        def test_unknown_setting(self):
            with self.assertRaises(ValueError):
                validate_setting("group_id", -100123)

        def test_guards_default_off(self):
            self.assertFalse(
                any(DEFAULTS[k + "_enabled"] for k in ["spam", "toxic", "pii"])
            )

        def test_import_rejects_foreign_scope(self):
            with self.assertRaises(ValueError):
                Marie.validate_import(
                    {"notes": [{"name": "x", "content": "x", "group_id": -999}]}
                )

        def test_import_rejects_unknown_top_level(self):
            with self.assertRaises(ValueError):
                Marie.validate_import({"owner_id": 1})

        def test_import_rejects_unknown_settings(self):
            with self.assertRaises(ValueError):
                Marie.validate_import({"settings": {"owner_id": 1}})

        def test_import_rejects_implicit_ai_enable(self):
            with self.assertRaises(ValueError):
                Marie.validate_import({"settings": {"spam_enabled": True}})

        def test_import_rejects_invalid_action(self):
            with self.assertRaises(ValueError):
                Marie.validate_import({"filters": [{"keyword": "x", "action": "ban"}]})

        def test_import_rejects_unsafe_buttons(self):
            with self.assertRaises(ValueError):
                Marie.validate_import(
                    {
                        "rules": [
                            {
                                "rules_text": "x",
                                "buttons_json": json.dumps(
                                    [{"text": "x", "url": "javascript:alert(1)"}]
                                ),
                            }
                        ]
                    }
                )

        def test_import_rejects_missing_fields(self):
            with self.assertRaises(ValueError):
                Marie.validate_import({"notes": [{"media_id": "foo"}]})

        def test_import_valid(self):
            data = Marie.validate_import(
                {
                    "version": 1,
                    "settings": {"flood": 8},
                    "notes": [{"name": "hi", "content": "hello"}],
                }
            )
            self.assertEqual(data.settings["flood"], 8)

        def test_telegram_post_links(self):
            self.assertEqual(
                telegram_link("https://t.me/example/123"), ("@example", 123)
            )
            self.assertEqual(
                telegram_link("https://t.me/c/123456/44/55"), (-100123456, 55)
            )
            for link in [
                "http://t.me/example/1",
                "https://t.me.evil/example/1",
                "https://t.me/+invite",
                "https://t.me/example",
                "https://evil.test/a/1",
            ]:
                with self.subTest(link=link), self.assertRaises(ValueError):
                    telegram_link(link)

        def test_url_validation(self):
            for url in [
                "javascript:alert(1)",
                "http://example.com",
                "https://user:pass@example.com",
                "https://bad host.com",
                None,
            ]:
                self.assertFalse(valid_url(url))
            self.assertTrue(valid_url("https://t.me/example"))

        def test_template_is_plain_text(self):
            user = T.User(id=1, is_bot=False, first_name="<b>Eve</b>")
            chat = T.Chat(id=-1, type="supergroup", title="chat")
            self.assertEqual(render_template("hi {first}", user, chat), "hi <b>Eve</b>")
            self.assertIn(
                "&lt;b&gt;",
                Screen(
                    "welcome", lines=[render_template("{first}", user, chat)]
                ).fallback(),
            )

        def test_lock_detection(self):
            m = T.Message(
                message_id=1,
                date=datetime.now(UTC),
                chat=T.Chat(id=-1, type="supergroup"),
                text="/go https://example.com",
                entities=[T.MessageEntity(type="url", offset=4, length=19)],
            )
            self.assertTrue({"text", "command", "url"} <= message_kinds(m))

        def test_inventory_disjoint(self):
            self.assertFalse(COMMANDS & set(DEFERRED))
            self.assertGreater(len(COMMANDS), 150)

        def test_help_styled_and_branch_tree(self):
            app = Marie(None, None, None, 1, None)
            for section in SECTIONS:
                if section == "owner":
                    continue
                s = app.help_screen(section)
                self.assertTrue(s.lines[-1].startswith("└"))
                self.assertEqual(s.rich().blocks[-1].type, "expandable_blockquote")
                self.assertTrue(all(b.style for row in s.rows for b in row))

        def test_owner_scope(self):
            app = Marie(None, None, None, 1, None)
            for uid, chat_type in [(2, "private"), (1, "supergroup")]:
                m = T.Message(
                    message_id=1,
                    date=datetime.now(UTC),
                    chat=T.Chat(
                        id=uid if chat_type == "private" else -1, type=chat_type
                    ),
                    from_user=T.User(id=uid, is_bot=False, first_name="u"),
                )
                with self.assertRaises(ValueError):
                    app.owner_only(m)

        def test_no_local_database(self):
            for url in ["file:foo.db", "sqlite:///foo", ":memory:", "http://unsafe"]:
                with self.assertRaises(ValueError):
                    DB(url, "x")

    class AsyncTests(unittest.IsolatedAsyncioTestCase):
        async def asyncSetUp(self):
            self.bot = SimpleNamespace(
                edit_message_text=AsyncMock(),
                send_rich_message=AsyncMock(),
                send_message=AsyncMock(),
                get_chat_member=AsyncMock(),
                restrict_chat_member=AsyncMock(),
                get_chat=AsyncMock(),
                ban_chat_member=AsyncMock(),
                unban_chat_member=AsyncMock(),
                delete_message=AsyncMock(),
            )
            self.db = SimpleNamespace(
                execute=AsyncMock(
                    return_value=SimpleNamespace(last_insert_rowid=7, rows_affected=1)
                ),
                one=AsyncMock(return_value=None),
                query=AsyncMock(return_value=[]),
                settings=AsyncMock(return_value=DEFAULTS.copy()),
                audit=AsyncMock(),
                set_setting=AsyncMock(),
            )
            self.redis = SimpleNamespace(
                set=AsyncMock(return_value=True),
                get=AsyncMock(return_value=None),
                eval=AsyncMock(return_value=1),
                delete=AsyncMock(),
                enqueue_job=AsyncMock(),
            )
            self.app = Marie(
                self.bot, self.db, self.redis, 1, SimpleNamespace(models={})
            )
            self.app.me = T.User(
                id=999, is_bot=True, first_name="Marie", username="MarieTestBot"
            )
            self.app.audit = AsyncMock()
            self.bot.get_chat_member.return_value = SimpleNamespace(status="creator")
            self.m = T.Message(
                message_id=5,
                date=datetime.now(UTC),
                chat=T.Chat(id=-1001, type="supergroup", title="test"),
                from_user=T.User(id=2, is_bot=False, first_name="Admin"),
                text="/settings",
            )

        async def test_private_feedback_ephemeral(self):
            await self.app.show(-1001, 2, Screen("x", "test"))
            kw = self.bot.send_rich_message.call_args.kwargs
            self.assertEqual(kw["ephemeral_message_parameters"].receiver_user_id, 2)

        async def test_private_fallback_never_leaks_to_group(self):
            from aiogram.methods import SendRichMessage

            self.bot.send_rich_message.side_effect = TelegramBadRequest(
                SendRichMessage(chat_id=-1001, rich_message=Screen("x").rich()),
                "unsupported",
            )
            await self.app.show(-1001, 2, Screen("x", "private"))
            self.assertEqual(self.bot.send_message.call_args.args[0], 2)

        async def test_public_content_has_no_ephemeral(self):
            await self.app.show(-1001, 2, Screen("x", "rules"), private=False)
            self.assertIsNone(
                self.bot.send_rich_message.call_args.kwargs[
                    "ephemeral_message_parameters"
                ]
            )

        async def test_admin_rights(self):
            self.bot.get_chat_member.return_value = SimpleNamespace(status="member")
            with self.assertRaises(ValueError):
                await self.app.require_admin(-1001, 2)
            self.bot.get_chat_member.return_value = SimpleNamespace(
                status="administrator", can_restrict_members=False
            )
            with self.assertRaises(ValueError):
                await self.app.require_admin(-1001, 2, "can_restrict_members")

        async def test_owner_protected_target(self):
            with self.assertRaises(ValueError):
                await self.app.safe_target(-1001, 1)

        async def test_admin_protected_target(self):
            self.bot.get_chat_member.return_value = SimpleNamespace(
                status="administrator"
            )
            with self.assertRaises(ValueError):
                await self.app.safe_target(-1001, 22)

        async def test_confirmation_is_expiring_and_bound(self):
            await self.app.pending(-1001, 2, -1001, "moderate", {"target": 3}, "ban 3?")
            args = self.redis.set.call_args
            data = json.loads(args.args[1])
            self.assertEqual(
                (data["actor"], data["chat"], data["gid"]), (2, -1001, -1001)
            )
            self.assertEqual(args.kwargs["ex"], 30)
            self.assertTrue(args.kwargs["nx"])
            self.assertLess(data["expires"] - now(), 31)

        async def test_foreign_confirmation_denied(self):
            self.redis.get.return_value = json.dumps(
                {"actor": 123, "chat": -1001, "gid": -1001, "expires": now() + 20}
            )
            call = T.CallbackQuery(
                id="q",
                from_user=self.m.from_user,
                chat_instance="c",
                message=self.m,
                data="confirm:abc",
            )
            # callback.answer needs a bound bot; skip its best-effort acknowledgement.
            call = MagicMock(wraps=call)
            call.message = self.m
            call.from_user = self.m.from_user
            call.data = "confirm:abc"
            call.answer = AsyncMock()
            with self.assertRaises(ValueError):
                await self.app.callback(call)
            self.redis.eval.assert_not_awaited()

        async def test_guard_enable_requires_models(self):
            with self.assertRaises(ValueError):
                await self.app.ai_command(
                    self.m, -1001, "aispam", ["on"], DEFAULTS.copy()
                )
            self.db.set_setting.assert_not_awaited()

        async def test_no_success_on_failed_telegram_action(self):
            self.app.require_admin = AsyncMock()
            self.app.safe_target = AsyncMock()
            from aiogram.methods import BanChatMember

            self.bot.ban_chat_member.side_effect = TelegramBadRequest(
                BanChatMember(chat_id=-1001, user_id=3), "no rights"
            )
            with self.assertRaises(TelegramBadRequest):
                await self.app.action(-1001, 2, 3, "ban", "test")
            statements = [call.args[0] for call in self.db.execute.call_args_list]
            self.assertTrue(any("status='failed'" in s for s in statements))
            self.assertFalse(any("status='applied'" in s for s in statements))
            self.app.audit.assert_not_awaited()

        async def test_unmute_restores_group_defaults(self):
            self.app.require_admin = AsyncMock()
            self.app.safe_target = AsyncMock()
            perms = T.ChatPermissions(can_send_messages=True, can_send_photos=False)
            self.bot.get_chat.return_value = SimpleNamespace(permissions=perms)
            await self.app.action(-1001, 2, 3, "unmute", "test")
            restored = self.bot.restrict_chat_member.call_args.args[2]
            self.assertFalse(restored.can_send_photos)

        async def test_native_list_pagination(self):
            rows = [{"n": i} for i in range(25)]
            screen = await self.app.list_screen("list", rows, ["n"], 1, "page:token")
            self.assertEqual(len(screen.table[1]), 10)
            self.assertEqual(screen.table[1][0], ["10"])
            self.assertEqual(screen.rows[0][0].callback_data, "page:token:0")
            self.assertEqual(screen.rows[0][2].callback_data, "page:token:2")

        async def test_last_page_is_bounded(self):
            screen = await self.app.list_screen(
                "list", [{"n": 1}], ["n"], 999, "page:t"
            )
            self.assertEqual(len(screen.table[1]), 1)
            self.assertIsNotNone(screen.rows[0][2].disabled)

        async def test_settings_changes_prepared(self):
            await self.app.command(self.m, "warnlimit", "5")
            self.db.set_setting.assert_awaited_once_with(-1001, "warnlimit", 5)

        async def test_database_transaction_context(self):
            db = object.__new__(DB)
            tx = MagicMock()
            tx.execute = AsyncMock()
            tx.commit = AsyncMock()
            tx.__enter__.return_value = tx
            db.client = SimpleNamespace(transaction=MagicMock(return_value=tx))
            await db.batch([("SELECT ?", [1])])
            tx.execute.assert_awaited_once_with("SELECT ?", [1])
            tx.commit.assert_awaited_once()
            tx.__exit__.assert_called_once()

        async def test_database_failure_does_not_commit(self):
            db = object.__new__(DB)
            tx = MagicMock()
            tx.execute = AsyncMock(side_effect=RuntimeError("broken"))
            tx.commit = AsyncMock()
            tx.__enter__.return_value = tx
            db.client = SimpleNamespace(transaction=MagicMock(return_value=tx))
            with self.assertRaises(RuntimeError):
                await db.batch([("SELECT ?", [1])])
            tx.commit.assert_not_awaited()

        async def test_decode_keeps_arq_binary_protocol(self):
            raw = SimpleNamespace(
                get=AsyncMock(return_value=b"value"),
                getdel=AsyncMock(return_value=b"taken"),
                enqueue_job=AsyncMock(),
            )
            wrapped = DecodedRedis(raw)
            self.assertEqual(await wrapped.get("key"), "value")
            self.assertEqual(await wrapped.getdel("key"), "taken")
            await wrapped.enqueue_job("job")
            raw.enqueue_job.assert_awaited_once_with("job", _queue_name=PREFIX + "jobs")

    class PhaseOneTests(unittest.IsolatedAsyncioTestCase):
        asyncSetUp = AsyncTests.asyncSetUp

        async def test_new_commands_deny_non_admin(self):
            self.app.require_admin = AsyncMock(
                side_effect=ValueError("⌁ need admin rights ᯓ")
            )
            for cmd in PHASE1_COMMANDS:
                with self.subTest(cmd=cmd), self.assertRaises(ValueError):
                    await self.app.phase1_command(self.m, cmd, "", [])

        async def test_ladder_happy_and_invalid(self):
            self.app.flow = AsyncMock()
            self.app.require_admin = AsyncMock()
            await self.app.phase1_command(self.m, "setwarnladder", "", [])
            self.app.flow.assert_awaited_once()
            with self.assertRaises(ValueError):
                await self.app.phase1_command(self.m, "setwarnladder", "x", ["x"])
            valid = [{"warns": n, "action": "mute", "seconds": 60} for n in [1, 3, 5]]
            self.assertEqual(validate_ladder(valid), valid)
            with self.assertRaises(ValueError):
                validate_ladder(valid[::-1])

        async def test_proposal_happy_and_invalid(self):
            self.app.require_admin = AsyncMock()
            self.app.safe_target = AsyncMock()
            self.bot.get_chat_administrators = AsyncMock(return_value=[])
            await self.app.phase1_command(
                self.m, "propose", "kick 33 spam", ["kick", "33", "spam"]
            )
            self.assertIn("admin_proposals", self.db.execute.call_args.args[0])
            with self.assertRaises(ValueError):
                await self.app.phase1_command(self.m, "propose", "erase", ["erase"])

        async def test_template_exact_payload(self):
            self.app.require_admin = AsyncMock()
            self.app.safe_target = AsyncMock()
            self.app.pending = AsyncMock()
            self.db.one.return_value = {
                "action": "mute",
                "reason": "spam {user} by {admin}",
                "duration": 259200,
            }
            await self.app.phase1_command(
                self.m, "template", "use spam 33", ["use", "spam", "33"]
            )
            payload = self.app.pending.call_args.args[4]
            self.assertEqual(payload["reason"], "spam 33 by 2")
            self.assertEqual(payload["seconds"], 259200)
            with self.assertRaises(ValueError):
                await self.app.phase1_command(
                    self.m,
                    "template",
                    "use spam 33 override",
                    ["use", "spam", "33", "override"],
                )

        async def test_templates_list_happy_and_invalid(self):
            self.app.require_admin = AsyncMock()
            self.app.tree_snapshot = AsyncMock()
            await self.app.phase1_command(self.m, "templates", "", [])
            self.app.tree_snapshot.assert_awaited_once()
            with self.assertRaises(ValueError):
                await self.app.phase1_command(self.m, "templates", "x", ["x"])

        async def test_bio_happy_and_invalid(self):
            self.app.require_admin = AsyncMock()
            self.app.pending = AsyncMock()
            await self.app.phase1_command(self.m, "bioblacklist", "crypto", ["crypto"])
            self.assertIn("bioblacklist", self.db.execute.call_args.args[0])
            await self.app.phase1_command(
                self.m, "rmbioblacklist", "crypto", ["crypto"]
            )
            self.app.pending.assert_awaited_once()
            for cmd in ["bioblacklist", "rmbioblacklist"]:
                with self.assertRaises(ValueError):
                    await self.app.phase1_command(self.m, cmd, "", [])

        async def test_import_parser_and_missing_attachment(self):
            good, skipped = parse_fed_import(
                b"33 spam\ninvalid\n-2 bad\n33 duplicate\n44 reason"
            )
            self.assertEqual(len(good), 2)
            self.assertEqual(skipped, 3)
            for content in [b"", b"[bad", b"\xff"]:
                with self.assertRaises(ValueError):
                    parse_fed_import(content)
            self.app.require_admin = AsyncMock()
            self.app.fed_authorize = AsyncMock()
            self.db.one.return_value = {"fed_id": "fed"}
            with self.assertRaises(ValueError):
                await self.app.phase1_command(self.m, "importfbans", "", [])

        async def test_pin_and_purge_validation(self):
            self.app.require_admin = AsyncMock()
            self.app.pending = AsyncMock()
            for cmd in ["permapin", "unpermapin", "purgeto", "delreactions"]:
                with self.subTest(cmd=cmd), self.assertRaises(ValueError):
                    await self.app.phase1_command(self.m, cmd, "bad", ["bad"])
            reply = self.m.model_copy(update={"message_id": 1})
            m = self.m.model_copy(update={"reply_to_message": reply})
            for cmd, text, args in [
                ("permapin", "", []),
                ("unpermapin", "", []),
                ("purgeto", "4", ["4"]),
                ("delreactions", "", []),
            ]:
                await self.app.phase1_command(m, cmd, text, args)
            self.assertEqual(self.app.pending.await_count, 4)

    class PhaseTwoTests(unittest.IsolatedAsyncioTestCase):
        asyncSetUp = AsyncTests.asyncSetUp

        def owner_message(self, text=""):
            return self.m.model_copy(
                update={
                    "chat": T.Chat(id=1, type="private"),
                    "from_user": T.User(id=1, is_bot=False, first_name="Owner"),
                    "text": text,
                }
            )

        def call(self, data, owner=True):
            m = self.owner_message() if owner else self.m
            c = MagicMock()
            c.message = m
            c.from_user = m.from_user
            c.data = data
            c.answer = AsyncMock()
            return c

        async def test_guided_broadcast_target_happy(self):
            self.app.flow = AsyncMock()
            await self.app.panel_callback(self.call("v2:bc:group"))
            self.assertEqual(
                self.app.flow.call_args.args[1]["kind"], "broadcast_target"
            )

        async def test_owner_actions_deny_foreign_actor(self):
            for data in [
                "v2:bc:groups",
                "v2:csv:audit",
                "v2:backup",
                "v2:global:ban:33",
                "v2:group:-1001",
                "v2:default:warnlimit",
                "v2:appeal:approve:bans:7",
            ]:
                with self.subTest(data=data), self.assertRaises(ValueError):
                    await self.app.panel_callback(self.call(data, False))

        async def test_broadcast_specific_target_validation(self):
            self.app.flow = AsyncMock()
            await self.app.panel_flow_input(
                self.owner_message("-1001"),
                {"kind": "broadcast_target", "target_type": "group"},
            )
            self.assertEqual(self.app.flow.call_args.args[1]["target"], "-1001")
            with self.assertRaises(ValueError):
                await self.app.panel_flow_input(
                    self.owner_message("33"),
                    {"kind": "broadcast_target", "target_type": "group"},
                )

        async def test_broadcast_content_happy_and_invalid(self):
            self.app.broadcast_options = AsyncMock()
            await self.app.panel_flow_input(
                self.owner_message("hello"),
                {"kind": "broadcast_content", "target": "groups"},
            )
            state = self.app.broadcast_options.call_args.args[1]
            self.assertEqual(state["message"], 5)
            self.assertFalse(state["pin"])
            with self.assertRaises(ValueError):
                await self.app.panel_flow_input(
                    self.owner_message(""),
                    {"kind": "broadcast_content", "target": "groups"},
                )

        async def test_appeal_is_bound_to_requester(self):
            call = self.call("v2:appeal:request:bans:7")
            await self.app.panel_callback(call)
            args = self.db.execute.call_args.args
            self.assertIn("user_id=?", args[0])
            self.assertEqual(args[1][1], 1)
            with self.assertRaises(ValueError):
                appeal_column("users")

        async def test_csv_memory_export_and_owner_permission(self):
            self.bot.send_document = AsyncMock()
            self.db.query.return_value = [{"reason": "=1+1", "user_id": 33}]
            await self.app.owner_export(1, "bans")
            data = self.bot.send_document.call_args.args[1]
            self.assertIn(b"'=1+1", data.data)
            self.assertTrue(data.filename.endswith(".csv"))
            with self.assertRaises(ValueError):
                await self.app.owner_export(2, "bans")
            with self.assertRaises(ValueError):
                await self.app.owner_export(1, "unknown")

        async def test_global_target_protection(self):
            self.app.pending = AsyncMock()
            await self.app.panel_callback(self.call("v2:global:ban:33"))
            self.assertEqual(self.app.pending.call_args.args[4]["target"], 33)
            with self.assertRaises(ValueError):
                await self.app.panel_callback(self.call("v2:global:ban:1"))

        async def test_warn_defaults_validation(self):
            self.db.bot_setting = AsyncMock(return_value={})
            self.db.set_bot_setting = AsyncMock()
            await self.app.panel_flow_input(
                self.owner_message("5"), {"kind": "warn_default", "key": "warnlimit"}
            )
            self.db.set_bot_setting.assert_awaited_once_with(
                "warn_defaults", {"warnlimit": 5}
            )
            with self.assertRaises(ValueError):
                await self.app.panel_flow_input(
                    self.owner_message("0"),
                    {"kind": "warn_default", "key": "warnlimit"},
                )

        async def test_schema_migration_idempotence(self):
            db = object.__new__(DB)
            db.query = AsyncMock(return_value=[{"name": "active"}])
            db.execute = AsyncMock()
            await db.ensure_columns(
                {"groups": {"active": "INTEGER DEFAULT 1", "new_col": "TEXT"}}
            )
            self.assertEqual(db.execute.await_count, 1)
            self.assertIn("ADD COLUMN new_col", db.execute.call_args.args[0])

    class PhaseThreeTests(unittest.IsolatedAsyncioTestCase):
        asyncSetUp = AsyncTests.asyncSetUp

        async def test_new_admin_commands_deny_members(self):
            self.app.require_admin = AsyncMock(
                side_effect=ValueError("⌁ need admin rights ᯓ")
            )
            for cmd in PHASE3_COMMANDS - {"setbirthday"}:
                with self.subTest(cmd=cmd), self.assertRaises(ValueError):
                    await self.app.phase3_command(self.m, cmd, "", [])

        async def test_drip_happy_invalid_and_position_remove(self):
            self.app.require_admin = AsyncMock()
            self.app.tree_snapshot = AsyncMock()
            self.app.pending = AsyncMock()
            await self.app.phase3_command(
                self.m, "setdrip", "24h hello", ["24h", "hello"]
            )
            self.assertIn("max(position)", self.db.execute.call_args.args[0])
            for cmd, text, args in [
                ("setdrip", "bad", ["bad"]),
                ("drip", "bad", ["bad"]),
                ("cleardrip", "-1", ["-1"]),
            ]:
                with self.assertRaises(ValueError):
                    await self.app.phase3_command(self.m, cmd, text, args)
            self.db.query.return_value = [{"seq_id": 9}]
            await self.app.phase3_command(self.m, "cleardrip", "1", ["1"])
            self.assertEqual(self.app.pending.call_args.args[4]["seq_id"], 9)

        async def test_birthday_dates_and_scope(self):
            self.assertEqual(birthday_date("02-29"), (2, 29))
            for value in ["02-30", "13-01", "1-1", "2026-01-01"]:
                with self.assertRaises(ValueError):
                    birthday_date(value)
            with self.assertRaises(ValueError):
                await self.app.phase3_command(self.m, "setbirthday", "03-21", ["03-21"])
            m = self.m.model_copy(update={"chat": T.Chat(id=2, type="private")})
            self.db.query.return_value = [{"group_id": -1001, "title": "group"}]
            self.app.save_birthday = AsyncMock()
            await self.app.phase3_command(m, "setbirthday", "03-21", ["03-21"])
            self.app.save_birthday.assert_awaited_once_with(2, -1001, 3, 21)

        async def test_birthday_message_and_list_validation(self):
            self.app.require_admin = AsyncMock()
            self.app.show_snapshot = AsyncMock()
            await self.app.phase3_command(
                self.m, "setbirthdaymsg", "happy {user}", ["happy", "{user}"]
            )
            with self.assertRaises(ValueError):
                await self.app.phase3_command(
                    self.m, "setbirthdaymsg", "missing", ["missing"]
                )
            self.db.query.return_value = []
            await self.app.phase3_command(self.m, "birthday", "list", ["list"])
            self.app.show_snapshot.assert_awaited_once()
            with self.assertRaises(ValueError):
                await self.app.phase3_command(
                    self.m, "birthday", "invalid", ["invalid"]
                )

        async def test_shift_notes_tree_and_invalid(self):
            self.app.require_admin = AsyncMock()
            self.app.tree_snapshot = AsyncMock()
            self.db.query.return_value = [
                {"admin_id": 2, "created_at": now(), "content": "all clear"}
            ]
            await self.app.phase3_command(self.m, "shiftnotes", "", [])
            self.assertIn("all clear", self.app.tree_snapshot.call_args.args[2][0])
            with self.assertRaises(ValueError):
                await self.app.phase3_command(self.m, "shiftnotes", "bad", ["bad"])

        async def test_cooldown_warning_and_safe_mute(self):
            self.app.delete = AsyncMock(return_value=True)
            self.app.action = AsyncMock()
            self.redis.eval.return_value = 3
            self.assertTrue(await self.app.cooldown_guard(self.m, 30))
            self.bot.send_rich_message.assert_awaited_once()
            self.redis.eval.return_value = 5
            await self.app.cooldown_guard(self.m, 1)
            self.assertEqual(self.app.action.call_args.args[5], 60)
            self.assertFalse(await self.app.cooldown_guard(self.m, 0))

        async def test_adminperf_happy_and_invalid(self):
            self.app.require_admin = AsyncMock()
            await self.app.phase3_command(self.m, "adminperf", "", [])
            screen = self.bot.send_rich_message.call_args.kwargs["rich_message"]
            self.assertTrue(screen.blocks)
            with self.assertRaises(ValueError):
                await self.app.phase3_command(self.m, "adminperf", "bad", ["bad"])

        async def test_duty_overnight(self):
            self.assertTrue(duty_active("22:00", "06:00", "02:00"))
            self.assertFalse(duty_active("22:00", "06:00", "12:00"))
            self.assertTrue(duty_active("09:00", "17:00", "12:00"))

    class PhaseFourTests(unittest.IsolatedAsyncioTestCase):
        asyncSetUp = AsyncTests.asyncSetUp

        async def test_long_summary_quoted(self):
            s = Screen("test", "one\ntwo\nthree\nfour")
            self.assertFalse(s.summary)
            self.assertEqual(s.rich().blocks[-1].type, "blockquote")

        async def test_tables_are_inside_quotes(self):
            s = Screen("data", table=(["a"], [["1"], ["2"], ["3"], ["4"]]))
            self.assertEqual(s.rich().blocks[-1].type, "blockquote")
            self.assertEqual(s.rich().blocks[-1].blocks[0].type, "table")

        async def test_disabled_buttons_explain_why(self):
            for text in ["←", "→", "support", "preview", "1 / 3"]:
                b = button(text)
                self.assertIsNotNone(b.disabled)
                self.assertTrue(
                    any(
                        word in b.text.lower()
                        for word in ["first", "last", "not configured", "current"]
                    )
                )
                self.assertTrue(b.style)

        async def test_help_indexes_expandable(self):
            screen = await self.app.section_menu(-1001)
            self.assertTrue(screen.expandable)
            for cmd in ["helpadmin", "helpuser"]:
                await self.app.command(self.m, cmd, "")
                rich = self.bot.send_rich_message.call_args.kwargs["rich_message"]
                quote = rich.blocks[-1]
                self.assertEqual(quote.type, "expandable_blockquote")
                self.assertIn("/", quote.text)

        async def test_settings_have_scoped_controls(self):
            s = await self.app.setting_screen(-1001)
            self.assertIsNotNone(s.table)
            actions = [
                b.callback_data for row in s.rows for b in row if b.callback_data
            ]
            self.assertIn("v2:setting:-1001:cooldown", actions)

        async def test_countdown_scheduled_for_real_message(self):
            message = self.m.model_copy(update={"ephemeral_message_id": 7})
            self.bot.send_rich_message.return_value = message
            await self.app.pending(-1001, 2, -1001, "moderate", {}, "ban?")
            self.redis.enqueue_job.assert_awaited_once()
            self.assertEqual(
                self.redis.enqueue_job.call_args.args[0], "confirmation_tick"
            )
            self.assertEqual(self.redis.enqueue_job.call_args.kwargs["_defer_by"], 5)

        async def test_friendly_errors_hide_parser_details(self):
            for error in [
                ValueError("invalid literal for int()"),
                ValueError("Error: bad"),
                KeyError("database"),
            ]:
                value = friendly_error(error)
                self.assertNotIn("Error:", value)
                self.assertIn("◡", value)

    class PhaseFiveTests(unittest.IsolatedAsyncioTestCase):
        async def test_required_new_table_contracts(self):
            for table, columns in {
                "bioblacklist": {"entry_id", "group_id", "pattern", "created_at"},
                "birthdays": {"user_id", "group_id", "month", "day"},
                "birthday_msg": {"group_id", "template"},
            }.items():
                self.assertTrue(columns <= set(schema_columns(table)))

        async def test_migrations_skip_existing_fields(self):
            db = object.__new__(DB)
            db.query = AsyncMock(
                return_value=[
                    {"name": "appeal_requested"},
                    {"name": "active"},
                    {"name": "last_seen"},
                ]
            )
            db.execute = AsyncMock()
            await db.ensure_columns(
                {
                    "bans": {"appeal_requested": "INTEGER DEFAULT 0"},
                    "mutes": {"appeal_requested": "INTEGER DEFAULT 0"},
                    "groups": {"active": "INTEGER DEFAULT 1"},
                    "users": {"last_seen": "INTEGER"},
                }
            )
            db.execute.assert_not_awaited()

        async def test_migrations_add_only_missing(self):
            db = object.__new__(DB)
            db.query = AsyncMock(return_value=[])
            db.execute = AsyncMock()
            await db.ensure_columns(
                {
                    "bans": {"appeal_requested": "INTEGER DEFAULT 0"},
                    "users": {"last_seen": "INTEGER"},
                }
            )
            statements = [c.args[0] for c in db.execute.call_args_list]
            self.assertEqual(
                statements,
                [
                    "ALTER TABLE bans ADD COLUMN appeal_requested INTEGER DEFAULT 0",
                    "ALTER TABLE users ADD COLUMN last_seen INTEGER",
                ],
            )
            self.assertFalse(any("IF NOT EXISTS" in s for s in statements))

    class PhaseSixTests(unittest.IsolatedAsyncioTestCase):
        asyncSetUp = AsyncTests.asyncSetUp

        async def test_every_phase_one_command_has_happy_dispatch(self):
            self.app.require_admin = AsyncMock()
            self.app.safe_target = AsyncMock()
            self.app.pending = AsyncMock()
            self.app.flow = AsyncMock()
            self.app.tree_snapshot = AsyncMock()
            self.app.fed_authorize = AsyncMock()
            self.bot.get_chat_administrators = AsyncMock(return_value=[])
            self.bot.download = AsyncMock(
                side_effect=lambda file, destination: destination.write(
                    b"33 spam\n44 scam"
                )
            )
            doc = T.Document(
                file_id="file",
                file_unique_id="unique",
                file_name="bans.txt",
                file_size=20,
            )
            reply = self.m.model_copy(update={"message_id": 1, "document": doc})
            message = self.m.model_copy(update={"reply_to_message": reply})
            cases = {
                "setwarnladder": "",
                "propose": "kick 33 spam",
                "template": "save spam mute 3d spam",
                "templates": "",
                "bioblacklist": "crypto",
                "rmbioblacklist": "crypto",
                "importfbans": "",
                "permapin": "",
                "unpermapin": "",
                "purgeto": "4",
                "delreactions": "",
            }
            for cmd, text in cases.items():
                self.db.one.return_value = {"fed_id": "fed"}
                with self.subTest(cmd=cmd):
                    await self.app.command(message, cmd, text)
            self.assertEqual(set(cases), PHASE1_COMMANDS)

        async def test_unsupported_operations_are_not_faked(self):
            for cmd in UNSUPPORTED:
                with self.subTest(cmd=cmd), self.assertRaises(ValueError):
                    await self.app.command(self.m, cmd, "100")
            self.bot.send_rich_message.assert_not_awaited()
            self.assertEqual(DEFERRED, {})

        async def test_proposal_senior_permission_rechecked_on_confirm(self):
            self.app.require_admin = AsyncMock(
                side_effect=ValueError("⌁ need senior rights ᯓ")
            )
            call = MagicMock()
            call.message = self.m
            call.from_user = self.m.from_user
            with self.assertRaises(ValueError):
                await self.app.phase_pending(
                    {
                        "op": "v2_proposal_approve",
                        "payload": {"id": 7},
                        "gid": -1001,
                        "actor": 2,
                    },
                    call,
                )
            self.db.execute.assert_not_awaited()

        async def test_import_permission_is_federation_specific(self):
            self.db.one.return_value = {"fed_id": "fed"}
            self.app.require_admin = AsyncMock()
            self.app.fed_authorize = AsyncMock(
                side_effect=ValueError("federation rights ᯓ")
            )
            with self.assertRaises(ValueError):
                await self.app.phase1_command(self.m, "importfbans", "", [])
            self.app.fed_authorize.assert_awaited_once_with("fed", 2)

        async def test_ladder_action_uses_saved_threshold_and_duration(self):
            self.app.require_admin = AsyncMock()
            self.app.safe_target = AsyncMock()
            self.db.settings.return_value = DEFAULTS | {
                "warn_ladder": [
                    {"warns": 1, "action": "mute", "seconds": 600},
                    {"warns": 3, "action": "kick", "seconds": 0},
                    {"warns": 5, "action": "mute", "seconds": 86400},
                ]
            }
            self.db.one.return_value = {"n": 1}
            await self.app.action(-1001, 2, 33, "warn", "test")
            self.bot.restrict_chat_member.assert_awaited_once()
            until = self.bot.restrict_chat_member.call_args.kwargs["until_date"]
            self.assertTrue(595 < (until - datetime.now(UTC)).total_seconds() <= 600)

        async def test_shadow_cache_warmup_and_no_message_db_lookup(self):
            self.redis.scan_iter = MagicMock(return_value=empty_async_iter())
            self.redis.sadd = AsyncMock()
            self.db.query.return_value = [{"group_id": -1001, "user_id": 33}]
            await warm_shadow_cache(self.app)
            self.redis.sadd.assert_awaited_once_with(
                PREFIX + "shadowmuted_users:-1001", "33"
            )
            self.redis.sismember = AsyncMock(return_value=True)
            self.app.delete = AsyncMock()
            self.db.one.reset_mock()
            await self.app.handle_message(self.m)
            self.app.delete.assert_awaited_once()
            self.db.one.assert_not_awaited()

        async def test_birthday_ownership_cannot_be_overridden(self):
            self.bot.get_chat_member.return_value = SimpleNamespace(status="left")
            with self.assertRaises(ValueError):
                await self.app.save_birthday(2, -1001, 3, 21)
            self.db.execute.assert_not_awaited()

        async def test_duty_report_routes_only_active_staff(self):
            self.db.query.return_value = [
                {
                    "admin_id": 22,
                    "start_time": "00:00",
                    "end_time": "23:59",
                    "is_active": 1,
                }
            ]
            self.bot.get_chat_administrators = AsyncMock(
                return_value=[
                    SimpleNamespace(
                        user=T.User(id=i, is_bot=False, first_name=f"staff{i}")
                    )
                    for i in [22, 23]
                ]
            )
            await self.app.notify_duty(-1001, 2, 33, 7)
            self.bot.send_message.assert_awaited_once()
            kw = self.bot.send_message.call_args.kwargs
            self.assertEqual(kw["ephemeral_message_parameters"].receiver_user_id, 22)

        async def test_duty_and_shift_note_permissions_and_invalid_input(self):
            self.app.require_admin = AsyncMock(side_effect=ValueError("⌁ no rights ᯓ"))
            for cmd in ["duty", "shiftnote"]:
                with self.assertRaises(ValueError):
                    await self.app.command(self.m, cmd, "on")
            self.app.require_admin = AsyncMock()
            with self.assertRaises(ValueError):
                await self.app.workflow_command(self.m, -1001, "shiftnote", [], "")
            with self.assertRaises(ValueError):
                await self.app.workflow_command(
                    self.m,
                    -1001,
                    "duty",
                    ["set", "33", "bad", "time"],
                    "set 33 bad time",
                )

        async def test_countdown_expiry_removes_active_buttons(self):
            state = json.dumps(
                {
                    "actor": 2,
                    "locator": {"chat": 2, "message": 5},
                    "expires": now() - 1,
                    "description": "ban?",
                }
            )
            self.redis.get = AsyncMock(
                side_effect=lambda key: state if key.endswith("countdown:t") else None
            )
            await confirmation_tick({"app": self.app}, "t")
            self.bot.edit_message_text.assert_awaited_once()
            markup = self.bot.edit_message_text.call_args.kwargs["reply_markup"]
            self.assertTrue(
                all(
                    b.callback_data is None
                    for row in markup.inline_keyboard
                    for b in row
                )
            )
            self.redis.enqueue_job.assert_not_awaited()

        async def test_delete_reactions_uses_observed_actor_api(self):
            self.app.require_admin = AsyncMock()
            self.bot.delete_message_reaction = AsyncMock()
            self.db.query.return_value = [{"actor_id": 33, "actor_type": "user"}]
            r = {
                "op": "v2_delreactions",
                "payload": {"message": 4},
                "gid": -1001,
                "actor": 2,
            }
            value = await self.app.phase_pending(r, MagicMock())
            self.assertIn("cleared", value)
            self.bot.delete_message_reaction.assert_awaited_once_with(
                -1001, 4, user_id=33, actor_chat_id=None
            )

        async def test_purgeto_batches_and_honest_count(self):
            self.app.require_admin = AsyncMock()
            self.bot.delete_messages = AsyncMock()
            value = await self.app.phase_pending(
                {
                    "op": "v2_purgeto",
                    "payload": {"first": 1, "last": 205},
                    "gid": -1001,
                    "actor": 2,
                },
                MagicMock(),
            )
            self.assertEqual(self.bot.delete_messages.await_count, 3)
            self.assertIn("205 ids", value)
            self.assertNotIn("205 deleted", value)

    class SQLContractTests(unittest.IsolatedAsyncioTestCase):
        """SQLite dialect checks in memory only; NOT live Turso integration."""

        async def asyncSetUp(self):
            import sqlite3

            self.sqlite3 = sqlite3

            class MemoryClient:
                def __init__(self):
                    self.conn = sqlite3.connect(":memory:", isolation_level=None)

                async def execute(self, sql, args=None):
                    cursor = self.conn.execute(sql, args or [])
                    return SimpleNamespace(
                        columns=[c[0] for c in cursor.description]
                        if cursor.description
                        else [],
                        rows=cursor.fetchall() if cursor.description else [],
                        last_insert_rowid=cursor.lastrowid,
                        rows_affected=max(0, cursor.rowcount),
                    )

                def transaction(self):
                    client = self

                    class Tx:
                        def __enter__(self):
                            client.conn.execute("BEGIN")
                            return self

                        def __exit__(self, kind, value, trace):
                            if client.conn.in_transaction:
                                client.conn.execute("ROLLBACK")

                        async def execute(self, *args):
                            return await client.execute(*args)

                        async def commit(self):
                            client.conn.execute("COMMIT")

                    return Tx()

            await AsyncTests.asyncSetUp(self)
            self.client = MemoryClient()
            self.db = object.__new__(DB)
            self.db.client = self.client
            await self.db.initialize()
            await self.db.execute(
                "INSERT INTO groups(group_id,title,joined_at) VALUES (-1001,'group',?)",
                [now()],
            )
            self.app.db = self.db
            self.app.require_admin = AsyncMock()
            self.app.safe_target = AsyncMock()
            self.redis.sadd = AsyncMock()
            self.redis.srem = AsyncMock()
            self.redis.sismember = AsyncMock(return_value=False)
            self.bot.send_document = AsyncMock()
            self.bot.pin_chat_message = AsyncMock()
            self.bot.leave_chat = AsyncMock()
            self.app.pending = AsyncMock()
            self.app.tree_snapshot = AsyncMock()

        async def asyncTearDown(self):
            self.client.conn.close()

        async def test_upgrade_preserves_legacy_rows_and_repeats(self):
            self.client.conn.close()
            self.client.conn = self.sqlite3.connect(":memory:", isolation_level=None)
            await self.db.execute(
                "CREATE TABLE groups ("
                + SCHEMA["groups"].replace(", active INTEGER DEFAULT 1", "")
                + ")"
            )
            await self.db.execute(
                "CREATE TABLE users ("
                + SCHEMA["users"].replace(", last_seen REAL", "")
                + ")"
            )
            await self.db.execute(
                "INSERT INTO groups(group_id,title,settings_json) VALUES (-1,'keep','{\"warnlimit\":7}')"
            )
            await self.db.execute(
                "INSERT INTO users(user_id,first_name) VALUES (33,'keep user')"
            )
            await self.db.initialize()
            await self.db.initialize()
            self.assertEqual(
                (await self.db.one("SELECT first_name FROM users WHERE user_id=33"))[
                    "first_name"
                ],
                "keep user",
            )
            self.assertEqual((await self.db.settings(-1))["warnlimit"], 7)
            self.assertEqual(
                (await self.db.one("SELECT active FROM groups WHERE group_id=-1"))[
                    "active"
                ],
                1,
            )
            for table in ("bans", "mutes"):
                self.assertIn(
                    "appeal_requested",
                    {
                        r["name"]
                        for r in await self.db.query(f"PRAGMA table_info({table})")
                    },
                )

        async def test_warn_defaults_only_new_groups_inherit(self):
            old = await self.db.settings(-1001)
            await self.db.set_bot_setting(
                "warn_defaults", {"warnlimit": 9, "warnaction": "kick", "warntime": 30}
            )
            await self.db.execute(
                "INSERT INTO groups(group_id,title) VALUES (-1002,'new')"
            )
            self.assertEqual(
                (await self.db.settings(-1001))["warnlimit"], old["warnlimit"]
            )
            self.assertEqual((await self.db.settings(-1002))["warnlimit"], 9)

        async def test_template_bio_and_drip_sql(self):
            call = MagicMock()
            call.message = self.m
            call.from_user = self.m.from_user
            await self.app.phase_pending(
                {
                    "op": "v2_template_save",
                    "gid": -1001,
                    "actor": 2,
                    "payload": {
                        "name": "spam",
                        "action": "mute",
                        "reason": "hello {user}",
                        "seconds": 259200,
                    },
                },
                call,
            )
            await self.app.phase1_command(
                self.m, "template", "use spam 33", ["use", "spam", "33"]
            )
            self.assertEqual(self.app.pending.call_args.args[4]["reason"], "hello 33")
            await self.app.phase1_command(self.m, "bioblacklist", "crypto", ["crypto"])
            await self.app.phase1_command(self.m, "bioblacklist", "crypto", ["crypto"])
            self.assertEqual(
                (await self.db.one("SELECT count(*) n FROM bioblacklist"))["n"], 1
            )
            for delay in ["72h", "1h", "24h"]:
                await self.app.phase3_command(
                    self.m, "setdrip", delay + " hi", [delay, "hi"]
                )
            await self.app.phase3_command(self.m, "cleardrip", "2", ["2"])
            seq = self.app.pending.call_args.args[4]["seq_id"]
            row = await self.db.one(
                "SELECT delay_seconds FROM drip_sequences WHERE seq_id=?", [seq]
            )
            self.assertEqual(row["delay_seconds"], 86400)

        async def test_proposal_sql_review_and_replay(self):
            result = await self.db.execute(
                "INSERT INTO admin_proposals(group_id,proposer_id,action,target_id,reason,status,created_at,duration) VALUES (-1001,22,'mute',33,'spam','pending',?,600)",
                [now()],
            )
            self.app.action = AsyncMock(return_value=9)
            r = {
                "op": "v2_proposal_approve",
                "gid": -1001,
                "actor": 2,
                "payload": {"id": result.last_insert_rowid},
            }
            await self.app.phase_pending(r, MagicMock())
            self.app.action.assert_awaited_once_with(-1001, 2, 33, "mute", "spam", 600)
            with self.assertRaises(ValueError):
                await self.app.phase_pending(r, MagicMock())

        async def test_appeal_sql_approve_and_backup_roundtrip(self):
            inserted = await self.db.execute(
                "INSERT INTO bans(group_id,user_id,admin_id,reason,created_at,appeal_requested) VALUES (-1001,33,2,'spam',?,1)",
                [now()],
            )
            message = self.m.model_copy(
                update={
                    "chat": T.Chat(id=1, type="private"),
                    "from_user": T.User(id=1, is_bot=False, first_name="owner"),
                }
            )
            call = MagicMock()
            call.message = message
            call.from_user = message.from_user
            self.app.action = AsyncMock(return_value=1)
            await self.app.panel_pending(
                {
                    "op": "v2_appeal_approve",
                    "actor": 1,
                    "payload": {"table": "bans", "id": inserted.last_insert_rowid},
                },
                call,
            )
            self.app.action.assert_awaited_once_with(
                -1001, 1, 33, "unban", "appeal approved", automatic=True
            )
            self.assertEqual(
                (await self.db.one("SELECT appeal_requested FROM bans"))[
                    "appeal_requested"
                ],
                4,
            )
            await self.app.owner_export(1, "backup")
            backup = json.loads(self.bot.send_document.call_args.args[1].data)
            self.assertEqual(set(backup["tables"]), set(SCHEMA))
            self.app.validate_import(backup["group_settings"]["-1001"])

        async def test_birthday_sql_deduplicates_daily_send(self):
            date = datetime.now(UTC)
            await self.db.execute(
                "INSERT INTO birthdays(user_id,group_id,month,day) VALUES (33,-1001,?,?)",
                [date.month, date.day],
            )
            self.bot.get_chat_member.return_value = SimpleNamespace(
                status="member", user=T.User(id=33, is_bot=False, first_name="member")
            )
            await birthday_daily({"app": self.app})
            await birthday_daily({"app": self.app})
            self.assertEqual(
                (
                    await self.db.one(
                        "SELECT count(*) n FROM birthday_delivery WHERE status='sent'"
                    )
                )["n"],
                1,
            )
            self.bot.send_rich_message.assert_awaited_once()

        async def test_broadcast_sql_reports_and_retry_skips(self):
            await self.db.execute(
                "INSERT INTO users(user_id,dm_opt_in,is_bot) VALUES (33,1,0),(44,1,0)"
            )
            self.bot.copy_message = AsyncMock(
                return_value=SimpleNamespace(message_id=50)
            )
            payload = {
                "source": 1,
                "message": 9,
                "target": "users",
                "broadcast_id": "test",
            }
            await broadcast_job({"app": self.app}, payload, 1)
            await broadcast_job({"app": self.app}, payload, 1)
            self.assertEqual(self.bot.copy_message.await_count, 2)
            row = await self.db.one(
                "SELECT * FROM broadcast_history ORDER BY broadcast_id DESC LIMIT 1"
            )
            self.assertEqual(row["sent_count"], 2)
            self.assertEqual(row["skipped_count"], 0)

        async def test_group_detail_sql_and_global_job(self):
            screen = await self.app.group_detail(-1001)
            self.assertEqual(screen.title, "group detail")
            self.app.action = AsyncMock(return_value=7)
            await global_moderation_job(
                {"app": self.app}, {"target": 33, "unban": False}, 1
            )
            self.app.action.assert_awaited_once_with(
                -1001, 1, 33, "ban", "owner global action", automatic=True
            )

    async def empty_async_iter():
        if False:
            yield None

    suite = unittest.TestSuite(
        [
            unittest.defaultTestLoader.loadTestsFromTestCase(CoreTests),
            unittest.defaultTestLoader.loadTestsFromTestCase(AsyncTests),
            unittest.defaultTestLoader.loadTestsFromTestCase(PhaseOneTests),
            unittest.defaultTestLoader.loadTestsFromTestCase(PhaseTwoTests),
            unittest.defaultTestLoader.loadTestsFromTestCase(PhaseThreeTests),
            unittest.defaultTestLoader.loadTestsFromTestCase(PhaseFourTests),
            unittest.defaultTestLoader.loadTestsFromTestCase(PhaseFiveTests),
            unittest.defaultTestLoader.loadTestsFromTestCase(PhaseSixTests),
            unittest.defaultTestLoader.loadTestsFromTestCase(SQLContractTests),
        ]
    )
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():
        raise SystemExit(1)


if __name__ == "__main__":
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if "--self-test" in sys.argv:
        self_test()
    elif "--coverage" in sys.argv:
        print(
            json.dumps(
                {
                    "registered": {k: v.split() for k, v in SECTIONS.items()},
                    "deferred": DEFERRED,
                    "unsupported_commands": UNSUPPORTED,
                    "platform_constraints": {
                        "purgeto": "deleteMessages accepts bounded ID batches; no getHistory exists and the API does not return an exact deleted-message count",
                        "delreactions": "deleteMessageReaction is applied to observed actors; no reactor-listing API or removeMessageReaction method exists",
                        "permapin": "member updates plus periodic repinning; no general message_unpin update exists",
                        "shadowmute": "Redis-first deletion happens after Telegram accepts the message; notification suppression is not guaranteed",
                        "cooldown": "mute duration is clamped to at least 60s to avoid Telegram's short-duration permanent-restriction behavior",
                        "migrations": "PRAGMA table_info plus ADD COLUMN; SQLite/libSQL does not support portable ADD COLUMN IF NOT EXISTS",
                    },
                    "limitations": [
                        "local text guards still require separately supplied checksummed model directories; no bundled models or image/video classifier",
                        "unrequested initial-spec gaps remain: native business checklists, rich poll option media, private forum topics, community sync, draft streaming, arbitrary SQL console and advanced search",
                        "scheduled posts, birthday greetings and broadcasts are at-least-once; crash windows after Telegram acceptance can duplicate delivery",
                        "per-group settings exports do not restore full operational backups or log/drip/federation routing; extract group_settings from a full backup for /importsettings",
                        "live Telegram, Turso, Docker and sustained-load acceptance tests still require deployment credentials and services",
                    ],
                },
                indent=2,
            )
        )
    else:
        asyncio.run(run())
