#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VCF 联系人解析与修正程序
功能：解析/显示联系人、自动加国际区号、联系人去重与号码合并、重复姓名修正、变更详细日志
"""

import re
import quopri
import copy
import argparse
import logging
from pathlib import Path
from datetime import datetime
from collections import OrderedDict
from typing import List, Dict, Set, Tuple, Optional


# ---------- 日志配置 ----------
LOG_DIR = Path(__file__).resolve().parent
LOG_FILE = LOG_DIR / f"vcf_fix_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"


def setup_logging(log_path: Optional[Path] = None, to_file: bool = True) -> logging.Logger:
    """配置日志：to_file=True 时同时输出到控制台和文件，否则仅控制台。"""
    logger = logging.getLogger("vcf_fix")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fmt = logging.Formatter(
        "[%(asctime)s] %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    if to_file and log_path:
        fh = logging.FileHandler(log_path, mode="w", encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger


# ---------- VCF 解析 ----------
def _strip_qp_soft_break(prev: str) -> str:
    """去掉上一行末尾的 QP 软换行符（单个 =），合并续行时用。"""
    s = prev.rstrip("\r\n")
    if s.endswith("="):
        return s[:-1]
    return s


def unfold_lines(lines: List[str]) -> List[str]:
    """VCF 续行：以空格/制表符开头的行与上一行合并；以 = 开头的行视为 QP 续行也合并。仅对以 = 开头的续行合并时去掉上一行末尾的 QP 软换行符 =。"""
    result = []
    for line in lines:
        if result and (line.startswith(" ") or line.startswith("\t")):
            result[-1] = result[-1].rstrip("\r\n") + line[1:]
        elif result and line.startswith("=") and ":" not in line.strip():
            prev = _strip_qp_soft_break(result[-1])
            result[-1] = prev + line.strip()
        else:
            result.append(line)
    return result


def decode_quoted_printable(value: str, charset: str = "utf-8") -> str:
    """解码 QUOTED-PRINTABLE 字符串。"""
    try:
        # 将 =XX 形式解码
        decoded = quopri.decodestring(value.encode("latin-1")).decode(charset, errors="replace")
        return decoded
    except Exception:
        return value


def encode_quoted_printable(value: str, charset: str = "utf-8") -> str:
    """将字符串编码为 QUOTED-PRINTABLE（与 vCard 原格式一致，不含内部换行由 VCF 折行统一处理）。"""
    try:
        encoded = quopri.encodestring(value.encode(charset))
        return encoded.decode("latin-1").replace("\r", "").replace("\n", "").rstrip()
    except Exception:
        return value


def parse_vcf_value(line: str) -> Tuple[str, str]:
    """解析一行 VCF：返回 (属性名, 值)。属性名含分号参数。"""
    if ":" not in line:
        return line.strip(), ""
    idx = line.index(":")
    name = line[:idx].strip()
    value = line[idx + 1:].strip()
    return name, value


def _strip_param_from_value(value: str) -> str:
    """
    若值被错误解析进参数片段（如 ENCODING=QUOTED-PRINTABLE:真实值），
    则只保留冒号后的真实值，避免显示「ENCODING=XXX」等。
    """
    if not value or ":" not in value:
        return value
    s = value.strip()
    if re.match(r"^(ENCODING|CHARSET)=[^:]*:", s, re.I):
        return s.split(":", 1)[-1].strip()
    return value


def decode_field_value(name: str, value: str) -> str:
    """若为 QUOTED-PRINTABLE 则解码。"""
    value = _strip_param_from_value(value)
    if "ENCODING=QUOTED-PRINTABLE" in name.upper() or "ENCODING=Q" in name.upper():
        charset = "utf-8"
        if "CHARSET=" in name.upper():
            m = re.search(r"CHARSET=([^;]+)", name, re.I)
            if m:
                charset = m.group(1).strip()
        return decode_quoted_printable(value.replace("\n", ""), charset)
    return value


def parse_one_vcard(lines: List[str]) -> Dict:
    """解析一个 VCARD 块为字典：key 为属性名，value 为列表（同一属性多行）。"""
    card = {}
    for line in lines:
        if not line.strip():
            continue
        name, value = parse_vcf_value(line)
        if not name:
            continue
        if name not in card:
            card[name] = []
        card[name].append(value)
    return card


def _fix_raw_n_fn_malformed_line(line: str) -> str:
    """
    修正首行中误把参数当值的情况（如 FN;CHARSET=UTF-8:ENCODING=QUOTED-PRINTABLE:值 → 参数归位），
    避免设备显示「ENCODING=XXX」。
    """
    if ":ENCODING=" in line.upper() or ":CHARSET=" in line.upper():
        line = re.sub(r":ENCODING=", ";ENCODING=", line, flags=re.I)
        line = re.sub(r":CHARSET=", ";CHARSET=", line, flags=re.I)
    return line


def _is_continuation_line(line: str) -> bool:
    """是否为续行（空格/制表符开头，或以 = 开头的 QP 续行）。"""
    if line.startswith(" ") or line.startswith("\t"):
        return True
    if line.startswith("=") and ":" not in line.strip():
        return True
    return False


def extract_raw_n_fn_adr(block: List[str]) -> Tuple[List[str], List[str], List[List[str]]]:
    """从原始 vCard 块中提取 N、FN、ADR 的原始行（含折行），原样保留供写入。返回 (_raw_N, _raw_FN, _raw_ADR)。"""
    raw_n: List[str] = []
    raw_fn: List[str] = []
    raw_adr_list: List[List[str]] = []
    current: Optional[str] = None  # "N", "FN", "ADR"
    current_lines: List[str] = []

    def flush() -> None:
        nonlocal current, current_lines
        if not current:
            return
        if current_lines:
            if current == "N":
                raw_n.extend(current_lines)
            elif current == "FN":
                raw_fn.extend(current_lines)
            elif current == "ADR":
                raw_adr_list.append(list(current_lines))
        current = None
        current_lines = []

    for line in block:
        s = line.strip()
        if s in ("BEGIN:VCARD", "END:VCARD"):
            flush()
            continue
        if not s:
            continue
        up = s.upper()
        if (up.startswith("N;") or up.startswith("N:")) and not up.startswith("NOTE"):
            flush()
            current = "N"
            current_lines = [_fix_raw_n_fn_malformed_line(line.rstrip("\r\n"))]
        elif up.startswith("FN;") or up.startswith("FN:"):
            flush()
            current = "FN"
            current_lines = [_fix_raw_n_fn_malformed_line(line.rstrip("\r\n"))]
        elif up.startswith("ADR;") or up.startswith("ADR:"):
            flush()
            current = "ADR"
            current_lines = [line.rstrip("\r\n")]
        elif _is_continuation_line(line) and current:
            current_lines.append(line.rstrip("\r\n"))
        else:
            flush()
    flush()
    return (raw_n, raw_fn, raw_adr_list)


def extract_raw_property_order(block: List[str]) -> List[Tuple[str, List[str]]]:
    """从原始 vCard 块按出现顺序提取每个属性的完整行（含折行）。返回 [(属性名, [行列表]), ...]。"""
    result: List[Tuple[str, List[str]]] = []
    current_key: Optional[str] = None
    current_lines: List[str] = []

    def flush() -> None:
        nonlocal current_key, current_lines
        if current_key is not None and current_lines:
            result.append((current_key, list(current_lines)))
        current_key = None
        current_lines = []

    for line in block:
        s = line.strip()
        if s in ("BEGIN:VCARD", "END:VCARD") or not s:
            flush()
            continue
        if _is_continuation_line(line) and current_key is not None:
            current_lines.append(line.rstrip("\r\n"))
            continue
        flush()
        if ":" in s:
            idx = s.index(":")
            current_key = s[:idx].strip()
            current_lines = [line.rstrip("\r\n")]
    flush()
    return result


def parse_vcf(content: str) -> List[Dict]:
    """解析整个 VCF 内容，返回 VCARD 列表。同时提取并保存 N、FN、ADR 的原始行供写入时原样输出。"""
    raw_lines = content.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    blocks: List[List[str]] = []
    block: List[str] = []
    for line in raw_lines:
        if line.strip() == "BEGIN:VCARD":
            block = []
        elif line.strip() == "END:VCARD":
            if block:
                blocks.append(block)
            block = []
        else:
            block.append(line)
    if block:
        blocks.append(block)
    cards = []
    for block in blocks:
        logical = unfold_lines(block)
        card = parse_one_vcard(logical)
        raw_n, raw_fn, raw_adr = extract_raw_n_fn_adr(block)
        if raw_n:
            card["_raw_N"] = raw_n
        if raw_fn:
            card["_raw_FN"] = raw_fn
        if raw_adr:
            card["_raw_ADR"] = raw_adr
        card["_raw_order"] = extract_raw_property_order(block)
        cards.append(card)
    return cards


def get_display_name(card: Dict) -> str:
    """从 VCARD 获取显示名：优先 FN，否则用 N 拼。"""
    fn_key = next((k for k in card if k.upper().startswith("FN")), None)
    if fn_key and card[fn_key]:
        return decode_field_value(fn_key, card[fn_key][0])
    n_key = next((k for k in card if k.upper().startswith("N")), None)
    if n_key and card[n_key]:
        n_val = decode_field_value(n_key, card[n_key][0])
        parts = [p.strip() for p in n_val.split(";")]
        return "".join(p for p in parts if p)
    return ""


def get_tel_list(card: Dict) -> List[Tuple[str, str]]:
    """获取 (type_key, number) 列表，如 ('TEL;CELL', '13800138000')。"""
    result = []
    for key in card:
        if key.upper().startswith("TEL"):
            for val in card[key]:
                if val.strip():
                    result.append((key, val.strip()))
    return result


def set_tel_list(card: Dict, tel_list: List[Tuple[str, str]]) -> None:
    """用新的 (type_key, number) 列表替换 TEL。"""
    to_remove = [k for k in card if k.upper().startswith("TEL")]
    for k in to_remove:
        del card[k]
    # 使用常见类型
    for i, (_, num) in enumerate(tel_list):
        key = "TEL;CELL" if i == 0 else "TEL;CELL"
        if i > 0:
            key = "TEL;CELL"
        card[key] = [num]


def set_display_name(card: Dict, new_name: str) -> None:
    """设置 FN 和 N 为统一姓名（N 格式为 ;名;;; ）。"""
    fn_key = next((k for k in card if k.upper().startswith("FN")), None)
    n_key = next((k for k in card if k.upper().startswith("N")), None)
    if fn_key:
        card[fn_key] = [new_name]
    if n_key:
        card[n_key] = [";" + new_name + ";;;"]


# ---------- 电话号码规范化与区号 ----------
def digits_only(s: str) -> str:
    return re.sub(r"\D", "", s)


def infer_region_and_add_prefix(num: str) -> Tuple[str, Optional[str]]:
    """
    根据纯数字长度与首位推断地区并加国际区号。
    返回 (带区号的号码, 地区说明)。
    中国大陆 +86 通常为 11 位且以 1 开头；香港 8 位 +852；澳门 8 位 +853。
    """
    raw = digits_only(num)
    if not raw:
        return num, None
    # 已带国际区号的不再添加
    if raw.startswith("86") and len(raw) in (11 + 2, 12 + 2):
        return "+" + raw[:2] + " " + raw[2:] if len(raw) > 2 else "+" + raw, "中国大陆(已有+86)"
    if raw.startswith("852") and len(raw) == 8 + 3:
        return "+" + raw[:3] + " " + raw[3:], "中国香港(已有+852)"
    if raw.startswith("853") and len(raw) == 8 + 3:
        return "+" + raw[:3] + " " + raw[3:], "中国澳门(已有+853)"
    # 11 位且以 1 开头 -> 中国大陆
    if len(raw) == 11 and raw[0] == "1":
        return "+86 " + raw, "中国大陆"
    # 8 位
    if len(raw) == 8:
        if raw[0] in ("5", "9"):
            return "+852 " + raw, "中国香港"
        if raw[0] == "6":
            return "+853 " + raw, "中国澳门"
        return "+852 " + raw, "中国香港(默认8位)"
    # 10 位固话等视为中国大陆
    if len(raw) == 10 or (len(raw) == 11 and raw[0] != "1"):
        return "+86 " + raw, "中国大陆(固话/其他)"
    if len(raw) == 12 and raw.startswith("86"):
        return "+86 " + raw[2:], "中国大陆(去重86)"
    return num, None


def normalize_phone_display(num: str) -> str:
    """规范化显示与存储格式（加区号）。"""
    normalized, _ = infer_region_and_add_prefix(num)
    return normalized


# ---------- 重复姓名修正 ----------
def fix_duplicate_name(name: str) -> str:
    """
    修正三类错误姓名：
    1) AAB/AABC 等：前两字相同且超过 2 字时删第 1 字，如「白白艳强」→「白艳强」。
    2) 整段重复：如「王三王三」→「王三」、「成龙 c00858566成龙 c00858566」→「成龙 c00858566」。
    3) 少于等于 2 字保留（如「朵朵」）。
    """
    if not name or len(name) <= 2:
        return name
    # 前两字相同且超过 2 字：删掉第 1 个字，循环直到不满足（如 白白艳强 → 白艳强）
    while len(name) > 2 and name[0] == name[1]:
        name = name[1:]
    if len(name) <= 2:
        return name
    n = len(name)
    # 完全两段相同（含「姓名+数字」重复，如 成龙 c00858566成龙 c00858566 → 成龙 c00858566）
    if n % 2 == 0 and name[: n // 2] == name[n // 2 :]:
        return name[: n // 2]
    # 尝试较短周期
    for length in range(1, n // 2 + 1):
        if n % length != 0:
            continue
        segment = name[:length]
        if segment * (n // length) == name:
            return segment
    return name


def get_merge_key(name: str) -> str:
    """
    合并用键：若姓名为「姓名+空格+数字/字母」形式（如「鲍旭 b00357649」），
    取空格前部分作为键，便于与纯姓名「鲍旭」合并为一条并保留「姓名+数字」。
    """
    s = name.strip()
    if not s:
        return "(无姓名)"
    if " " in s:
        suffix = s[s.rfind(" ") + 1 :]
        if suffix and (suffix.isalnum() or re.match(r"^[a-zA-Z0-9\-_]+$", suffix)):
            return s[: s.rfind(" ")].strip() or s
    return s


def _is_name_with_id(name: str) -> bool:
    """是否为「姓名+空格+数字/字母」形式（如「鲍旭 b00357649」）。"""
    s = name.strip()
    if " " not in s:
        return False
    suffix = s[s.rfind(" ") + 1 :]
    return bool(suffix) and bool(re.match(r"^[a-zA-Z0-9\-_]+$", suffix))


# ---------- 联系人合并（按姓名去重、号码合并）----------
def merge_contacts(
    cards: List[Dict],
    logger: logging.Logger,
    fix_name: bool = True,
) -> Tuple[List[Dict], int, int]:
    """按显示名合并联系人，同一人的多个号码合并到一个 VCARD。返回 (结果列表, 合并掉的联系人数, 修正姓名的联系人数)。"""
    by_name: Dict[str, Dict] = OrderedDict()
    merged_count = 0
    name_fix_count = 0
    for card in cards:
        name = get_display_name(card)
        # 先做姓名修正（如 鲍鲍永亮→鲍永亮、王三王三→王三），仅用于合并键与显示；N/FN 输出保持源文件原样以保证导入兼容
        if fix_name:
            name_fixed = fix_duplicate_name(name)
            if name_fixed != name:
                name_fix_count += 1
                logger.info(f"[姓名去重] 「{name}」 -> 「{name_fixed}」")
                logger.debug(f"  修正重复姓名: {name!r} -> {name_fixed!r}")
                name = name_fixed
                set_display_name(card, name)
                # 用修正后的姓名重写 _raw_N/_raw_FN，折行字符数与原文件一致，便于手机导入
                _set_card_raw_n_fn_from_current(card)

        name_key = get_merge_key(name)
        tels = get_tel_list(card)
        if name_key not in by_name:
            by_name[name_key] = copy.deepcopy(card)
            by_name[name_key]["_tels"] = []
            seen = set()
            for type_k, num in tels:
                d = digits_only(num)
                if d and d not in seen:
                    seen.add(d)
                    by_name[name_key]["_tels"].append((type_k, num))
        else:
            merged_count += 1
            existing_digits = set(digits_only(t[1]) for t in by_name[name_key]["_tels"])
            for type_k, num in tels:
                num_digits = digits_only(num)
                if num_digits and num_digits not in existing_digits:
                    by_name[name_key]["_tels"].append((type_k, num))
                    existing_digits.add(num_digits)
                    logger.info(f"[联系人合并] 将号码 {num} 合并到联系人「{name_key}」")
            # 合并后显示名优先保留「姓名+数字」或更规范的一条（如 白艳强 优先于 白白艳强）；采用该条的原始 N/FN 行
            existing_name = get_display_name(by_name[name_key])
            preferred = name if _is_name_with_id(name) else existing_name
            if not _is_name_with_id(name) and not _is_name_with_id(existing_name):
                # 两条均为纯姓名时，优先保留「修正后」更短/规范的一条（如 白艳强 而非 白白艳强）
                if fix_duplicate_name(existing_name) != existing_name and fix_duplicate_name(name) == name:
                    preferred = name
            if preferred != existing_name:
                logger.info(f"[联系人合并] 显示名采用: 「{existing_name}」 -> 「{preferred}」")
            set_display_name(by_name[name_key], preferred)
            if preferred == name:
                if "_raw_N" in card:
                    by_name[name_key]["_raw_N"] = list(card["_raw_N"])
                if "_raw_FN" in card:
                    by_name[name_key]["_raw_FN"] = list(card["_raw_FN"])
            continue

    # 写回 TEL 到每个 card，保留原 TEL 类型（TEL;CELL、TEL;HOME、TEL;VOICE 等）
    result = []
    for name_key, card in by_name.items():
        tels = card.pop("_tels", [])
        to_remove = [k for k in card if k.upper().startswith("TEL")]
        for k in to_remove:
            del card[k]
        for type_k, num in tels:
            if type_k not in card:
                card[type_k] = []
            card[type_k].append(num)
        result.append(card)
    if merged_count:
        logger.info(f"[联系人合并] 共合并 {merged_count} 条重复联系人")
    return result, merged_count, name_fix_count


# ---------- 为所有号码加区号 ----------
def add_country_codes_to_cards(cards: List[Dict], logger: logging.Logger) -> int:
    """为每个联系人的电话号码添加国际区号：保留原号码，并新增带区号的号码。返回添加了区号的号码个数。"""
    added_count = 0
    for card in cards:
        name = get_display_name(card)
        for key in list(card):
            if not key.upper().startswith("TEL"):
                continue
            new_vals = []
            for num in card[key]:
                normalized, region = infer_region_and_add_prefix(num)
                new_vals.append(num)  # 保留不带国际区号的原号码
                if region and normalized != num:
                    new_vals.append(normalized)  # 新增带国际区号的号码
                    added_count += 1
                    logger.info(f"[区号] 联系人「{name}」: 保留 {num}，新增 {normalized} ({region})")
                    logger.debug(f"  原号码: {num!r} -> 新增: {normalized!r}")
            card[key] = new_vals
    return added_count


# ---------- 输出 VCF（RFC 2426：75 字符折行；未修改属性原样输出以兼容 iOS 等）----------
# RFC 2426 section 2.6: lines longer than 75 characters SHOULD be folded (continuation = space + content)
VCF_LINE_MAX = 75
VCF_FOLD_CONTINUATION = 74  # 续行 = " " + 最多 74 字符，总长 75
VCF_CRLF = "\r\n"


def fold_vcf_line(line: str) -> List[str]:
    """
    按 RFC 2426 折行：首行最多 75 字符，续行以空格开头、每行最多 74 字符。
    不在 QUOTED-PRINTABLE 的 '=' 处断开，避免续行行末单独 '=' 被解析为 QP 软换行导致兼容性问题。
    """
    return fold_vcf_line_with_params(line, VCF_LINE_MAX, VCF_FOLD_CONTINUATION)


def fold_vcf_line_with_params(
    line: str, first_max: int = 75, continuation_max: int = 74
) -> List[str]:
    """
    按指定字符数折行：首行最多 first_max 字符，续行以空格开头、每行最多 continuation_max 字符。
    与原文件折行一致时有利于手机等设备导入。
    """
    if len(line) <= first_max:
        return [line]
    out = []
    pos = 0
    first = True
    while pos < len(line):
        if first:
            end = min(pos + first_max, len(line))
            while end > pos and line[end - 1] == "=":
                end -= 1
            if end <= pos:
                end = min(pos + first_max, len(line))
            out.append(line[pos:end])
            pos = end
            first = False
        else:
            end = min(pos + continuation_max, len(line))
            while end > pos and line[end - 1] == "=":
                end -= 1
            if end <= pos:
                end = min(pos + continuation_max, len(line))
            chunk = line[pos:end]
            if chunk:
                out.append(" " + chunk)
            pos = end
    return out


def _infer_fold_params_from_raw_lines(raw_lines: List[str]) -> Tuple[int, int]:
    """
    从原始 N/FN 折行推断首行最大长度与续行内容最大长度（续行含前导空格时总长为 1+continuation_max）。
    若只有一行或无法推断，返回默认 75、74。
    """
    if not raw_lines:
        return VCF_LINE_MAX, VCF_FOLD_CONTINUATION
    first_len = len(raw_lines[0].rstrip("\r\n"))
    if len(raw_lines) == 1:
        return first_len if first_len >= VCF_LINE_MAX else VCF_LINE_MAX, VCF_FOLD_CONTINUATION
    cont_lens = [len(ln.rstrip("\r\n")) - 1 for ln in raw_lines[1:] if ln.startswith(" ") or ln.startswith("\t")]
    cont_max = max(cont_lens, default=VCF_FOLD_CONTINUATION)
    return first_len, cont_max


def _looks_like_qp(value: str) -> bool:
    """判断是否已是 QUOTED-PRINTABLE 形式（=XX 且无可解码的宽字符），避免双编码。"""
    if not value or "=" not in value:
        return False
    # 已是 QP：含 =XX 且不含常见 Unicode 字符（我们解码后会得到中文等）
    if re.search(r"[\u4e00-\u9fff\u3000-\u303f\uff00-\uffef]", value):
        return False
    return bool(re.search(r"=[0-9A-Fa-f]{2}", value))


def _value_for_output(key: str, value: str) -> str:
    """若 key 含 ENCODING=QUOTED-PRINTABLE：值已是 QP 则原样，否则按 QP 编码。"""
    if "ENCODING=QUOTED-PRINTABLE" in key.upper() or "ENCODING=Q" in key.upper():
        if _looks_like_qp(value):
            return value
        charset = "utf-8"
        if "CHARSET=" in key.upper():
            m = re.search(r"CHARSET=([^;]+)", key, re.I)
            if m:
                charset = m.group(1).strip()
        return encode_quoted_printable(value, charset)
    return value


def _strip_encoding_from_key(key: str) -> str:
    """
    从属性名中移除 ENCODING=QUOTED-PRINTABLE / ENCODING=Q，避免生成的 N/FN 使用 QP 编码。
    QP 编码值中的字面量 '-' 可能被部分手机误解析，改为仅用 CHARSET 输出 UTF-8 更稳妥。
    """
    k = key
    for pat in (r";ENCODING=QUOTED-PRINTABLE", r";ENCODING=Q\b"):
        k = re.sub(pat, "", k, flags=re.I)
    return k


def _set_card_raw_n_fn_from_current(card: Dict) -> None:
    """
    用卡片当前 N/FN 的值生成折行写入 _raw_N / _raw_FN。
    折行字符数与原文件一致；且去掉 ENCODING 参数、直接输出 UTF-8，避免 QP 值中的 '-' 导致手机导入失败。
    """
    n_key = next((k for k in card if k.upper().startswith("N") and not k.upper().startswith("NOTE")), None)
    fn_key = next((k for k in card if k.upper().startswith("FN")), None)
    if n_key and n_key in card and card[n_key]:
        v = str(card[n_key][0]).strip()
        if v:
            out_key = _strip_encoding_from_key(n_key)
            line = f"{out_key}:{_value_for_output(out_key, v)}"
            first_max, cont_max = _infer_fold_params_from_raw_lines(card.get("_raw_N", []))
            card["_raw_N"] = fold_vcf_line_with_params(line, first_max, cont_max)
    if fn_key and fn_key in card and card[fn_key]:
        v = str(card[fn_key][0]).strip()
        if v:
            out_key = _strip_encoding_from_key(fn_key)
            line = f"{out_key}:{_value_for_output(out_key, v)}"
            first_max, cont_max = _infer_fold_params_from_raw_lines(card.get("_raw_FN", []))
            card["_raw_FN"] = fold_vcf_line_with_params(line, first_max, cont_max)


def _key_prefix(key: str) -> str:
    """属性名的前缀（N、FN、TEL、ADR、PHOTO 等），用于判断类型。"""
    k = key.upper()
    if k.startswith("NOTE"):
        return "NOTE"
    if k.startswith("N"):
        return "N"
    if k.startswith("FN"):
        return "FN"
    if k.startswith("TEL"):
        return "TEL"
    if k.startswith("ADR"):
        return "ADR"
    if k.startswith("PHOTO"):
        return "PHOTO"
    return key.split(";")[0] if ";" in key else key


def card_to_vcard_lines_simple(card: Dict) -> List[str]:
    """按原始属性顺序输出，符合 RFC 2426：N/FN/ADR 及未修改属性原样输出，仅 TEL 由程序生成并 75 字符折行。"""
    out = ["BEGIN:VCARD", "VERSION:2.1"]
    raw_order = card.get("_raw_order")
    if raw_order:
        i = 0
        while i < len(raw_order):
            key, raw_lines = raw_order[i]
            prefix = _key_prefix(key)
            if prefix == "VERSION" or key.upper() == "VERSION":
                pass  # 已在上方输出
            elif prefix == "N":
                if "_raw_N" in card:
                    out.extend(card["_raw_N"])
                else:
                    for k in [x for x in card if x.upper().startswith("N") and not x.upper().startswith("NOTE")]:
                        for v in card[k]:
                            if v is not None and str(v).strip():
                                for folded in fold_vcf_line(f"{k}:{_value_for_output(k, str(v))}"):
                                    out.append(folded)
            elif prefix == "FN":
                if "_raw_FN" in card:
                    out.extend(card["_raw_FN"])
                else:
                    for k in [x for x in card if x.upper().startswith("FN")]:
                        for v in card[k]:
                            if v is not None and str(v).strip():
                                for folded in fold_vcf_line(f"{k}:{_value_for_output(k, str(v))}"):
                                    out.append(folded)
            elif prefix == "ADR":
                out.extend(raw_lines)
            elif prefix == "TEL":
                # 原始文件中 TEL 类型顺序（如先 CELL 后 HOME），据此输出卡片中所有 TEL，每行 RFC 2426 折行
                tel_key_order: List[str] = []
                j = i
                while j < len(raw_order) and _key_prefix(raw_order[j][0]) == "TEL":
                    k = raw_order[j][0]
                    if k not in tel_key_order:
                        tel_key_order.append(k)
                    j += 1
                seen_tel_keys = set(tel_key_order)
                for tel_key in tel_key_order:
                    for v in card.get(tel_key, []):
                        if v is not None and str(v).strip():
                            for folded in fold_vcf_line(f"{tel_key}:{v}"):
                                out.append(folded)
                for tel_key in (k for k in card if k.upper().startswith("TEL") and k not in seen_tel_keys):
                    for v in card[tel_key]:
                        if v is not None and str(v).strip():
                            for folded in fold_vcf_line(f"{tel_key}:{v}"):
                                out.append(folded)
                i = j - 1
            else:
                out.extend(raw_lines)
            i += 1
    else:
        # 无原始顺序（不应出现）：按固定顺序生成，全部 75 字符折行
        if "_raw_N" in card:
            out.extend(card["_raw_N"])
        else:
            for k in [x for x in card if x.upper().startswith("N") and not x.upper().startswith("NOTE")]:
                for v in card[k]:
                    if v is not None and str(v).strip():
                        for folded in fold_vcf_line(f"{k}:{_value_for_output(k, str(v))}"):
                            out.append(folded)
        if "_raw_FN" in card:
            out.extend(card["_raw_FN"])
        else:
            for k in [x for x in card if x.upper().startswith("FN")]:
                for v in card[k]:
                    if v is not None and str(v).strip():
                        for folded in fold_vcf_line(f"{k}:{_value_for_output(k, str(v))}"):
                            out.append(folded)
        if "_raw_ADR" in card:
            for adr_lines in card["_raw_ADR"]:
                out.extend(adr_lines)
        else:
            for k in [x for x in card if x.upper().startswith("ADR")]:
                for v in card[k]:
                    if v is not None and str(v).strip():
                        for folded in fold_vcf_line(f"{k}:{v}"):
                            out.append(folded)
        for k in card:
            if k.upper().startswith("TEL"):
                for v in card[k]:
                    if v is not None and str(v).strip():
                        for folded in fold_vcf_line(f"{k}:{v}"):
                            out.append(folded)
        other = [
            k for k in card
            if not k.upper().startswith("TEL") and not k.upper().startswith("N") and not k.upper().startswith("FN")
            and not k.upper().startswith("ADR") and k not in ("_raw_N", "_raw_FN", "_raw_ADR", "_raw_order")
            and k.upper() not in ("VERSION", "BEGIN", "END")
        ]
        for k in other:
            for v in card[k]:
                if v is not None and str(v).strip():
                    for folded in fold_vcf_line(f"{k}:{v}"):
                        out.append(folded)
    out.append("END:VCARD")
    return out


def write_vcf(cards: List[Dict], path: Path) -> None:
    """将 VCARD 列表写回 VCF 文件（CRLF、与原 vCard 格式一致）。"""
    with open(path, "w", encoding="utf-8", newline="") as f:
        for card in cards:
            for line in card_to_vcard_lines_simple(card):
                f.write(line + VCF_CRLF)


# ---------- 查询/展示 ----------
def print_contact(card: Dict, index: Optional[int] = None) -> str:
    """格式化单条联系人信息。"""
    name = get_display_name(card)
    tels = get_tel_list(card)
    tel_str = ", ".join(n for _, n in tels)
    prefix = f"  [{index}] " if index is not None else "  "
    return f"{prefix}{name} | {tel_str}"


def show_all_contacts(cards: List[Dict], logger: logging.Logger) -> None:
    """打印显示所有联系人的号码。"""
    logger.info("========== 全部联系人 ==========")
    for i, card in enumerate(cards):
        logger.info(print_contact(card, i + 1))


def show_contacts_by_name(cards: List[Dict], name_query: str, logger: logging.Logger) -> None:
    """显示指定姓名下的全部号码信息（支持模糊匹配）。"""
    name_query = name_query.strip()
    found = [
        (i, card) for i, card in enumerate(cards)
        if name_query in get_display_name(card)
    ]
    if not found:
        logger.info(f"未找到姓名包含「{name_query}」的联系人")
        return
    logger.info(f"========== 姓名包含「{name_query}」的联系人（共 {len(found)} 条）==========")
    for i, (idx, card) in enumerate(found):
        name = get_display_name(card)
        tels = get_tel_list(card)
        logger.info(f"  [{i+1}] 姓名: {name}")
        for _, num in tels:
            logger.info(f"       号码: {num}")
        logger.info("")


def show_contacts_by_number(cards: List[Dict], number_query: str, logger: logging.Logger) -> None:
    """显示包含指定号码的全部联系人信息。"""
    digits = digits_only(number_query)
    if not digits:
        logger.info("未输入有效号码")
        return
    found = []
    for card in cards:
        for _, num in get_tel_list(card):
            if digits_only(num) == digits or digits in digits_only(num):
                found.append(card)
                break
    if not found:
        logger.info(f"未找到包含号码「{number_query}」的联系人")
        return
    logger.info(f"========== 包含号码「{number_query}」的联系人（共 {len(found)} 条）==========")
    for i, card in enumerate(found):
        name = get_display_name(card)
        tels = get_tel_list(card)
        logger.info(f"  [{i+1}] 姓名: {name}")
        for _, num in tels:
            logger.info(f"       号码: {num}")
        logger.info("")


# ---------- 主流程 ----------
def main():
    parser = argparse.ArgumentParser(
        description="VCF 联系人解析与修正。不指定功能选项时，默认自动修复并写入日志。"
    )
    parser.add_argument("input", nargs="?", default="Contacts_20260119.vcf", help="输入 VCF 文件")
    parser.add_argument("-o", "--output", default=None, help="输出 VCF 文件（默认在输入文件名后加 _fixed）")

    # 功能开关（默认开启；加对应 --no-* 则关闭）
    parser.add_argument("--no-country-code", action="store_true", help="不自动添加国际区号")
    parser.add_argument("--no-log", action="store_true", help="不将变更日志写入文件（仅控制台输出）")
    parser.add_argument("--no-fix-name", action="store_true", help="不自动修正重复姓名")
    parser.add_argument("--no-merge", action="store_true", help="不自动合并同名联系人的号码")
    parser.add_argument("--no-photo", action="store_true", help="输出时去掉头像（不保留 PHOTO 字段）")

    # 仅查询/展示（指定后只执行对应展示并退出，不写回文件）
    parser.add_argument("--list", "--show-all", dest="show_all", action="store_true", help="打印显示所有联系人及号码")
    parser.add_argument("--name", dest="query_name", metavar="NAME", help="显示指定姓名下的全部号码信息")
    parser.add_argument("--number", dest="query_number", metavar="NUM", help="显示指定号码所属的全部联系人信息")

    # 无号码联系人
    parser.add_argument("--remove-no-tel", action="store_true", help="删除没有号码的联系人（仅姓名无号码的不写入输出文件）")

    args = parser.parse_args()

    log_path = LOG_DIR / f"vcf_fix_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    to_file = not args.no_log
    logger = setup_logging(log_path if to_file else None, to_file=to_file)
    if to_file:
        logger.info(f"日志文件: {log_path}")

    input_path = Path(args.input)
    if not input_path.is_absolute():
        input_path = LOG_DIR / input_path
    if not input_path.exists():
        logger.error(f"输入文件不存在: {input_path}")
        return 1

    content = input_path.read_text(encoding="utf-8", errors="replace")
    cards = parse_vcf(content)
    logger.info(f"共解析到 {len(cards)} 个联系人")

    # 仅查询模式：只展示，不修正、不写回
    if args.show_all:
        show_all_contacts(cards, logger)
        return 0
    if args.query_name:
        show_contacts_by_name(cards, args.query_name, logger)
        return 0
    if args.query_number:
        show_contacts_by_number(cards, args.query_number, logger)
        return 0

    # 统计用
    total_before = len(cards)
    merged_contacts_count = 0
    name_fix_count = 0
    country_code_added_count = 0

    # 自动修复模式：先显示修正前列表
    logger.info("========== 解析的联系人（修正前）==========")
    for i, card in enumerate(cards):
        logger.info(print_contact(card, i + 1))

    # 合并联系人（可选：姓名修正、合并）
    if args.no_merge:
        # 不合并时仅做同卡内号码去重，保留原 TEL 类型（TEL;CELL、TEL;HOME 等）
        for card in cards:
            tels = get_tel_list(card)
            seen = set()
            new_tels = []  # (type_k, num)
            for tk, num in tels:
                d = digits_only(num)
                if d and d not in seen:
                    seen.add(d)
                    new_tels.append((tk, num))
            to_remove = [k for k in card if k.upper().startswith("TEL")]
            for k in to_remove:
                del card[k]
            for type_k, num in new_tels:
                card.setdefault(type_k, []).append(num)
    else:
        cards, merged_contacts_count, name_fix_count = merge_contacts(
            cards, logger, fix_name=not args.no_fix_name
        )

    # 添加国际区号（可选）
    if not args.no_country_code:
        country_code_added_count = add_country_codes_to_cards(cards, logger)

    # 再次修正姓名（仅当开启姓名修正且做了合并时可能还有重复）
    if not args.no_fix_name and not args.no_merge:
        for card in cards:
            name = get_display_name(card)
            fixed = fix_duplicate_name(name)
            if fixed != name:
                name_fix_count += 1
                logger.info(f"[姓名去重] 「{name}」 -> 「{fixed}」")
                set_display_name(card, fixed)

    total_after = len(cards)

    # 输出修正后列表
    logger.info("========== 修正后联系人 ==========")
    for i, card in enumerate(cards):
        logger.info(print_contact(card, i + 1))

    # 仅姓名、无号码的联系人
    no_tel_cards = [c for c in cards if not get_tel_list(c)]
    no_tel_count = len(no_tel_cards)
    logger.info("========== 仅姓名无号码的联系人 ==========")
    if no_tel_cards:
        logger.info(f"  共 {no_tel_count} 人（无任何号码）:")
        for i, card in enumerate(no_tel_cards):
            name = get_display_name(card)
            logger.info(f"  [{i + 1}] {name}")
    else:
        logger.info("  无")

    # 若指定删除无号码联系人，则从输出中排除
    removed_no_tel_count = 0
    if args.remove_no_tel and no_tel_cards:
        cards = [c for c in cards if get_tel_list(c)]
        removed_no_tel_count = no_tel_count
        total_after = len(cards)
        logger.info(f"[删除无号码联系人] 已从输出中移除 {removed_no_tel_count} 人")

    # 修复完成汇总统计
    logger.info("========== 修复完成汇总 ==========")
    logger.info(f"  解析总计联系人: {total_before}")
    logger.info(f"  修复及合并后总计联系人: {total_after}")
    logger.info(f"  其中合并了联系人: {merged_contacts_count}")
    logger.info(f"  修复姓名联系人数: {name_fix_count}")
    logger.info(f"  添加区号的号码数: {country_code_added_count}")
    logger.info(f"  仅姓名无号码的联系人: {no_tel_count}")
    if removed_no_tel_count:
        logger.info(f"  已删除无号码联系人（未写入输出）: {removed_no_tel_count}")

    # 若指定不保留头像，则从每条联系人中移除 PHOTO
    if args.no_photo:
        for card in cards:
            for k in list(card):
                if k.upper().startswith("PHOTO"):
                    del card[k]
        logger.info("[去掉头像] 已从输出中移除所有 PHOTO 字段")

    out_path = Path(args.output) if args.output else input_path.parent / (input_path.stem + "_fixed.vcf")
    if not out_path.is_absolute():
        out_path = LOG_DIR / out_path
    write_vcf(cards, out_path)
    logger.info(f"已写入: {out_path}")
    return 0


if __name__ == "__main__":
    exit(main() or 0)
