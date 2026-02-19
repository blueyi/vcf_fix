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
def unfold_lines(lines: List[str]) -> List[str]:
    """VCF 续行：以空格/制表符开头的行与上一行合并。"""
    result = []
    for line in lines:
        if result and (line.startswith(" ") or line.startswith("\t")):
            result[-1] = result[-1].rstrip("\r\n") + line[1:]
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


def parse_vcf_value(line: str) -> Tuple[str, str]:
    """解析一行 VCF：返回 (属性名, 值)。属性名含分号参数。"""
    if ":" not in line:
        return line.strip(), ""
    idx = line.index(":")
    name = line[:idx].strip()
    value = line[idx + 1:].strip()
    return name, value


def decode_field_value(name: str, value: str) -> str:
    """若为 QUOTED-PRINTABLE 则解码。"""
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


def parse_vcf(content: str) -> List[Dict]:
    """解析整个 VCF 内容，返回 VCARD 列表。"""
    raw_lines = content.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    lines = unfold_lines(raw_lines)
    cards = []
    current = []
    for line in lines:
        if line.strip() == "BEGIN:VCARD":
            current = []
        elif line.strip() == "END:VCARD":
            if current:
                cards.append(parse_one_vcard(current))
            current = []
        else:
            current.append(line)
    if current:
        cards.append(parse_one_vcard(current))
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
    若姓名为重复拼接（如「王三王三」），则改为单次（「王三」）。
    也处理奇数长度一半重复等情况。
    少于等于 2 个字的名字不去重（如「朵朵」保留）。
    """
    if not name or len(name) <= 2:
        return name
    n = len(name)
    # 完全两段相同
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
        if fix_name:
            name_fixed = fix_duplicate_name(name)
            if name_fixed != name:
                name_fix_count += 1
                logger.info(f"[姓名去重] 「{name}」 -> 「{name_fixed}」")
                logger.debug(f"  修正重复姓名: {name!r} -> {name_fixed!r}")
                name = name_fixed
                set_display_name(card, name)

        name_key = name.strip() or "(无姓名)"
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
            continue

    # 写回 TEL 到每个 card，并去掉辅助 key（保留 PHOTO 等其它字段）
    result = []
    for name_key, card in by_name.items():
        tels = card.pop("_tels", [])
        to_remove = [k for k in card if k.upper().startswith("TEL")]
        for k in to_remove:
            del card[k]
        for i, (_, num) in enumerate(tels):
            key = "TEL;CELL"
            if key not in card:
                card[key] = []
            card[key].append(num)
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


# ---------- 输出 VCF（保留 PHOTO 等字段，长行按 RFC 折行）----------
VCF_LINE_MAX = 75
VCF_FOLD_CONTINUATION = 74


def fold_vcf_line(line: str) -> List[str]:
    """VCF 长行折行：首行最多 75 字符，续行以空格开头、每行最多 74 字符内容。"""
    if len(line) <= VCF_LINE_MAX:
        return [line]
    out = [line[:VCF_LINE_MAX]]
    i = VCF_LINE_MAX
    while i < len(line):
        chunk = line[i : i + VCF_FOLD_CONTINUATION]
        out.append(" " + chunk)
        i += VCF_FOLD_CONTINUATION
    return out


def card_to_vcard_lines_simple(card: Dict) -> List[str]:
    """顺序输出所有字段（含 PHOTO 等），长行自动折行以兼容头像等 BASE64 内容。"""
    out = ["BEGIN:VCARD", "VERSION:2.1"]
    for k, vals in card.items():
        for v in vals:
            if v and not v.isspace():
                line = f"{k}:{v}"
                for folded in fold_vcf_line(line):
                    out.append(folded)
    out.append("END:VCARD")
    return out


def write_vcf(cards: List[Dict], path: Path) -> None:
    """将 VCARD 列表写回 VCF 文件（保留联系人头像等所有字段）。"""
    with open(path, "w", encoding="utf-8") as f:
        for card in cards:
            for line in card_to_vcard_lines_simple(card):
                f.write(line + "\n")


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
        # 不合并时仅做同卡内号码去重（保持原列表）
        for card in cards:
            tels = get_tel_list(card)
            seen = set()
            new_tels = []
            for tk, num in tels:
                d = digits_only(num)
                if d and d not in seen:
                    seen.add(d)
                    new_tels.append((tk, num))
            to_remove = [k for k in card if k.upper().startswith("TEL")]
            for k in to_remove:
                del card[k]
            for _, num in new_tels:
                card.setdefault("TEL;CELL", []).append(num)
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

    out_path = Path(args.output) if args.output else input_path.parent / (input_path.stem + "_fixed.vcf")
    if not out_path.is_absolute():
        out_path = LOG_DIR / out_path
    write_vcf(cards, out_path)
    logger.info(f"已写入: {out_path}")
    return 0


if __name__ == "__main__":
    exit(main() or 0)
