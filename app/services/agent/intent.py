from __future__ import annotations

import re
from datetime import datetime
from typing import List, Optional

from app.services.agent.utils import TimeRange


class IntentDetector:
    """
    Regex-first intent detector with conservative gibberish blocking.
    """

    _NAMED_ENTERPRISE_TOKENS = [
        "比亚迪",
        "理想",
        "小鹏",
        "蔚来",
        "上汽",
        "广汽",
        "长城",
        "特斯拉",
        "吉利",
        "零跑",
        "极氪",
        "赛力斯",
        "长安",
        "五菱",
        "埃安",
        "小米",
        "宁德时代",
    ]

    _CHAT_PATTERNS = [
        r"^(你好|您好|嗨|hi|hello|在吗|在不在|早上好|晚上好|谢谢|感谢|你是谁|介绍下你自己)",
        r"(聊聊|闲聊|打个招呼)",
        r"^(哈+|呵+|嘿+|嘻+|hh+|233+|lol+)$",
        r"(笑死|哈哈哈|哈哈|离谱|绷不住|太好笑|无语|我服了|行吧|嗯嗯|好滴|好的|ok|okay)",
    ]

    _DECISION_PATTERNS = [
        r"(值得(投资|买吗)?|能不能投|要不要投|买不买|建议(买入|投资)|是否投资|值不值得|加仓|减仓|建仓|回避)",
        r"(该不该|可不可以).*(投资|买入|持有)",
    ]

    _TASK_KEYWORDS = r"(分析|对比|风险|机会|建议|报告|评估|结论|怎么做|怎么看|是否|值不值得|投资|买|卖|加仓|减仓)"

    def is_gibberish(self, query: str) -> bool:
        q = (query or "").strip()
        if not q:
            return True
        if re.search(self._TASK_KEYWORDS, q):
            return False
        if len(q) <= 2:
            return True
        filler = set("嗯哦呃额啊呀哎诶哈呵嘿嘻唉啦嘛呀噢欸")
        filler_count = sum(1 for ch in q if ch in filler)
        if len(q) <= 12 and (filler_count / max(len(q), 1)) >= 0.5:
            return True
        unique_ratio = len(set(q)) / max(len(q), 1)
        if len(q) <= 12 and unique_ratio <= 0.35:
            return True
        return False

    def detect(self, query: str) -> str:
        raw = (query or "").strip()
        text = raw.lower()
        if not text:
            return "analysis"
        if self.is_gibberish(raw):
            return "chat"
        if any(re.search(p, text, flags=re.IGNORECASE) for p in self._CHAT_PATTERNS):
            return "chat"
        if any(re.search(p, text, flags=re.IGNORECASE) for p in self._DECISION_PATTERNS):
            return "decision"
        if len(raw) <= 4 and not self.extract_enterprises(raw):
            return "chat"
        return "analysis"

    def extract_enterprises(self, query: str) -> List[str]:
        q = (query or "").strip()
        if not q:
            return []
        mentions: List[str] = []
        parts = re.split(r"(?:和|与|vs|VS|Vs|、|,|，|\s+)", q)
        for raw in parts:
            token = (raw or "").strip()
            if not token:
                continue
            segs = re.findall(r"([^和与vs、\s,，]{2,12})", token, flags=re.IGNORECASE)
            for seg in segs:
                if seg in {"对比", "比较", "分析", "营收", "盈利能力", "风险", "诉讼", "最近", "新闻", "舆情", "是否值得投资"}:
                    continue
                if seg not in mentions and (
                    seg in self._NAMED_ENTERPRISE_TOKENS
                    or seg.endswith("汽车")
                    or re.fullmatch(r"(?:\d{6}|0\d{4}|TSLA|XPEV|NIO|LI)", seg, flags=re.IGNORECASE)
                ):
                    mentions.append(seg.upper() if re.fullmatch(r"(?:TSLA|XPEV|NIO|LI)", seg, flags=re.IGNORECASE) else seg)
        for token in self._NAMED_ENTERPRISE_TOKENS:
            if token in q and token not in mentions:
                mentions.append(token)
        for m in re.findall(r"([\u4e00-\u9fa5]{2,8}汽车)", q):
            if m not in mentions:
                mentions.append(m)
        for m in re.findall(r"\b(?:\d{6}|0\d{4}|TSLA|XPEV|NIO|LI)\b", q, flags=re.IGNORECASE):
            norm = m.upper()
            if norm not in mentions:
                mentions.append(norm)
        return mentions

    def extract_time_range(self, query: str) -> Optional[TimeRange]:
        text = (query or "").strip()
        if not text:
            return None
        m = re.search(r"(20\d{2})\s*年", text)
        if m:
            return TimeRange(kind="year", year=int(m.group(1)))
        m2 = re.search(r"(20\d{2})", text)
        if m2:
            return TimeRange(kind="year", year=int(m2.group(1)))
        # relative time words -> map to concrete year windows based on calendar year
        now_year = datetime.now().year
        if "去年" in text:
            return TimeRange(kind="year", year=now_year - 1)
        if re.search(r"近\s*两年|近2年", text):
            # represent as LAST_2_YEARS; downstream resolves to concrete years
            return TimeRange(kind="LAST_2_YEARS")
        if re.search(r"近\s*三年|近3年", text):
            return TimeRange(kind="LAST_3_YEARS")
        # IMPORTANT: if user didn't provide any explicit/relative time, return None (force clarification)
        return None

