from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import List, Optional

from app.services.agent.utils import TimeRange

logger = logging.getLogger(__name__)

# 全量筛选/排行榜：下游用 ["__ALL__"] 识别，再在 orchestrator 展开为企业宇宙
GLOBAL_RANKING_ENTERPRISE_MARKER = "__ALL__"


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
        # 26 家核心扩展（简称/全称检索）
        "力帆科技",
        "力帆",
        "中汽股份",
        "一汽解放",
        "万向钱潮",
        "东风汽车",
        "东风科技",
        "中国重汽",
        "宇通客车",
        "宇通",
        "江铃汽车",
        "江铃",
        "东安动力",
        "云意电气",
        "京威股份",
        "伯特利",
        "信隆健康",
        "旷达科技",
        "汉马科技",
        "索菱股份",
        "贝斯特",
        "路畅科技",
        "亚星客车",
        "安凯客车",
        "福田汽车",
        "福田",
        "一彬科技",
        "解放",
    ]
    # 别名 -> 规范简称（与 orchestrator 证据展开/排行宇宙一致）；长键优先匹配
    _ENTERPRISE_ALIASES = {
        # 既有车企
        "理想汽车": "理想",
        "长城汽车": "长城",
        "长安汽车": "长安",
        "比亚迪汽车": "比亚迪",
        "比亚迪股份": "比亚迪",
        "BYD": "比亚迪",
        "byd": "比亚迪",
        # 三领域
        "力帆股份": "力帆科技",
        "力帆": "力帆科技",
        "中国汽研": "中汽股份",
        "中汽中心": "中汽股份",
        "中汽": "中汽股份",
        # 财务+销售
        "一汽解放汽车有限公司": "一汽解放",
        "一汽解放": "一汽解放",
        "万向钱潮股份公司": "万向钱潮",
        "万向钱潮": "万向钱潮",
        "万向": "万向钱潮",
        "东风汽车股份": "东风汽车",
        "东风汽车": "东风汽车",
        "中国重汽集团": "中国重汽",
        "中国重汽": "中国重汽",
        "重汽": "中国重汽",
        "宇通客车": "宇通客车",
        "宇通集团": "宇通客车",
        "宇通": "宇通客车",
        "广汽集团": "广汽",
        "广汽": "广汽",
        "江铃汽车": "江铃汽车",
        "江铃": "江铃汽车",
        # 财务+司法
        "哈尔滨东安汽车动力股份有限公司": "东安动力",
        "东安动力": "东安动力",
        "东安": "东安动力",
        "东风电子科技": "东风科技",
        "东风科技股份": "东风科技",
        "东风科技": "东风科技",
        "南京云意电气": "云意电气",
        "云意电气": "云意电气",
        "云意": "云意电气",
        "北京威卡威": "京威股份",
        "京威股份": "京威股份",
        "京威": "京威股份",
        "芜湖伯特利": "伯特利",
        "伯特利": "伯特利",
        "信隆实业": "信隆健康",
        "信隆健康": "信隆健康",
        "信隆": "信隆健康",
        "旷达科技": "旷达科技",
        "旷达": "旷达科技",
        "汉马科技": "汉马科技",
        "汉马": "汉马科技",
        "索菱股份": "索菱股份",
        "索菱": "索菱股份",
        "贝斯特": "贝斯特",
        "路畅科技": "路畅科技",
        "路畅": "路畅科技",
        # 销售+司法
        "亚星客车": "亚星客车",
        "亚星": "亚星客车",
        "安凯客车": "安凯客车",
        "安凯": "安凯客车",
        "福田汽车": "福田汽车",
        "北汽福田": "福田汽车",
        "福田": "福田汽车",
        "一彬科技": "一彬科技",
        "力帆汽车": "力帆科技",
        "解放汽车": "一汽解放",
        "解放": "一汽解放",
        "东风股份": "东风汽车",
    }
    _SENTIMENT_KEYWORDS = ("舆情", "新闻", "口碑", "舆论")

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
    _SIMPLE_METRIC_KEYWORDS = (
        "销量",
        "销售",
        "趋势",
        "走势",
        "变化",
        "增长",
        "下降",
        "营收",
        "收入",
        "净利润",
        "利润",
        "总资产",
        "负债",
        "ROE",
        "roe",
        "流动比率",
    )
    _SIMPLE_METRIC_INTERCEPT_KEYWORDS = (
        "为什么",
        "原因",
        "归因",
        "怎么回事",
        "是什么导致",
    )
    _ANALYTIC_GUARD_KEYWORDS = (
        "为什么",
        "原因",
        "分析",
        "归因",
        "评估",
        "风险",
        "对比",
        "建议",
        "投资",
        "怎么",
        "如何",
    )
    # 与 orchestrator._guess_enterprises_from_question / 全量排行宇宙一致：句子里已出现其中任一名称则视为「已点名企业」，不走 __ALL__
    _KNOWN_ENTERPRISE_SUBSTRINGS_FOR_GUARD = (
        "比亚迪",
        "比亚迪汽车",
        "比亚迪股份",
        "长城汽车",
        "长安汽车",
        "广汽集团",
        "理想汽车",
        "蔚来",
        "上汽集团",
        "宁德时代",
        "特斯拉",
        "长城",
        "长安",
        "广汽",
        "理想",
        "吉利",
        "小鹏",
        "力帆科技",
        "力帆",
        "中汽股份",
        "一汽解放",
        "万向钱潮",
        "万向",
        "东风汽车",
        "东风科技",
        "中国重汽",
        "宇通客车",
        "宇通",
        "江铃汽车",
        "江铃",
        "东安动力",
        "云意电气",
        "京威股份",
        "伯特利",
        "信隆健康",
        "旷达科技",
        "汉马科技",
        "索菱股份",
        "贝斯特",
        "路畅科技",
        "亚星客车",
        "安凯客车",
        "福田汽车",
        "福田",
        "一彬科技",
        "解放",
    )
    _GLOBAL_AGGREGATE_PATTERN = re.compile(
        r"(哪些企业|哪几家企业|哪家公司|哪家企业|谁家|谁\s*的|哪些公司|车企|"
        r"企业有哪些|公司有哪些|有哪些企业|有哪些公司|"
        r"排行榜|榜单|排序|排名|前几|top\s*\d+|TOP\d+|前三|前五|前十|"
        r"最高|最低|最多|最少|靠前|靠后|"
        r"不足|低于|高于|超过|不少于|不多于|未达|不达|"
        r"既有|又有|同时有|同时具备)",
        re.IGNORECASE,
    )
    _GLOBAL_AGGREGATE_METRIC_PATTERN = re.compile(
        r"(ROE|roe|净资产收益率|销量|营收|净利润|诉讼|案件|司法|负债|资产|评分|综合|市值|利润|排名|排序|"
        r"流动比率|资产负债率|毛利率|净利率|偿债|财务压力|司法风险|"
        r"销售数据|司法风险数据)",
        re.IGNORECASE,
    )

    def detect_special_query_type(self, query: str) -> Optional[str]:
        """特殊问法路由（非 LLM）：排行榜补漏、否定筛选、跨域名单、零值解释。"""
        q = (query or "").strip()
        if not q:
            return None
        if re.search(r"(为什么是0|为什么为0|为什么没有数据|为什么为空|为什么没数据|为何为0|为何没有数据)", q):
            return "zero_explain"
        if re.search(
            r"(没有法律诉讼|无法律诉讼|无诉讼记录|不存在\s*诉讼|零诉讼|无司法案件|没有司法案件|没有诉讼记录)",
            q,
        ) and re.search(r"(哪些|哪家|哪几|企业|公司|车企)", q):
            return "negative_filter"
        if re.search(r"(既有|同时有|同时具备|又有).{0,40}(销售|销量).{0,40}(司法|诉讼)", q) or re.search(
            r"(既有|同时有).{0,40}(司法|诉讼).{0,40}(销售|销量)", q
        ):
            if re.search(r"(哪些|哪家|哪几|企业|公司)", q):
                return "cross_domain"
        if re.search(
            r"(最高|最低|最多|最少|排名|排序|前三|前五|前十|靠前|靠后|榜首|垫底)",
            q,
        ) and re.search(r"(车企|企业|公司|零部件|哪些|哪家|哪几|名单)", q) and re.search(
            r"(净利润|营收|销量|ROE|roe|司法|诉讼)", q, flags=re.IGNORECASE
        ):
            return "ranking"
        return None

    def strip_zero_explain_clauses(self, query: str) -> str:
        """去掉零值追问语气，便于剩余片段做实体抽取。"""
        q = (query or "").strip()
        q = re.sub(
            r"(为什么是0|为什么为0|为什么没有数据|为什么为空|为什么没数据|为何为0|为何没有数据|为什么|为啥|呢|？|\?)+",
            " ",
            q,
        )
        return re.sub(r"\s+", " ", q).strip()

    def is_global_aggregate_query(self, query: str) -> bool:
        """排行榜/筛选类：不要求用户先点名企业。"""
        q = (query or "").strip()
        if not q:
            return False
        if not self._GLOBAL_AGGREGATE_PATTERN.search(q):
            return False
        if not self._GLOBAL_AGGREGATE_METRIC_PATTERN.search(q):
            return False
        if any(sub in q for sub in self._KNOWN_ENTERPRISE_SUBSTRINGS_FOR_GUARD):
            return False
        return True

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
        # 极简追问：不得判为闲聊，否则 orchestrator 无法走分析链恢复 session 企业
        _minimal_followups = (
            "为什么",
            "为啥",
            "原因",
            "原因呢",
            "为什么呢",
            "怎么会这样",
            "什么原因",
            "咋回事",
        )
        if raw.strip() in _minimal_followups or (
            len(raw) <= 10
            and bool(
                re.fullmatch(
                    r"(为什么|为啥|原因|原因呢|为什么呢|怎么会这样|什么原因|咋回事)[？?！!….\s]*",
                    raw.strip(),
                    flags=re.IGNORECASE,
                )
            )
        ):
            return "analysis"
        if re.fullmatch(r"(利润|净利润|营收|收入|销量)呢[？?\s]*", raw.strip(), flags=re.IGNORECASE):
            return "analysis"
        if re.fullmatch(
            r"(长城|长安|福田|宇通|重汽|索菱|汉马|江铃|理想|比亚迪|力帆|中汽|广汽)呢[？?\s]*",
            raw.strip(),
            flags=re.IGNORECASE,
        ):
            return "analysis"
        if re.fullmatch(r"(它|她)呢[？?\s]*", raw.strip(), flags=re.IGNORECASE):
            return "analysis"
        if re.search(r"^(它的|她的)(司法|财务|舆情|法律)", raw):
            return "analysis"
        if re.search(r"谁.*(司法风险|更值得投资|更值得买)", raw):
            return "analysis"
        if len(raw) <= 4 and not self.extract_enterprises(raw):
            return "chat"
        return "analysis"

    def extract_enterprises(self, query: str) -> List[str]:
        q = (query or "").strip()
        if not q:
            return []
        if self.is_global_aggregate_query(q):
            return [GLOBAL_RANKING_ENTERPRISE_MARKER]
        mentions: List[str] = []
        # 先按连接词切分片段，再匹配别名（强化「汉马科技和索菱股份」等双主体）
        _q_conn = re.sub(r"^(对比|比较|关于)", "", q)
        for chunk in re.split(r"[和与、,，]+", _q_conn):
            ck = (chunk or "").strip()
            if len(ck) < 2:
                continue
            for alias, canonical in sorted(self._ENTERPRISE_ALIASES.items(), key=lambda kv: len(kv[0]), reverse=True):
                if len(alias) < 2:
                    continue
                if alias == "东风" and "东风科技" in q:
                    continue
                if alias in ck and canonical not in mentions:
                    mentions.append(canonical)
        # 别名优先：长键先匹配，避免「一汽」误吃「一汽解放」等子串冲突
        for alias, canonical in sorted(self._ENTERPRISE_ALIASES.items(), key=lambda kv: len(kv[0]), reverse=True):
            if alias == "东风" and "东风科技" in q:
                continue
            if alias in q and canonical not in mentions:
                mentions.append(canonical)
        for canonical in sorted({c for c in self._ENTERPRISE_ALIASES.values() if c}, key=len, reverse=True):
            if canonical in q and canonical not in mentions:
                mentions.append(canonical)
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
        # Canonicalize and dedupe aliases like "理想/理想汽车" to avoid false multi-enterprise comparison.
        canonical_mentions: List[str] = []
        for m in mentions:
            c = self._canonicalize_enterprise_name(m)
            if c and c not in canonical_mentions:
                canonical_mentions.append(c)
        logger.info("[INTENT] extract_enterprises query=%s mentions=%s canonical=%s", q, mentions, canonical_mentions)
        return canonical_mentions

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
        m22 = re.search(r"(?<![0-9])([12]\d)\s*年", text)
        if m22:
            yy = int(m22.group(1))
            full_year = 2000 + yy
            return TimeRange(kind="year", year=full_year)
        # relative time words -> map to concrete year windows based on calendar year
        now_year = datetime.now().year
        if "去年" in text:
            return TimeRange(kind="year", year=now_year - 1)
        if re.search(r"近\s*两年|近2年", text):
            # represent as LAST_2_YEARS; downstream resolves to concrete years
            return TimeRange(kind="LAST_2_YEARS")
        if re.search(r"近\s*三年|近3年", text):
            return TimeRange(kind="LAST_3_YEARS")
        if re.search(r"(这几年|近些年|多年以来|多年来)", text):
            return TimeRange(kind="LAST_3_YEARS")
        # IMPORTANT: if user didn't provide any explicit/relative time, return None (force clarification)
        return None

    def _contains_multiple_metrics(self, question: str) -> bool:
        """
        是否同时包含两个及以上「可量化核心指标」问法。
        命中则禁止走单指标快路径，改走完整分析链。
        """
        q = (question or "").strip()
        if not q:
            return False
        kinds = 0
        if re.search(r"(销量|销售额|销售量|销售情况)", q) or (
            re.search(r"销售", q) and not re.search(r"(销售费用|销售渠道|销售模式)", q)
        ):
            kinds += 1
        if re.search(r"(营收|营业收入|总收入)", q):
            kinds += 1
        if "净利润" in q or "净利率" in q:
            kinds += 1
        if "毛利润" in q or "毛利率" in q:
            kinds += 1
        if "营业利润" in q:
            kinds += 1
        if "总资产" in q:
            kinds += 1
        if re.search(r"(ROE|roe|净资产收益率)", q, flags=re.IGNORECASE):
            kinds += 1
        if "流动比率" in q or "速动比率" in q:
            kinds += 1
        if "资产负债率" in q or re.search(r"(负债率|有息负债)", q):
            kinds += 1
        elif "资产负债" in q or "资产和负债" in q:
            kinds += 1
        if re.search(r"(诉讼|司法|纠纷|法律风险)", q):
            kinds += 1
        return kinds >= 2

    def is_simple_metric_query(self, query: str) -> bool:
        q = (query or "").strip()
        if not q:
            return False
        if self._contains_multiple_metrics(q):
            return False
        # Cause-analysis follow-ups must go through deep analysis route.
        if any(k in q for k in self._SIMPLE_METRIC_INTERCEPT_KEYWORDS):
            return False
        has_metric = any(k in q for k in self._SIMPLE_METRIC_KEYWORDS)
        has_analytic_guard = any(k in q for k in self._ANALYTIC_GUARD_KEYWORDS)
        return has_metric and not has_analytic_guard

    def is_sentiment_query(self, query: str) -> bool:
        q = (query or "").strip()
        if not q:
            return False
        return any(k in q for k in self._SENTIMENT_KEYWORDS)

    def _canonicalize_enterprise_name(self, name: str) -> str:
        n = (name or "").strip()
        if not n:
            return ""
        if n in self._ENTERPRISE_ALIASES:
            return self._ENTERPRISE_ALIASES[n]
        for alias, canonical in self._ENTERPRISE_ALIASES.items():
            if n == canonical:
                return canonical
            if n.endswith("汽车") and n[:-2] == canonical:
                return canonical
            if n == alias:
                return canonical
        return n

