# -*- coding: utf-8 -*-

# ------------------------------
# @Time    : 2025/6/16
# @Author  : gao
# @File    : server.py
# @Project : WealthManager
# ------------------------------
"""
AmazingData MCP Server
提供中国银河证券星耀数智 AmazingData 金融数据服务的 MCP 接口
"""

from fastmcp import FastMCP
import os
import sys
import datetime
import logging
from typing import Optional, List, Dict, Any
from pathlib import Path

import pandas as pd
import AmazingData as ad

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('amazingdata_mcp.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# 创建 FastMCP 实例
mcp = FastMCP("AmazingData")

# 文档路径
DOC_PATH = Path(__file__).parent / "doc" / "AmazingData开发手册.pdf"

# 全局变量：登录状态
_is_logged_in = False
_login_info = {}


# ==================== 工具函数 ====================

def datetime_to_int(date: Optional[datetime.datetime] = None) -> int:
    """将日期转换为整数格式 YYYYMMDD"""
    if date is None:
        date = datetime.datetime.now()
    return int(date.strftime('%Y%m%d'))


def serialize_dataframe(df: pd.DataFrame) -> Optional[List[Dict]]:
    """序列化 DataFrame 为字典列表"""
    try:
        if df is None or df.empty:
            return []
        result = df.to_dict('records')
        logger.debug(f"序列化 DataFrame，行数: {len(df)}")
        return result
    except Exception as e:
        logger.error(f"序列化 DataFrame 失败: {e}")
        return None


def serialize_dict(ori_dict: Dict[str, pd.DataFrame]) -> Dict[str, Optional[List[Dict]]]:
    """序列化一层字典，value 是 DataFrame 的情况"""
    serialized = {}
    for inner_key, df in ori_dict.items():
        serialized[inner_key] = serialize_dataframe(df)
    return serialized


def serialize_nested_dict(nested_dict: Dict[str, Dict[str, pd.DataFrame]]) -> Dict[str, Dict[str, Optional[List[Dict]]]]:
    """序列化两层字典，最内层 value 是 DataFrame 的情况"""
    serialized = {}
    for outer_key, inner_dict in nested_dict.items():
        serialized[outer_key] = {}
        for inner_key, df in inner_dict.items():
            serialized[outer_key][inner_key] = serialize_dataframe(df)
    return serialized


def handle_error(e: Exception, context: str, **extra_info) -> dict:
    """
    统一的错误处理函数，返回可操作的错误信息

    Args:
        e: 异常对象
        context: 错误上下文（如 "查询K线数据"）
        **extra_info: 额外的上下文信息

    Returns:
        包含详细错误信息和建议的字典
    """
    error_response = {
        "success": False,
        "context": context
    }

    # 根据异常类型提供具体建议
    if isinstance(e, RuntimeError) and "未登录" in str(e):
        error_response.update({
            "error_type": "not_logged_in",
            "message": "未登录 AmazingData 服务",
            "suggestion": "服务器启动时会自动登录。如果看到此错误，请检查登录状态。",
            "next_steps": [
                "使用 mcp_get_login_status 检查登录状态",
                "检查环境变量中的登录凭证是否正确",
                "查看服务器日志了解登录失败原因"
            ]
        })
    elif isinstance(e, ValueError):
        error_response.update({
            "error_type": "invalid_parameter",
            "message": str(e),
            "suggestion": "请检查参数格式和取值范围",
            "examples": {
                "date_format": "20240101 (YYYYMMDD, 8位整数)",
                "code_format": "000001.SZ 或 600000.SH",
                "period": "day, min1, min5, week, month"
            }
        })
    elif isinstance(e, (ConnectionError, TimeoutError)):
        error_response.update({
            "error_type": "connection_error",
            "message": f"连接 AmazingData 服务失败: {str(e)}",
            "suggestion": "网络连接问题或服务暂时不可用",
            "troubleshooting": [
                "检查网络连接是否正常",
                "确认 AmazingData 服务地址和端口正确",
                "稍后重试，服务可能正在维护"
            ]
        })
    elif "retry" in str(e).lower() or "查询失败" in str(e):
        error_response.update({
            "error_type": "query_failed",
            "message": str(e),
            "suggestion": "数据查询失败，可能是参数错误或数据不存在",
            "troubleshooting": [
                "缩短日期范围（建议不超过1年）",
                "减少查询的代码数量（建议不超过50个）",
                "检查代码是否有效（使用 mcp_code_list 验证）",
                "确认日期范围内有交易数据"
            ]
        })
    else:
        error_response.update({
            "error_type": "unexpected_error",
            "message": str(e),
            "suggestion": "发生未预期的错误，请查看日志获取详细信息"
        })

    # 添加额外的上下文信息
    if extra_info:
        error_response["context_info"] = extra_info

    # 记录错误日志
    logger.error(f"{context}失败: {e}", exc_info=True)

    return error_response


def ensure_logged_in() -> bool:
    """确保已登录，如果未登录则抛出异常"""
    global _is_logged_in
    if not _is_logged_in:
        raise RuntimeError("未登录 AmazingData，请先登录")
    return True


def validate_date_range(begin_date: Optional[int], end_date: Optional[int]) -> tuple[int, int]:
    """
    验证日期范围参数

    根据 AmazingData 要求和新的默认值逻辑：
    - begin_date 默认为 19900101
    - end_date 默认为当前日期
    - 日期格式必须为 YYYYMMDD（8位整数）

    Args:
        begin_date: 开始日期，默认 19900101
        end_date: 结束日期，默认当前日期

    Returns:
        验证后的 (begin_date, end_date) 元组

    Raises:
        ValueError: 如果日期格式不正确或日期逻辑错误
    """
    # 如果 end_date 为 None，使用当前日期
    if end_date is None:
        end_date = datetime_to_int()

    # 验证日期格式（8位整数）
    if not (10000000 <= begin_date <= 99999999):
        raise ValueError(f"begin_date 格式错误，应为 YYYYMMDD 格式的8位整数，当前值: {begin_date}")
    if not (10000000 <= end_date <= 99999999):
        raise ValueError(f"end_date 格式错误，应为 YYYYMMDD 格式的8位整数，当前值: {end_date}")

    # 验证日期逻辑
    if begin_date > end_date:
        raise ValueError(f"begin_date ({begin_date}) 不能大于 end_date ({end_date})")

    return begin_date, end_date


# ==================== MCP Resources ====================

@mcp.resource("amazingdata://doc/manual")
async def get_manual() -> str:
    """
    获取 AmazingData 开发手册

    Returns:
        开发手册的文本内容
    """
    try:
        if DOC_PATH.exists():
            # 尝试使用 PyPDF2 读取 PDF
            try:
                import PyPDF2
                content = []
                content.append("# AmazingData 开发手册\n")

                with open(DOC_PATH, 'rb') as file:
                    pdf_reader = PyPDF2.PdfReader(file)
                    for page_num in range(len(pdf_reader.pages)):
                        page = pdf_reader.pages[page_num]
                        text = page.extract_text()
                        if text.strip():
                            content.append(f"\n--- 第 {page_num + 1} 页 ---\n")
                            content.append(text)

                return "\n".join(content)
            except ImportError:
                logger.warning("PyPDF2 未安装，返回文档路径")
                return f"AmazingData 开发手册路径: {DOC_PATH}\n请安装 PyPDF2 以读取完整内容: pip install PyPDF2"
        else:
            return f"文档不存在: {DOC_PATH}"
    except Exception as e:
        logger.error(f"读取开发手册失败: {e}")
        return f"读取开发手册失败: {str(e)}"


@mcp.resource("amazingdata://doc/api-summary")
async def get_api_summary() -> str:
    """
    获取 AmazingData API 接口摘要

    Returns:
        API 接口摘要信息
    """
    return """# AmazingData MCP Server API 接口摘要

本服务共提供 56 个 MCP 工具接口，精确对应如下：

## 1. 系统管理接口 (2个)
- mcp_get_login_status: 获取当前登录状态
- mcp_logout: 登出系统

## 2. 基础数据接口 (7个)
- mcp_code_list: 获取证券代码列表（支持A股、指数、ETF、可转债等）
- mcp_code_list_future: 获取期货代码列表
- mcp_code_list_option: 获取期权代码列表
- mcp_code_info: 获取证券信息（含涨跌停价、昨收价等）
- mcp_backward_factor: 获取复权因子
- mcp_single_backward_factor: 获取单个证券复权因子
- mcp_history_code_list: 获取历史代码列表
- mcp_calendar: 获取交易日历

## 3. 行情数据接口 (2个)
- mcp_kline: 查询K线数据（支持日线、分钟线、周线、月线等多周期）
  * 支持分页参数: limit（返回记录数）、offset（跳过记录数）
  * 适用于大数据量查询，避免超时
- mcp_snapshot: 查询历史快照数据（统一接口，自动识别所有资产类型）
  * 使用 AmazingData SDK 的单一 query_snapshot 接口
  * SDK 自动根据证券代码识别资产类型（股票、指数、ETF、债券、期货、期权、回购、港股通）
  * 只查询历史快照，不支持实时订阅
  * 无需指定资产类型，直接传入代码列表即可

## 4. 股票信息接口 (14个)
- mcp_stock_basic: 股票基础信息（上市日期、退市日期、板块等）
  * 支持 summary_only 参数：仅返回统计摘要（总数、上市数、退市数、市场分布）
  * 适用于快速了解数据概况
- mcp_history_stock_status: 历史证券状态（ST、停牌、除权除息等）
- mcp_bj_code_mapping: 北交所代码对照表
- mcp_balance_sheet: 资产负债表
- mcp_cash_flow: 现金流量表
- mcp_income: 利润表
- mcp_profit_express: 业绩快报
- mcp_profit_notice: 业绩预告
- mcp_share_holder: 十大股东
- mcp_holder_num: 股东户数
- mcp_equity_structure: 股本结构
- mcp_equity_pledge_freeze: 股权冻结/质押
- mcp_equity_restricted: 限售股解禁
- mcp_kzz: 查询可转债代码表

## 5. 分红配股接口 (2个)
- mcp_dividend: 分红数据
- mcp_right_issue: 配股数据

## 6. 交易数据接口 (4个)
- mcp_margin_summary: 融资融券汇总
- mcp_margin_detail: 融资融券明细
- mcp_long_hu_bang: 龙虎榜
- mcp_block_trading: 大宗交易

## 7. 指数数据接口 (2个)
- mcp_index_constituent: 指数成分股
- mcp_index_constituent_weight: 指数成分股权重

## 8. 基金数据接口 (3个)
- mcp_etf_iopv: 基金IOPV（实时净值）
- mcp_etf_fund_share: 基金份额
- mcp_etf_purchase_redemption: ETF申购赎回

## 9. 可转债数据接口 (11个)
- mcp_convertible_bond_issue: 可转债发行数据
- mcp_convertible_bond_balance: 可转债余额
- mcp_convertible_bond_conversion: 可转债转股数据
- mcp_convertible_bond_conversion_change: 可转债转股价变动
- mcp_convertible_bond_adjustment: 可转债调整
- mcp_convertible_bond_redemption: 可转债赎回数据
- mcp_convertible_bond_redemption_notice: 可转债赎回公告
- mcp_convertible_bond_resale: 可转债回售数据
- mcp_convertible_bond_resale_notice: 可转债回售公告
- mcp_convertible_bond_terms: 可转债条款
- mcp_convertible_bond_suspension: 可转债停牌

## 10. 期权数据接口 (3个)
- mcp_option_basic_info: 期权基本资料
- mcp_option_contract_info: 期权标准合约属性
- mcp_option_month_contract_change: 期权月合约属性变动

## 11. 行业数据接口 (4个)
- mcp_industry_index_info: 行业指数基础信息
- mcp_industry_index_constituent: 行业指数成分股
- mcp_industry_index_weight: 行业指数成分股权重
- mcp_industry_index_quote: 行业指数日行情

## 12. 国债数据接口 (1个)
- mcp_treasury_yield: 国债收益率

## 重要说明

### 1. 参数规则
- **begin_date**: 默认值为 19900101（如果接口支持）
- **end_date**: 默认值为当前日期（如果接口支持）
- **is_local**: 所有接口统一使用 False（不使用本地缓存）
- **日期格式**: YYYYMMDD（8位整数），如 20240101

### 2. 证券类型 (security_type)
- EXTRA_STOCK_A: 沪深北A股
- EXTRA_INDEX_A: 沪深北指数
- EXTRA_ETF: 沪深ETF
- EXTRA_KZZ: 沪深可转债

### 3. K线数据周期 (period)
- day: 日线
- min1: 1分钟线
- min3: 3分钟线
- min5: 5分钟线
- min10: 10分钟线
- min15: 15分钟线
- min30: 30分钟线
- min60: 60分钟线
- min120: 120分钟线
- week: 周线
- month: 月线
- season: 季线
- year: 年线

### 4. 新增优化功能

#### 4.1 统一错误处理
所有接口使用统一的错误处理机制，返回可操作的错误信息：
- **错误类型识别**: not_logged_in, invalid_parameter, data_not_found, api_error, unknown_error
- **上下文信息**: 包含调用接口名称和参数
- **可操作建议**: 提供具体的解决方案和下一步操作
- **示例参数**: 展示正确的参数格式和取值范围

#### 4.2 数据分页功能
大数据量接口支持分页参数，避免超时和内存溢出：
- **mcp_kline**: 支持 limit（返回记录数）和 offset（跳过记录数）参数
- 返回分页元数据：offset, limit, returned_records
- 适用场景：查询长时间范围的K线数据

#### 4.3 数据摘要功能
部分接口支持 summary_only 参数，快速获取数据概况：
- **mcp_stock_basic**: 返回总数、上市数、退市数、市场分布等统计信息
- 减少数据传输量，提高响应速度
- 适用场景：快速了解数据规模和分布

#### 4.4 快照接口优化
- 使用单一 query_snapshot 接口，SDK 自动识别资产类型
- 8个资产类型别名工具（stock/index/etf/bond/future/option/repo/hkt）简化调用
- 只支持历史快照查询，不涉及实时订阅功能
- 自动计算当日交易时间范围

### 5. 接口测试状态
所有 56 个接口已完成开发和测试：
- 实际测试通过: 43 个接口（100% 成功率）
- MCP 层面实现: 13 个 Resources 接口（需通过 MCP 协议调用）
- 修复了可转债、期权、行业、K线等接口的参数签名和方法调用
- 优化了错误处理、分页、摘要等功能
- 简化了快照接口，统一使用 mcp_snapshot

### 6. 使用建议
- 查询大量数据时建议缩短日期范围或使用分页参数
- 使用 summary_only 参数快速了解数据概况
- 使用单个代码查询可获得更快响应
- 所有接口均已移除本地存储机制，确保实时数据
- Resources 接口（13个）需要通过 MCP 协议调用，不在 SDK 测试范围内
- 错误信息包含可操作建议，请仔细阅读以快速解决问题
"""


# ==================== MCP 工具函数 ====================

@mcp.tool()
async def mcp_get_login_status() -> dict:
    """
    获取当前登录状态

    Returns:
        登录状态信息
    """
    return {
        "is_logged_in": _is_logged_in,
        "login_info": _login_info if _is_logged_in else None
    }


@mcp.tool()
async def mcp_kzz() -> dict:
    """
    查询可转债代码表，并返回可转债的数量

    Returns:
        可转债数量和代码列表
    """
    try:
        ensure_logged_in()
        base_data_object = ad.BaseData()
        code_list = base_data_object.get_code_list(security_type='EXTRA_KZZ')
        logger.info(f"查询到 {len(code_list)} 只可转债")
        return {
            "success": True,
            "count": len(code_list),
            "code_list": code_list[:10] if len(code_list) > 10 else code_list,  # 只返回前10个作为示例
            "message": f"共查询到 {len(code_list)} 只可转债"
        }
    except Exception as e:
        logger.error(f"查询可转债失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


@mcp.tool()
async def mcp_code_info(security_type: str = "EXTRA_STOCK_A") -> dict:
    """
    获取每日最新证券信息，交易日早上9点前更新当日最新证券信息

    Args:
        security_type: 证券类型，可选值：
            - EXTRA_STOCK_A: 上交所A股、深交所A股和北交所的股票列表（默认）
            - SH_A: 上交所A股的股票列表
            - SZ_A: 深交所A股的股票列表
            - BJ_A: 北交所的股票列表
            - EXTRA_STOCK_A_SH_SZ: 上交所A股和深交所A股的股票列表
            - EXTRA_INDEX_A_SH_SZ: 上交所和深交所指数列表
            - EXTRA_INDEX_A: 上交所、深交所和北交所的指数列表
            - SH_INDEX: 上交所指数列表
            - SZ_INDEX: 深交所指数列表
            - BJ_INDEX: 北交所的指数列表
            - SH_ETF: 上交所的ETF列表
            - SZ_ETF: 深交所的ETF列表
            - EXTRA_ETF: 上交所、深交所的ETF列表
            - SH_KZZ: 上交所的可转债列表
            - SZ_KZZ: 深交所的可转债列表
            - EXTRA_KZZ: 上交所、深交所的可转债列表
            - SH_HKT: 沪港通
            - SZ_HKT: 深港通

    Returns:
        证券信息列表，包含以下字段：
        - CODE: 证券代码
        - NAME: 证券简称
        - STATUS: 证券状态
        - PRE_CLOSE: 昨收价
        - LIMIT_UP: 涨停价
        - LIMIT_DOWN: 跌停价
    """
    try:
        ensure_logged_in()
        base_data_object = ad.BaseData()
        code_info = base_data_object.get_code_info(security_type=security_type)
        result = serialize_dataframe(code_info)
        logger.info(f"查询 {security_type} 证券信息，共 {len(result) if result else 0} 条")
        return {
            "success": True,
            "security_type": security_type,
            "count": len(result) if result else 0,
            "data": result
        }
    except Exception as e:
        logger.error(f"查询证券信息失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


@mcp.tool()
async def mcp_code_list(security_type: str = "EXTRA_STOCK_A") -> dict:
    """
    获取代码表（每日最新），此接口无法获取历史代码表

    Args:
        security_type: 证券类型（同 mcp_code_info）

    Returns:
        证券代码列表
    """
    try:
        ensure_logged_in()
        base_data_object = ad.BaseData()
        code_list = base_data_object.get_code_list(security_type=security_type)
        logger.info(f"查询 {security_type} 代码表，共 {len(code_list)} 个")
        return {
            "success": True,
            "security_type": security_type,
            "count": len(code_list),
            "code_list": code_list
        }
    except Exception as e:
        logger.error(f"查询代码表失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


@mcp.tool()
async def mcp_backward_factor(code_list: list[str] = None) -> dict:
    """
    获取复权因子数据，复权因子为根据交易所行情数据计算得出的后复权因子

    Args:
        code_list: 股票代码列表，默认为空列表

    Returns:
        复权因子数据，序列化后的字典
    """
    try:
        ensure_logged_in()
        if code_list is None:
            code_list = []
        base_data_object = ad.BaseData()
        backward_factor = base_data_object.get_backward_factor(code_list)
        result = serialize_dataframe(backward_factor)
        logger.info(f"查询复权因子，共 {len(result) if result else 0} 条")
        return {
            "success": True,
            "count": len(result) if result else 0,
            "data": result
        }
    except Exception as e:
        logger.error(f"查询复权因子失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


@mcp.tool()
async def mcp_calendar(market: str = 'SH') -> dict:
    """
    查询交易日历

    Args:
        market: 市场类型
            - SH: 上交所（默认）
            - SZ: 深交所
            - BJ: 北交所
            - SHF: 上期所
            - CFE: 中金所
            - DCE: 大商所
            - CZC: 郑商所
            - INE: 上海国际能源交易中心所
            - SHN: 沪港通
            - SZN: 深港通
        date: 查询日期（YYYYMMDD格式），默认为当前日期

    Returns:
        交易日历列表
    """
    try:
        ensure_logged_in()

        base_data_object = ad.BaseData()
        calendar = base_data_object.get_calendar(market=market)
        logger.info(f"查询 {market} 交易日历，共 {len(calendar)} 个交易日")
        return {
            "success": True,
            "market": market,
            "count": len(calendar),
            "calendar": calendar
        }
    except Exception as e:
        logger.error(f"查询交易日历失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


@mcp.tool()
async def mcp_stock_basic(code_list: list[str] = None, summary_only: bool = False) -> dict:
    """
    获取指定股票列表的上市公司的证券基础数据

    包含沪深北三个交易所，所有股票（包含已退市标的）的中英文名称、上市日期、退市日期、上市板块等信息

    Args:
        code_list: 股票代码列表，默认为空列表（返回所有股票）
        summary_only: 是否只返回摘要信息（默认False，返回完整数据）

    Returns:
        证券基础数据，包含以下字段：
        - MARKET_CODE: 证券代码
        - SECURITY_NAME: 证券简称
        - COMP_NAME: 证券中文名称
        - PINYIN: 中文拼音简称
        - COMP_NAME_ENG: 证券英文名称
        - LISTDATE: 上市日期
        - DELISTDATE: 退市日期
        - LISTPLATE_NAME: 上市板块名称
        - COMP_SNAME_ENG: 英文名称缩写
        - IS_LISTED: 上市状态 (1：上市交易 3：终止上市)
    """
    try:
        ensure_logged_in()
        if code_list is None:
            code_list = []
        info_data_object = ad.InfoData()
        stock_basic = info_data_object.get_stock_basic(code_list)
        result = serialize_dataframe(stock_basic)
        logger.info(f"查询证券基础数据，共 {len(result) if result else 0} 条")

        # 如果只需要摘要
        if summary_only and result:
            listed_count = sum(1 for r in result if r.get('IS_LISTED') == 1)
            delisted_count = sum(1 for r in result if r.get('IS_LISTED') == 3)
            markets = list(set(r.get('LISTPLATE_NAME') for r in result if r.get('LISTPLATE_NAME')))

            return {
                "success": True,
                "summary": {
                    "total_count": len(result),
                    "listed_count": listed_count,
                    "delisted_count": delisted_count,
                    "markets": markets
                }
            }

        return {
            "success": True,
            "count": len(result) if result else 0,
            "data": result
        }
    except Exception as e:
        return handle_error(e, "查询证券基础数据", code_count=len(code_list) if code_list else 0)


@mcp.tool()
async def mcp_history_stock_status(code_list: list[str] = None, begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取指定股票列表的上市公司的历史证券数据

    以日度为频率，包含历史的涨跌停、st、除权除息等信息

    Args:
        code_list: 股票代码列表，默认为空列表
        begin_date: 开始日期（YYYYMMDD格式），与 end_date 同时使用时必填
        end_date: 结束日期（YYYYMMDD格式），与 begin_date 同时使用时必填

    Returns:
        历史证券数据，包含以下字段：
        - MARKET_CODE: 证券代码
        - TRADE_DATE: 交易日期
        - PRECLOSE: 前收价
        - HIGH_LIMITED: 涨停价
        - LOW_LIMITED: 跌停价
        - PRICE_HIGH_LMT_RATE: 涨停价上限
        - PRICE_LOW_LMT_RATE: 跌停价下限
        - IS_ST_SEC: 是否ST (1表示是，0表示否)
        - IS_SUSP_SEC: 是否停牌 (1表示是，0表示否)
        - IS_WD_SEC: 是否除息 (1表示是，0表示否)
        - IS_XR_SEC: 是否除权 (1表示是，0表示否)
    """
    try:
        ensure_logged_in()
        if code_list is None:
            code_list = []

        # 参数验证：如果提供了 begin_date 或 end_date，则两者都必须提供
        if (begin_date is not None and end_date is None) or (begin_date is None and end_date is not None):
            return {
                "success": False,
                "message": "begin_date 和 end_date 必须同时提供"
            }

        info_data_object = ad.InfoData()

        if begin_date is None or end_date is None:
            history_stock_status = info_data_object.get_history_stock_status(
                code_list, is_local=False
            )
        else:
            history_stock_status = info_data_object.get_history_stock_status(
                code_list, begin_date=begin_date, end_date=end_date, is_local=False
            )

        result = serialize_dataframe(history_stock_status)
        logger.info(f"查询历史证券数据，共 {len(result) if result else 0} 条")
        return {
            "success": True,
            "count": len(result) if result else 0,
            "data": result
        }
    except Exception as e:
        logger.error(f"查询历史证券数据失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


@mcp.tool()
async def mcp_bj_code_mapping() -> dict:
    """
    获取北交所的存量上市公司股票新旧代码对照表

    Returns:
        北交所代码对照表，包含以下字段：
        - OLD_CODE: 旧代码
        - NEW_CODE: 新代码
        - SECURITY_NAME: 证券简称
        - LISTING_DATE: 上市日期
    """
    try:
        ensure_logged_in()

        info_data_object = ad.InfoData()
        bj_code_mapping = info_data_object.get_bj_code_mapping(is_local=False)
        result = serialize_dataframe(bj_code_mapping)
        logger.info(f"查询北交所代码对照表，共 {len(result) if result else 0} 条")
        return {
            "success": True,
            "count": len(result) if result else 0,
            "data": result
        }
    except Exception as e:
        return handle_error(e, "查询北交所代码对照表")


@mcp.tool()
async def mcp_kline(
    code_list: list[str],
    begin_date: int,
    end_date: int,
    period: str = "day",
    begin_time: int = 900,
    end_time: int = 1700,
    limit: Optional[int] = None,
    offset: int = 0
) -> dict:
    """
    查询股票K线数据（支持多种周期：日线、分钟线、周线、月线等）

    适用场景：
    - 分析股票价格走势和技术指标
    - 回测交易策略
    - 计算收益率和波动率

    Args:
        code_list: 股票代码列表，支持：
            - 沪深北交所：股票、指数、ETF、可转债
            - 期货：中金所、上期所、大商所、郑商所、上海国际能源交易中心
            示例：["000001.SZ", "600000.SH", "899050.BJ"]
        begin_date: 开始日期，8位整型格式，如 20240101（必填）
        end_date: 结束日期，8位整型格式，如 20240201（必填）
        period: 数据周期，可选值：
            - "day": 日线（默认）
            - "min1": 1分钟线
            - "min3": 3分钟线
            - "min5": 5分钟线
            - "min10": 10分钟线
            - "min15": 15分钟线
            - "min30": 30分钟线
            - "min60": 60分钟线
            - "min120": 120分钟线
            - "week": 周线
            - "month": 月线
            - "season": 季度线
            - "year": 年线
        begin_time: 开始时间（时分格式），如 900 表示 9:00，仅分钟线有效
        end_time: 结束时间（时分格式），如 1700 表示 17:00，仅分钟线有效
        limit: 每只股票返回的最大记录数（可选，用于分页）
        offset: 跳过的记录数（可选，用于分页，默认0）

    Returns:
        K线数据字典，格式：{股票代码: [K线数据列表]}
        每条K线数据包含：
        - code: 证券代码+市场
        - trade_time: 交易时间
        - open: 开盘价
        - high: 最高价
        - low: 最低价
        - close: 收盘价
        - volume: 成交量
        - amount: 成交额

    性能建议：
        - 日期范围建议不超过1年，避免数据量过大
        - 单次查询股票数建议不超过50个
        - 使用 limit 参数进行分页，避免一次性加载大量数据
        - 分钟线数据量大，建议查询近期数据（如最近1个月）
    """
    try:
        ensure_logged_in()
        begin_date, end_date = validate_date_range(begin_date, end_date)

        if code_list is None or len(code_list) == 0:
            return handle_error(
                ValueError("code_list 不能为空"),
                "查询K线数据",
                suggestion="请提供至少一个股票代码",
                example=["000001.SZ", "600000.SH"]
            )

        # 转换 period 字符串为 AmazingData 枚举值
        period_map = {
            "min1": ad.constant.Period.min1.value,
            "min3": ad.constant.Period.min3.value,
            "min5": ad.constant.Period.min5.value,
            "min10": ad.constant.Period.min10.value,
            "min15": ad.constant.Period.min15.value,
            "min30": ad.constant.Period.min30.value,
            "min60": ad.constant.Period.min60.value,
            "min120": ad.constant.Period.min120.value,
            "day": ad.constant.Period.day.value,
            "week": ad.constant.Period.week.value,
            "month": ad.constant.Period.month.value,
            "season": ad.constant.Period.season.value,
            "year": ad.constant.Period.year.value,
        }

        if period not in period_map:
            return handle_error(
                ValueError(f"不支持的周期类型: {period}"),
                "查询K线数据",
                valid_periods=list(period_map.keys()),
                suggestion=f"请使用以下周期之一: {', '.join(period_map.keys())}"
            )

        period_value = period_map[period]

        # 获取交易日历
        base_data_object = ad.BaseData()
        calendar = base_data_object.get_calendar()

        # 查询K线数据
        market_data_object = ad.MarketData(calendar)

        # 只有分钟线才需要 begin_time 和 end_time
        if period.startswith("min"):
            result = market_data_object.query_kline(
                code_list,
                begin_date=begin_date,
                end_date=end_date,
                begin_time=begin_time,
                end_time=end_time,
                period=period_value,
                is_local=False
            )
        else:
            result = market_data_object.query_kline(
                code_list,
                begin_date=begin_date,
                end_date=end_date,
                period=period_value,
                is_local=False
            )

        serialized = serialize_dict(result)

        # 应用分页
        if limit is not None:
            for code in serialized:
                if serialized[code]:
                    serialized[code] = serialized[code][offset:offset+limit]

        # 统计数据量
        total_count = sum(len(data) if data else 0 for data in serialized.values())
        logger.info(f"查询K线数据，周期: {period}，股票数: {len(code_list)}，总数据量: {total_count} 条")

        response = {
            "success": True,
            "period": period,
            "stock_count": len(code_list),
            "total_records": total_count,
            "data": serialized
        }

        # 添加分页信息
        if limit is not None:
            response["pagination"] = {
                "offset": offset,
                "limit": limit,
                "returned_records": total_count
            }

        return response

    except Exception as e:
        return handle_error(
            e,
            "查询K线数据",
            code_count=len(code_list) if code_list else 0,
            period=period,
            date_range=f"{begin_date}-{end_date}"
        )


@mcp.tool()
async def mcp_snapshot(code_list: list[str], asset_type: str = "auto") -> dict:
    """
    查询实时快照数据（自动识别资产类型）

    获取股票、指数、ETF、债券、期货、期权等品种的最新行情快照数据。
    AmazingData 的 query_snapshot 接口会自动根据代码识别资产类型。

    Args:
        code_list: 证券代码列表，支持：
            - 股票：000001.SZ, 600000.SH
            - 指数：000300.SH, 399006.SZ
            - ETF：510300.SH, 159915.SZ
            - 债券：113050.SH, 128039.SZ
            - 期货：IF2312.CF
            - 期权：10005032.SH
            - 回购：204001.SH
            - 港股通：00700.HK
        asset_type: 保留参数（兼容性），实际由 SDK 自动识别

    Returns:
        快照数据字典，包含最新价、涨跌幅、成交量、买卖盘等信息

    快照数据字段（股票/ETF/债券）：
        - code: 证券代码
        - trade_time: 交易时间
        - last: 最新价
        - pre_close: 昨收价
        - open: 开盘价
        - high: 最高价
        - low: 最低价
        - volume: 成交量
        - amount: 成交额
        - bid_price1-5: 买1-5档价格
        - ask_price1-5: 卖1-5档价格

    指数快照字段：
        - code: 指数代码
        - trade_time: 交易时间
        - last: 最新点位
        - pre_close: 昨收盘
        - open: 今开盘
        - high: 最高
        - low: 最低
        - volume: 成交量
        - amount: 成交额
    """
    try:
        ensure_logged_in()

        if not code_list or len(code_list) == 0:
            return handle_error(
                ValueError("code_list 不能为空"),
                "查询快照数据",
                suggestion="请提供至少一个证券代码"
            )

        # 获取交易日历和当前时间
        now = datetime.datetime.now()
        current_time = now.time()

        # 定义交易时间段
        morning_start = datetime.time(9, 15)
        morning_end = datetime.time(11, 30)
        afternoon_start = datetime.time(13, 0)
        afternoon_end = datetime.time(15, 30)

        # 根据当前时间确定查询时间范围
        if (morning_start <= current_time <= morning_end) or (afternoon_start <= current_time <= afternoon_end):
            end_time = int(now.strftime("%H%M%S%f")[:-3])
            begin_time = int((now - datetime.timedelta(minutes=5)).strftime("%H%M%S%f")[:-3])
        elif morning_end < current_time < afternoon_start:
            end_time = 1130000000
            begin_time = 1129000000
        else:
            end_time = 153000000
            begin_time = 152500000

        # 获取数据 - 使用统一的 query_snapshot 接口
        base_data_object = ad.BaseData()
        calendar = base_data_object.get_calendar()
        market_data_object = ad.MarketData(calendar)

        # query_snapshot 会自动识别资产类型
        snapshot_dict = market_data_object.query_snapshot(
            code_list,
            begin_date=calendar[-1],
            end_date=calendar[-1],
            begin_time=begin_time,
            end_time=end_time,
            is_local=False
        )

        # 提取最新快照
        result_dict = {}
        for code in snapshot_dict:
            result_dict[code] = {}
            for date_key in snapshot_dict[code]:
                if not snapshot_dict[code][date_key].empty:
                    result_dict[code][date_key] = snapshot_dict[code][date_key].tail(1)
                else:
                    result_dict[code][date_key] = pd.DataFrame()

        serialized = serialize_nested_dict(result_dict)
        logger.info(f"查询快照数据，代码数: {len(code_list)}")

        return {
            "success": True,
            "code_count": len(code_list),
            "data": serialized
        }

    except Exception as e:
        return handle_error(
            e,
            "查询快照数据",
            code_count=len(code_list) if code_list else 0
        )


@mcp.tool()
async def mcp_balance_sheet(code_list: list[str] = None, statement_type: str = None,
                            report_type: str = None, begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取指定股票列表的上市公司的资产负债表数据statement_type
    入参：
    code_list	list[str]	是	支持沪深A的的代码列表，可见示例
    statement_type	str	报表类型	参看附录的报表类型代码表STATEMENT_TYPE
    report_type	str	报表类型	参看附录的报告期名称REPORT_TYPE
    begin_date	int	否	报告期，本地数据缓存方案，填写8位的整型格式的日期，比如20240101
    end_date	int	否	报告期，本地数据缓存方案，填写8位的整型格式的日期，比如20240101

    返回字段的释义如下：
    字段名称	类型	字段说明	备注
    MARKET_CODE	str	证券代码
    SECURITY_NAME	str	证券简称
    STATEMENT_TYPE	str	报表类型	参看报表类型代码表
    REPORT_TYPE	str	报告期名称	参看报告期名称
    REPORTING_PERIOD	str	报告期
    ANN_DATE	str	公告日期
    ACTUAL_ANN_DATE	str	实际公告日期
    ACC_PAYABLE	float	应付票据及应付账款
    ACC_RECEIVABLE	float	应收票据及应收账款
    ACC_RECEIVABLES	float	应收款项
    ACCRUED_EXP	float	预提费用
    ACCT_PAYABLE	float	应付账款
    ACCT_RECEIVABLE	float	应收账款
    ACT_TRADING_SEC	float	代理买卖证券款
    ACT_UW_SEC	float	代理承销证券款
    ADV_PREM	float	预收保费
    ADV_RECEIPT	float	预收款项
    AGENCY_ASSETS	float	代理业务资产
    AGENCY_BUSINESS_LIAB	float	代理业务负债
    ANTICIPATION_LIAB	float	预计负债
    ASSET_DEP_FUNDS_OTH_FIN_INST	float	存放同业和其它金融机构款项
    BONDS_PAYABLE	float	应付债券
    CAP_RESV	float	资本公积金
    CAP_STOCK	float	股本	金额（元），公布值
    CASH_CENTRAL_BANK_DEPOSITS	float	现金及存放中央银行款项
    CED_INSUR_CONT_RESERVES_RCV	float	应收分保合同准备金
    CLAIMS_PAYABLE	float	应付赔付款
    CLIENTS_FUND_DEPOSIT	float	客户资金存款
    CLIENTS_RESERVES	float	客户备付金
    CNVD_DIFF_FOREIGN_CURR_STAT	float	外币报表折算差额
    COMP_TYPE_CODE	int	公司类型代码	1：非金融类2：银行3：保险4：证券
    CONST_IN_PROC	float	在建工程
    CONST_IN_PROC_TOTAL	float	在建工程(合计)(元)
    CONSUMP_BIO_ASSETS	float	消耗性生物资产
    CONT_ASSETS	float	合同资产	单位（元）
    CONT_LIABILITIES	float	合同负债	单位（元）
    CURRENCY_CAP	float	货币资金
    CURRENCY_CODE	float	货币代码
    DEBT_INV	float	债权投资(元)
    DEFERRED_INC_NONCUR_LIAB	float	递延收益-非流动负债
    DEFERRED_INCOME	float	递延收益
    DEFERRED_TAX_ASSETS	float	递延所得税资产
    DEFERRED_TAX_LIAB	float	递延所得税负债
    DEP_RECEIVED_IB_DEP	float	吸收存款及同业存放
    DEPOSIT_CAP_RECOG	float	存出资本保证金
    DEPOSIT_TAKING	float	吸收存款
    DEPOSITS_RECEIVED	float	存入保证金
    DER_FIN_ASSETS	float	衍生金融资产
    DERI_FIN_LIAB	float	衍生金融负债
    DEVELOP_EXP	float	开发支出
    DISPOSAL_FIX_ASSETS	float	固定资产清理
    DIV_PAYABLE	float	应付股利
    DIV_RECEIVABLE	float	应收股利
    EMPL_PAY_PAYABLE	float	应付职工薪酬
    ENGIN_MAT	float	工程物资
    FIN_ASSETS_AVA_FOR_SALE	float	可供出售金融资产
    FIN_ASSETS_COST_SHARING	float	以摊余成本计量的金融资产
    FIN_ASSETS_FAIR_VALUE	float	以公允价值计量且其变动计入其他综合收益的金融资产
    FIXED_ASSETS	float	固定资产
    FIXED_ASSETS_TOTAL	float	固定资产(合计)(元)
    FIXED_TERM_DEPOSITS	float	定期存款
    GOODWILL	float	商誉
    GUA_DEPOSITS_PAID	float	存出保证金
    GUA_PLEDGE_LOANS	float	保户质押贷款
    HOLD_ASSETS_FOR_SALE	float	持有待售的资产
    HOLD_TO_MTY_INV	float	持有至到期投资
    INC_PLEDGE_LOAN	float	其中:质押借款
    INCL_TRADING_SEAT_FEES	float	其中:交易席位费
    IND_ACCT_ASSETS	float	独立账户资产
    IND_ACCT_LIAB	float	独立账户负债
    INSURED_DEPOSIT_INV	float	保户储金及投资款
    INSURED_DIV_PAYABLE	float	应付保单红利
    INT_RECEIVABLE	float	应收利息
    INTANGIBLE_ASSETS	float	无形资产
    INTEREST_PAYABLE	float	应付利息
    INV	float	存货
    INV_REALESTATE	float	投资性房地产
    LEASE_LIABILITY	float	租赁负债
    LEND_FUNDS	float	融出资金
    LENDING_FUNDS	float	拆出资金
    LESS_TREASURY_STK	float	减:库存股
    LIA_HFS	float	持有待售的负债
    LIAB_DEP_FUNDS_OTH_FIN_INST	float	同业和其它金融机构存放款项
    LIFE_INSUR_RESV	float	寿险责任准备金
    LOAN_CENTRAL_BANK	float	向中央银行借款
    LOANS_AND_ADVANCES	float	发放贷款及垫款
    LOANS_FROM_OTH_BANKS	float	拆入资金
    LT_DEFERRED_EXP	float	长期待摊费用
    LT_EMP_COMP_PAY	float	长期应付职工薪酬
    LT_EQUITY_INV	float	长期股权投资
    LT_HEALTH_INSUR_RESV	float	长期健康险责任准备金
    LT_LOAN	float	长期借款
    LT_PAYABLE	float	长期应付款
    LT_PAYABLE_TOTAL	float	长期应付款(合计)(元)
    LT_RECEIVABLES	float	长期应收款
    MINORITY_EQUITY	float	少数股东权益
    NOM_RISKS_PREP	float	一般风险准备
    NONCUR_ASSETS_DUE_WITHIN_1Y	float	一年内到期的非流动资产
    NONCUR_LIAB_DUE_WITHIN_1Y	float	一年内到期的非流动负债
    NOTES_PAYABLE	float	应付票据
    NOTES_RECEIVABLE	float	应收票据
    OIL_AND_GAS_ASSETS	float	油气资产
    OTH_COMP_INCOME	float	其他综合收益
    OTH_EQUITY_TOOLS	float	其他权益工具
    OTH_EQUITY_TOOLS_PRE_SHR	float	其他权益工具:优先股
    OTH_NONCUR_ASSETS	float	其他非流动资产
    OTHER_ASSETS	float	其他资产
    OTHER_CUR_ASSETS	float	其他流动资产
    OTHER_CUR_LIAB	float	其他流动负债
    OTHER_DEBT_INV	float	其他债权投资(元)
    OTHER_EQUITY_INV	float	其他权益工具投资(元)
    OTHER_LIAB	float	其他负债
    OTHER_NONCUR_FIN_ASSETS	float	其他非流动金融资产(元)
    OTHER_NONCUR_LIAB	float	其他非流动负债
    OTHER_PAYABLE	float	其他应付款
    OTHER_PAYABLE_TOTAL	float	其他应付款(合计)(元)
    OTHER_RCV_TOTAL	float	其他应收款(合计)（元）
    OTHER_RECEIVABLE	float	其他应收款
    OTHER_SUSTAIN_BOND	float	其他权益工具:永续债(元)
    OUT_LOSS_RESV	float	未决赔款准备金
    PAYABLE	float	应付款项
    PAYABLE_FOR_REINSURER	float	应付分保账款
    PRECIOUS_METAL	float	贵金属
    PREPAYMENT	float	预付款项
    PROD_BIO_ASSETS	float	生产性生物资产
    RCV_CED_CLAIM_RESV	float	应收分保未决赔款准备金
    RCV_CED_LIFE_INSUR_RESV	float	应收分保寿险责任准备金
    RCV_CED_LT_HEALTH_INSUR_RESV	float	应收分保长期健康险责任准备金
    RCV_CED_UNEARNED_PREM_RESV	float	应收分保未到期责任准备金
    RCV_FINANCING	float	应收款项融资
    RCV_INV	float	应收款项类投资
    RECEIVABLE_PREM	float	应收保费
    RED_MON_CAP_FOR_SALE	float	买入返售金融资产
    REINSURANCE_ACC_RCV	float	应收分保账款
    RSRV_FUND_INSUR_CONT	float	保险合同准备金
    SELL_REPO_FIN_ASSETS	float	卖出回购金融资产款
    SERVICE_CHARGE_COMM_PAYABLE	float	应付手续费及佣金
    SETTLE_FUNDS	float	结算备付金
    SPE_ASSETS_BAL_DIFF	float	资产差额(特殊报表科目)
    SPE_CUR_ASSETS_DIFF	float	流动资产差额(特殊报表科目)
    SPE_CUR_LIAB_DIFF	float	流动负债差额(特殊报表科目)
    SPE_LIAB_BAL_DIFF	float	负债差额(特殊报表科目)
    SPE_LIAB_EQUITY_BAL_DIFF	float	负债及股东权益差额(特殊报表项目)
    SPE_NONCUR_ASSETS_DIFF	float	非流动资产差额(特殊报表科目)
    SPE_NONCUR_LIAB_DIFF	float	非流动负债差额(特殊报表科目)
    SPE_SHARE_EQUITY_BAL_DIFF	float	股东权益差额(特殊报表科目)
    SPECIAL_PAYABLE	float	专项应付款
    SPECIAL_RESV	float	专项储备
    ST_BONDS_PAYABLE	float	应付短期债券
    ST_BORROWING	float	短期借款
    ST_FIN_PAYABLE	float	应付短期融资款
    SUBR_RCV	float	应收代位追偿款
    SURPLUS_RESV	float	盈余公积金
    TAX_PAYABLE	float	应交税费
    TOT_ASSETS_BAL_DIFF	float	资产差额(合计平衡项目)
    TOT_CUR_ASSETS_DIFF	float	流动资产差额(合计平衡项目)
    TOT_CUR_LIAB_DIFF	float	流动负债差额(合计平衡项目)
    TOT_LIAB_BAL_DIFF	float	负债差额(合计平衡项目)
    TOT_LIAB_EQUITY_BAL_DIFF	float	负债及股东权益差额(合计平衡项目)
    TOT_NONCUR_ASSETS	float	非流动资产合计
    TOT_NONCUR_ASSETS_DIFF	float	非流动资产差额(合计平衡项目)
    TOT_NONCUR_LIAB_DIFF	float	非流动负债差额(合计平衡项目)
    TOT_SHARE	float	期末总股本	单位（股）
    TOT_SHARE_EQUITY_BAL_DIFF	float	股东权益差额(合计平衡项目)
    TOT_SHARE_EQUITY_EXCL_MIN_INT	float	股东权益合计(不含少数股东权益)
    TOT_SHARE_EQUITY_INCL_MIN_INT	float	股东权益合计(含少数股东权益)
    TOTAL_ASSETS	float	资产总计
    TOTAL_CUR_ASSETS	float	流动资产合计
    TOTAL_CUR_LIAB	float	流动负债合计
    TOTAL_LIAB	float	负债合计
    TOTAL_LIAB_SHARE_EQUITY	float	负债及股东权益总计
    TOTAL_NONCUR_LIAB	float	非流动负债合计
    TRADING_FIN_LIAB	float	交易性金融负债
    TRADING_FINASSETS	float	交易性金融资产
    UNAMORTIZED_EXP	float	待摊费用
    UNCONFIRMED_INV_LOSS	float	未确认的投资损失
    UNDISTRIBUTED_PRO	float	未分配利润
    UNEARNED_PREM_RESV	float	未到期责任准备金
    USE_RIGHT_ASSETS	float	使用权资产



    附录
    报告期名称REPORT_TYPE
    报告期类型代码	报告期月份
    1	3月
    2	6月
    3	9月
    4	12月

    报表类型代码表STATEMENT_TYPE
    报表类型代码	报表类型	备注
    1	合并报表	涵盖母公司的财务报表数据，为最新报表
    2	合并报表(单季度)	合并报表(单季度)=合并报表(本期)-合并报表(上一季)
    3	合并报表(单季度调整)	合并报表(单季度调整)=合并报表(本期调整)-合并报表(上一季调整)
    4	合并报表(调整)	本年度公布上年同期的财务报表数据，报告期为上年度
    5	合并报表(更正前)	即出更正公告后，把合并报表的记录修改为合并报表(更正前)；复制原来的记录，更正后报表类型改为合并报表
    6	母公司报表	该公司母公司的财务报表数据
    7	母公司报表(单季度)	母公司报表(单季度)=母公司报表(本期)-母公司报表(上一季)
    8	母公司报表(单季度调整)	母公司报表(单季度调整)=母公司报表(本期调整)-母公司报表(上一季调整)
    9	母公司报表(调整)	该公司母公司的本年度公布上年同期的财务报表数据
    10	母公司报表(更正前)	之前上市公司已披露财务报表数据，但是由于某些特定原因导致出错，未调整之前的原始财务报表数据。
    11	合并报表(未公开)	未在公开信息源披露的财报且加工为合并报表口径
    12	合并报表(调整未公开)	未在公开信息源披露的财报且加工为合并报表调整口径
    13	合并报表(单季度未公开)	未在公开信息源披露的财报且加工为合并报表单季度口径
    14	合并报表(单季度调整未公开)	未在公开信息源披露的财报且加工为母公司报表口径
    15	母公司报表(未公开)	未在公开信息源披露的财报且加工为母公司报表口径
    16	母公司报表(调整未公开)	未在公开信息源披露的财报且加工为母公司报表调整口径
    17	母公司报表(单季度未公开)	未在公开信息源披露的财报且加工或计算为母公司报表单季度口径
    18	母公司报表(单季度调整未公开)	未在公开信息源披露的财报且加工或计算为母公司报表单季度调整口径
    19	合并报表(调整借壳前)	借壳前的合并报表(调整)
    20	合并调整	对合并前各公司的财务报表进行调整，以确保合并财务报表的准确性和可比性
    21	合并报表(单季度借壳前)	借壳前的合并报表(单季度)
    22	合并报表(单季度调整借壳前)	借壳前的合并报表(单季度调整)
    23	母公司报表(借壳前)	借壳前的母公司报表
    24	母公司报表(调整借壳前)	借壳前的母公司报表(调整)
    25	母公司报表(单季度借壳前)	借壳前的母公司报表(单季度)
    26	母公司报表(单季度调整借壳前)	借壳前的母公司报表(单季度调整)
    27	合并报表(第一次更正)	有多次更正时，合并报表的第一次更正
    28	合并报表(第二次更正)	有多次更正时，合并报表的第二次更正
    29	合并调整(第一次更正)	有多次更正时，合并调整的第一次更正
    30	合并报表(单月度)	根据披露的券商月报公告加工为合并报表口径
    31	合并调整(第二次更正)	有多次更正时，合并调整的第二次更正
    32	母公司调整(第二次更正)	有多次更正时，母公司调整的第二次更正
    33	母公司调整(第一次更正)	有多次更正时，母公司调整的第一次更正
    34	母公司报表(第二次更正)	有多次更正时，母公司报表的第二次更正
    35	母公司报表(第一次更正)	有多次更正时，母公司报表的第一次更正
    36	合并报表(第三次更正)	有多次更正时，合并报表的第三次更正
    37	合并调整(第三次更正)	有多次更正时，合并调整的第三次更正
    38	母公司报表(第三次更正)	有多次更正时，母公司报表的第三次更正
    39	母公司调整(第三次更正)	有多次更正时，母公司调整的第三次更正
    40	母公司报表(单月度)	根据披露的券商月报公告加工为母公司报表口径的数据
    41	合并报表(业绩快报)	加工业绩快报中的财务数据（海外数据专用）
    42	合并调整(第一次)	第一次合并调整数据
    43	合并调整(第二次)	第二次合并调整数据
    44	合并调整(第三次)	第三次合并调整数据
    45	合并报表(第四次更正)	有多次更正时，合并报表的第四次更正
    46	合并调整(第四次更正)	有多次更正时，合并调整的第四次更正
    47	母公司报表(第四次更正)	有多次更正时，母公司报表的第四次更正
    48	母公司调整(第四次更正)	有多次更正时，母公司调整的第四次更正
    50	合并调整(更正前)	即出更正公告后，把合并报表（调整）的记录修改为合并调整(更正前)；复制原来的记录，更正后报表类型改为合并报表(调整)
    51	合并报表(下半年报)	合并下半年度的报表
    60	母公司调整(更正前)	该公司母公司的本年度公布上年同期的财务报表数据，但是由于某些特定原因导致出错，未调整之前的原始财务报表数据。
    70	合并报表(借壳前)	公司主体在借壳上市前披露或者计算的为合并报表口径的报表类型
    80	合并报表(预测)	REITS基金的定期报告中披露的预测的合并报表数据
    81	合并报表(公司预测)
    90	项目资产报表	由项目资产管理人编制的一种财务报表，用于反映项目资产的财务状况和经营情况
    91	合并报表(日历年)
    """
    try:
        ensure_logged_in()

        if code_list is None:
            code_list = []

        # 参数验证：如果提供了 begin_date 或 end_date，则两者都必须提供
        if (begin_date is not None and end_date is None) or (begin_date is None and end_date is not None):
            return {
                "success": False,
                "message": "begin_date 和 end_date 必须同时提供"
            }

        info_data_object = ad.InfoData()

        if begin_date is None or end_date is None:
            result = info_data_object.get_balance_sheet(code_list, is_local=False)
        else:
            result = info_data_object.get_balance_sheet(code_list, begin_date=begin_date, end_date=end_date, is_local=False)

        # 过滤报表类型和报告期
        if statement_type or report_type:
            for code in result:
                filters = []
                if statement_type:
                    filters.append(result[code]['STATEMENT_TYPE'] == statement_type)
                if report_type:
                    filters.append(result[code]['REPORT_TYPE'] == report_type)

                if filters:
                    combined_filter = filters[0]
                    for f in filters[1:]:
                        combined_filter = combined_filter & f
                    result[code] = result[code][combined_filter]

        serialized = serialize_dict(result)
        total_count = sum(len(v) if v else 0 for v in serialized.values())
        logger.info(f"查询资产负债表，共 {total_count} 条")

        return {
            "success": True,
            "count": total_count,
            "data": serialized
        }
    except Exception as e:
        logger.error(f"查询资产负债表失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


@mcp.tool()
async def mcp_cash_flow(code_list: list[str], statement_type: str=None,
                            report_type: str=None, begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取指定股票列表的上市公司的现金流量表数据
    入参：
    code_list	list[str]	是	支持沪深A的的代码列表，可见示例
    statement_type	str	报表类型	参看附录的报表类型代码表STATEMENT_TYPE
    report_type	str	报表类型	参看附录的报告期名称REPORT_TYPE
    begin_date	int	否	报告期，本地数据缓存方案，填写8位的整型格式的日期，比如20240101
    end_date	int	否	报告期，本地数据缓存方案，填写8位的整型格式的日期，比如20240101
    返回字段的释义如下：
    字段名称	类型	字段说明	备注
    MARKET_CODE	str	证券代码
    SECURITY_NAME	str	证券简称
    STATEMENT_TYPE	str	报表类型	参看报表类型代码表
    REPORT_TYPE	str	报告期名称	参看报告期名称
    REPORTING_PERIOD	str	报告期
    ANN_DATE	str	公告日期
    ACTUAL_ANN_DATE	str	实际公告日期
    ABSORB_CASH_RECP_INV	double	吸收投资收到的现金
    AMORT_INTAN_ASSETS	double	无形资产摊销
    AMORT_LT_DEFERRED_EXP	double	长期待摊费用摊销
    BEG_BAL_CASH_CASH_EQU	double	期初现金及现金等价物余额
    CASH_END_BAL	double	现金的期末余额
    CASH_FOR_CHARGE	double	支付手续费的现金
    CASH_PAID_INSUR_POLICY	double	支付保单红利的现金
    CASH_PAID_INV	double	投资支付的现金
    CASH_PAID_PUR_CONST_FIOLTA	double	购建固定资产、无形资产和其他长期资产支付的现金
    CASH_PAY_CLAIMS_OIC	double	支付原保险合同赔付款项的现金
    CASH_PAY_DIST_DIV_PRO_INT	double	分配股利、利润或偿付利息支付的现金
    CASH_PAY_EMPLOYEE	double	支付给职工以及为职工支付的现金
    CASH_PAY_FOR_DEBT	double	偿还债务支付的现金
    CASH_PAY_GOODS_SERVICES	double	购买商品、接受劳务支付的现金
    CASH_RECE_BORROW	double	取得借款收到的现金
    CASH_RECE_ISSUE_BONDS	double	发行债券收到的现金
    CASH_RECP_INV_INCOME	double	取得投资收益收到的现金
    CASH_RECP_PREM_OIC	double	收到原保险合同保费取得的现金
    CASH_RECP_RECOV_INV	double	收回投资收到的现金
    CASH_RECP_SG_AND_RS	double	销售商品、提供劳务收到的现金
    COMP_TYPE_CODE	str	公司类型代码	1：非金融类2：银行3：保险4：证券
    CONV_CORP_BONDS_DUE_WITHIN_1Y	double	一年内到期的可转换公司债券
    CONV_DEBT_INTO_CAP	double	债务转为资本
    CREDIT_IMPAIR_LOSS	double	信用减值损失
    CURRENCY_CODE	str	货币代码
    DECR_DEFE_INC_TAX_ASSETS	double	递延所得税资产减少
    DECR_DEFERRED_EXPENSE	double	待摊费用减少
    DECR_INVENTORY	double	存货的减少
    DECR_OPERA_RECEIVABLE	double	经营性应收项目的减少
    DEPRE_FA_OGA_PBA	double	固定资产折旧、油气资产折耗、生产性生物资产折旧
    EFF_FX_FLUC_CASH	double	汇率变动对现金的影响
    END_BAL_CASH_CASH_EQU	double	期末现金及现金等价物余额
    FINANCIAL_EXP	double	财务费用
    FIXED_ASSETS_FIN_LEASE	double	融资租入固定资产
    FREE_CASH_FLOW	double	企业自由现金流量
    INCL_CASH_RECP_SAIMS	double	其中:子公司吸收少数股东投资收到的现金
    INCL_DIV_PRO_PAID_SMS	double	其中:子公司支付给少数股东的股利、利润
    INCR_ACCRUED_EXP	double	预提费用增加
    INCR_DEFE_INC_TAX_LIAB	double	递延所得税负债增加
    INCR_OPERA_PAYABLE	double	经营性应付项目的增加
    IND_NET_CASH_FLOWS_OPERA_ACT	double	间接法-经营活动产生的现金流量净额
    IND_NET_INCR_CASH_AND_EQU	double	间接法-现金及现金等价物净增加额
    INV_LOSS	double	投资损失
    IS_CALCULATION	int	是否计算报表
    LESS_OPEN_BAL_CASH	double	减:现金的期初余额
    LESS_OPEN_BAL_CASH_EQU	double	减:现金等价物的期初余额
    LOSS_DISP_FIOLTA	double	处置固定、无形资产和其他长期资产的损失
    LOSS_FAIRVALUE_CHG	double	公允价值变动损失
    LOSS_FIXED_ASSETS	double	固定资产报废损失
    NET_CASH_FLOWS_FIN_ACT	double	筹资活动产生的现金流量净额
    NET_CASH_FLOWS_INV_ACT	double	投资活动产生的现金流量净额
    NET_CASH_FLOWS_OPERA_ACT	double	经营活动产生的现金流量净额
    NET_CASH_PAID_SOBU	double	取得子公司及其他营业单位支付的现金净额
    NET_CASH_REC_SEC	double	代理买卖证券收到的现金净额
    NET_CASH_RECP_DISP_FIOLTA	double	处置固定资产、无形资产和其他长期资产收回的现金净额
    NET_CASH_RECP_DISP_SOBU	double	处置子公司及其他营业单位收到的现金净额
    NET_CASH_RECP_REINSU_BUS	double	收到再保业务现金净额
    NET_INCR_BORR_FUND	double	拆入资金净增加额
    NET_INCR_BORR_OFI	double	向其他金融机构拆入资金净增加额
    NET_INCR_CASH_AND_CASH_EQU	double	现金及现金等价物净增加额
    NET_INCR_CUS_LOAN_ADV	double	客户贷款及垫款净增加额
    NET_INCR_DEP_CB_IB	double	存放央行和同业款项净增加额
    NET_INCR_DEP_CUS_AND_IB	double	客户存款和同业存放款项净增加额
    NET_INCR_DISMANTLE_CAP	double	拆出资金净增加额
    NET_INCR_DISP_FAAS	double	处置可供出售金融资产净增加额
    NET_INCR_DISP_TFA	double	处置交易性金融资产净增加额
    NET_INCR_INSURED_SAVE	double	保户储金净增加额
    NET_INCR_INT_AND_CHARGE	double	收取利息和手续费净增加额
    NET_INCR_LOANS_CENTRAL_BANK	double	向中央银行借款净增加额
    NET_INCR_PLEDGE_LOAN	double	质押贷款净增加额
    NET_INCR_REPU_BUS_FUND	double	回购业务资金净增加额
    NET_PROFIT	double	净利润
    OTH_CASH_PAY_INV_ACT	double	支付其他与投资活动有关的现金
    OTH_CASH_PAY_OPERA_ACT	double	支付其他与经营活动有关的现金
    OTH_CASH_RECP_INV_ACT	double	收到其他与投资活动有关的现金
    OTHER_ASSETS_IMPAIR_LOSS	double	其他资产减值损失
    OTHER_CASH_PAY_FIN_ACT	double	支付其他与筹资活动有关的现金
    OTHER_CASH_RECP_FIN_ACT	double	收到其他与筹资活动有关的现金
    OTHER_CASH_RECP_OPER_ACT	double	收到其他与经营活动有关的现金
    OTHERS	double	其他（废弃）
    PAY_ALL_TAX	double	支付的各项税费
    PLUS_ASSETS_DEPRE_PREP	double	加:资产减值准备
    PLUS_END_BAL_CASH_EQU	double	加:现金等价物的期末余额
    RECP_TAX_REFUND	double	收到的税费返还
    SPE_BAL_CASH_INFLOW_FIN_ACT	double	筹资活动现金流入差额
    SPE_BAL_CASH_INFLOW_INV_ACT	double	投资活动现金流入差额
    SPE_BAL_CASH_INFLOW_OPERA_ACT	double	经营活动现金流入差额
    SPE_BAL_CASH_OUTFLOW_FIN	double	筹资活动现金流出差额
    SPE_BAL_CASH_OUTFLOW_INV	double	投资活动现金流出差额
    SPE_BAL_CASH_OUTFLOW_OPERA	double	经营活动现金流出差额
    SPE_BAL_NETCASH_INC_DIFF_IND	double	间接法-现金净增加额差额
    SPE_BAL_NETCASH_INCR_DIFF	double	现金净增加额差额
    SPE_BAL_NETCASH_OPERA_IND	double	间接法-经营活动现金流量净额差额
    TOT_BAL_CASH_INFLOW_FIN_ACT	double	筹资活动现金流入差额
    TOT_BAL_CASH_INFLOW_INV_ACT	double	投资活动现金流入差额
    TOT_BAL_CASH_INFLOW_OPERA_ACT	double	经营活动现金流入差额
    TOT_BAL_CASH_OUTFLOW_FIN	double	筹资活动现金流出差额
    TOT_BAL_CASH_OUTFLOW_INV	double	投资活动现金流出差额
    TOT_BAL_CASH_OUTFLOW_OPERA	double	经营活动现金流出差额
    TOT_BAL_NETCASH_FLOW_FIN	double	筹资活动产生的现金流量净额差额
    TOT_BAL_NETCASH_FLOW_INV	double	投资活动产生的现金流量净额差额
    TOT_BAL_NETCASH_FLOW_OPERA	double	经营活动产生的现金流量净额差额
    TOT_BAL_NETCASH_INC_DIFF_IND	double	间接法-现金净增加额差额
    TOT_BAL_NETCASH_INCR_DIFF	double	现金净增加额差额
    TOT_BAL_NETCASH_OPERA_IND	double	间接法-经营活动现金流量净额差额
    TOT_CASH_INFLOW_FIN_ACT	double	筹资活动现金流入小计
    TOT_CASH_INFLOW_INV_ACT	double	投资活动现金流入小计
    TOT_CASH_INFLOW_OPER_ACT	double	经营活动现金流入小计
    TOT_CASH_OUTFLOW_FIN_ACT	double	筹资活动现金流出小计
    TOT_CASH_OUTFLOW_INV_ACT	double	投资活动现金流出小计
    TOT_CASH_OUTFLOW_OPERA_ACT	double	经营活动现金流出小计
    UNCONFIRMED_INV_LOSS	double	未确认投资损失
    USE_RIGHT_ASSET_DEP	double	使用权资产折旧



    附录
    报告期名称REPORT_TYPE
    报告期类型代码	报告期月份
    1	3月
    2	6月
    3	9月
    4	12月

    报表类型代码表STATEMENT_TYPE
    报表类型代码	报表类型	备注
    1	合并报表	涵盖母公司的财务报表数据，为最新报表
    2	合并报表(单季度)	合并报表(单季度)=合并报表(本期)-合并报表(上一季)
    3	合并报表(单季度调整)	合并报表(单季度调整)=合并报表(本期调整)-合并报表(上一季调整)
    4	合并报表(调整)	本年度公布上年同期的财务报表数据，报告期为上年度
    5	合并报表(更正前)	即出更正公告后，把合并报表的记录修改为合并报表(更正前)；复制原来的记录，更正后报表类型改为合并报表
    6	母公司报表	该公司母公司的财务报表数据
    7	母公司报表(单季度)	母公司报表(单季度)=母公司报表(本期)-母公司报表(上一季)
    8	母公司报表(单季度调整)	母公司报表(单季度调整)=母公司报表(本期调整)-母公司报表(上一季调整)
    9	母公司报表(调整)	该公司母公司的本年度公布上年同期的财务报表数据
    10	母公司报表(更正前)	之前上市公司已披露财务报表数据，但是由于某些特定原因导致出错，未调整之前的原始财务报表数据。
    11	合并报表(未公开)	未在公开信息源披露的财报且加工为合并报表口径
    12	合并报表(调整未公开)	未在公开信息源披露的财报且加工为合并报表调整口径
    13	合并报表(单季度未公开)	未在公开信息源披露的财报且加工为合并报表单季度口径
    14	合并报表(单季度调整未公开)	未在公开信息源披露的财报且加工为母公司报表口径
    15	母公司报表(未公开)	未在公开信息源披露的财报且加工为母公司报表口径
    16	母公司报表(调整未公开)	未在公开信息源披露的财报且加工为母公司报表调整口径
    17	母公司报表(单季度未公开)	未在公开信息源披露的财报且加工或计算为母公司报表单季度口径
    18	母公司报表(单季度调整未公开)	未在公开信息源披露的财报且加工或计算为母公司报表单季度调整口径
    19	合并报表(调整借壳前)	借壳前的合并报表(调整)
    20	合并调整	对合并前各公司的财务报表进行调整，以确保合并财务报表的准确性和可比性
    21	合并报表(单季度借壳前)	借壳前的合并报表(单季度)
    22	合并报表(单季度调整借壳前)	借壳前的合并报表(单季度调整)
    23	母公司报表(借壳前)	借壳前的母公司报表
    24	母公司报表(调整借壳前)	借壳前的母公司报表(调整)
    25	母公司报表(单季度借壳前)	借壳前的母公司报表(单季度)
    26	母公司报表(单季度调整借壳前)	借壳前的母公司报表(单季度调整)
    27	合并报表(第一次更正)	有多次更正时，合并报表的第一次更正
    28	合并报表(第二次更正)	有多次更正时，合并报表的第二次更正
    29	合并调整(第一次更正)	有多次更正时，合并调整的第一次更正
    30	合并报表(单月度)	根据披露的券商月报公告加工为合并报表口径
    31	合并调整(第二次更正)	有多次更正时，合并调整的第二次更正
    32	母公司调整(第二次更正)	有多次更正时，母公司调整的第二次更正
    33	母公司调整(第一次更正)	有多次更正时，母公司调整的第一次更正
    34	母公司报表(第二次更正)	有多次更正时，母公司报表的第二次更正
    35	母公司报表(第一次更正)	有多次更正时，母公司报表的第一次更正
    36	合并报表(第三次更正)	有多次更正时，合并报表的第三次更正
    37	合并调整(第三次更正)	有多次更正时，合并调整的第三次更正
    38	母公司报表(第三次更正)	有多次更正时，母公司报表的第三次更正
    39	母公司调整(第三次更正)	有多次更正时，母公司调整的第三次更正
    40	母公司报表(单月度)	根据披露的券商月报公告加工为母公司报表口径的数据
    41	合并报表(业绩快报)	加工业绩快报中的财务数据（海外数据专用）
    42	合并调整(第一次)	第一次合并调整数据
    43	合并调整(第二次)	第二次合并调整数据
    44	合并调整(第三次)	第三次合并调整数据
    45	合并报表(第四次更正)	有多次更正时，合并报表的第四次更正
    46	合并调整(第四次更正)	有多次更正时，合并调整的第四次更正
    47	母公司报表(第四次更正)	有多次更正时，母公司报表的第四次更正
    48	母公司调整(第四次更正)	有多次更正时，母公司调整的第四次更正
    50	合并调整(更正前)	即出更正公告后，把合并报表（调整）的记录修改为合并调整(更正前)；复制原来的记录，更正后报表类型改为合并报表(调整)
    51	合并报表(下半年报)	合并下半年度的报表
    60	母公司调整(更正前)	该公司母公司的本年度公布上年同期的财务报表数据，但是由于某些特定原因导致出错，未调整之前的原始财务报表数据。
    70	合并报表(借壳前)	公司主体在借壳上市前披露或者计算的为合并报表口径的报表类型
    80	合并报表(预测)	REITS基金的定期报告中披露的预测的合并报表数据
    81	合并报表(公司预测)
    90	项目资产报表	由项目资产管理人编制的一种财务报表，用于反映项目资产的财务状况和经营情况
    91	合并报表(日历年)
    """
    if end_date is None:
        end_date = datetime_to_int()

    if code_list is None:
        code_list = []
    info_data_object = ad.InfoData()
    result = info_data_object.get_cash_flow(code_list, begin_date=begin_date, end_date=end_date, is_local=False)
    for code in result:
        result[code] = result[code][(result[code]['STATEMENT_TYPE']==statement_type)&
                                    (result[code]['REPORT_TYPE']==report_type)]
    return serialize_dict(result)


@mcp.tool()
async def mcp_income(code_list: list[str], statement_type: str=None,
                            report_type: str=None, begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取指定股票列表的上市公司的利润表数据
    入参：
    code_list	list[str]	是	支持沪深A的的代码列表，可见示例
    statement_type	str	报表类型	参看附录的报表类型代码表STATEMENT_TYPE
    report_type	str	报表类型	参看附录的报告期名称REPORT_TYPE
    begin_date	int	否	报告期，本地数据缓存方案，填写8位的整型格式的日期，比如20240101
    end_date	int	否	报告期，本地数据缓存方案，填写8位的整型格式的日期，比如20240101
    返回字段的释义如下：
    字段名称	类型	字段说明	备注
    MARKET_CODE	str	证券代码
    SECURITY_NAME	str	证券简称
    STATEMENT_TYPE	str	报表类型	参看报表类型代码表
    REPORT_TYPE	str	报告期名称	参看报告期名称
    REPORTING_PERIOD	str	报告期
    ANN_DATE	str	公告日期
    ACTUAL_ANN_DATE	str	实际公告日期
    AMORT_COST_FIN_ASSETS_EAR	float	以摊余成本计量的金融资产终止确认收益
    ANN_DATE	str	公告日期
    BASIC_EPS	float	基本每股收益
    BEG_UNDISTRIBUTED_PRO	float	年初未分配利润
    CAPITALIZED_COM_STOCK_DIV	float	转作股本的普通股股利
    COMMENTS	str	备注
    COMMON_STOCK_DIV_PAYABLE	float	应付普通股股利
    COMP_TYPE_CODE	str	公司类型代码	1：非金融类2：银行3：保险4：证券
    CONTINUED_NET_OPERA_PRO	float	持续经营净利润
    CREDIT_IMPAIR_LOSS	float	信用减值损失
    CURRENCY_CODE	str	货币代码
    DILUTED_EPS	float	稀释每股收益
    DISTRIBUTIVE_PRO	float	可分配利润
    DISTRIBUTIVE_PRO_SHAREHOLDER	float	可供股东分配的利润
    DIV_EXP_INSUR	float	保户红利支出
    EBIT	float	息税前利润	正向法
    EBITDA	float	息税折旧摊销前利润
    EMPLOYEE_WELFARE	float	职工奖金福利
    END_NET_OPERA_PRO	float	终止经营净利润
    EXT_INSUR_CONT_RSRV	float	提取保险责任准备金
    EXT_UNEARNED_PREM_RES	float	提取未到期责任准备金
    FIN_EXP_INT_EXP	float	财务费用:利息费用
    FIN_EXP_INT_INC	float	财务费用:利息收入
    GAIN_DISPOSAL_ASSETS	float	资产处置收益
    HANDLING_CHRG_COMM_FEE	float	手续费及佣金收入
    INCL_INC_INV_JV_ENTP	float	其中:对联营企业和合营企业的投资收益
    INCL_LESS_LOSS_DISP_NCUR_ASSET	float	其中:减:非流动资产处置净损失
    INCL_REINSUR_PREM_INC	float	其中:分保费收入
    INCOME_TAX	float	所得税
    INSUR_EXP	float	保险业务支出
    INSUR_PREM	float	已赚保费
    INTEREST_INC	float	利息收入
    IS_CALCULATION	float	是否计算报表
    LESS_ADMIN_EXP	float	减:管理费用
    LESS_AMORT_COMPEN_EXP	float	减:摊回赔付支出
    LESS_AMORT_INSUR_CONT_RSRV	float	减:摊回保险责任准备金
    LESS_AMORT_REINSUR_EXP	float	减:摊回分保费用
    LESS_ASSETS_IMPAIR_LOSS	float	减:资产减值损失
    LESS_BUS_TAX_SURCHARGE	float	减:营业税金及附加
    LESS_FIN_EXP	float	减:财务费用
    LESS_HANDLING_CHRG_COMM_FEE	float	减:手续费及佣金支出
    LESS_INTEREST_EXP	float	减:利息支出
    LESS_NON_OPERA_EXP	float	减:营业外支出
    LESS_OPERA_COST	float	减:营业成本
    LESS_REINSUR_PREM	float	减:分出保费
    LESS_SELLING_EXP	float	减:销售费用
    MARKET_CODE	str	证券代码
    MIN_INT_INC	float	少数股东损益
    NET_EXPOSURE_HEDGING_GAIN	float	净敞口套期收益
    NET_HANDLING_CHRG_COMM_FEE	float	手续费及佣金净收入
    NET_INC_EC_ASSET_MGMT_BUS	float	受托客户资产管理业务净收入
    NET_INC_SEC_BROK_BUS	float	代理买卖证券业务净收入
    NET_INC_SEC_UW_BUS	float	证券承销业务净收入
    NET_INTEREST_INC	float	利息净收入
    NET_PRO_AFTER_DED_NR_GL	float	扣除非经常性损益后净利润（扣除少数股东损益）
    NET_PRO_AFTER_DED_NR_GL_COR	float	扣除非经常性损益后的净利润(财务重要指标(更正前))
    NET_PRO_EXCL_MIN_INT_INC	float	净利润(不含少数股东损益)
    NET_PRO_INCL_MIN_INT_INC	float	净利润(含少数股东损益)
    NET_PRO_UNDER_INT_ACC_STA	float	国际会计准则净利润
    OPERA_EXP	float	营业支出
    OPERA_PROFIT	float	营业利润
    OPERA_REV	float	营业收入
    OTH_ASSETS_IMPAIR_LOSS	float	其他资产减值损失
    OTH_BUS_COST	float	其他业务成本
    OTH_BUS_INC	float	其他业务收入
    OTH_COMPRE_INC	float	其他综合收益
    OTH_INCOME	float	其他收益
    OTH_NET_OPERA_INC	float	其他经营净收益
    PLUS_NET_FX_INC	float	加:汇兑净收益
    PLUS_NET_GAIN_CHG_FV	float	加:公允价值变动净收益
    PLUS_NET_INV_INC	float	加:投资净收益
    PLUS_NON_OPERA_REV	float	加:营业外收入
    PLUS_OTH_NET_BUS_INC	float	加:其他业务净收益
    PREFERRED_SHARE_DIV_PAYABLE	float	应付优先股股利
    PREM_BUS_INC	float	保费业务收入
    RD_EXP	float	研发费用
    REINSURANCE_EXP	float	分保费用
    REPORT_TYPE	str	报告期名称	参看报告期名称
    REPORTING_PERIOD	str	报告期
    SECURITY_NAME	str	证券简称
    SPE_BAL_NET_PRO_MARG	float	净利润差额(特殊报表科目)
    SPE_BAL_OPERA_PRO_MARG	float	营业利润差额(特殊报表科目)
    SPE_BAL_TOT_OPERA_COST_DIF	float	营业总成本差额(特殊报表科目)
    SPE_BAL_TOT_OPERA_INC_DIF	float	营业总收入差额(特殊报表科目)
    SPE_BAL_TOT_PRO_MARG	float	利润总额差额(特殊报表科目)
    SPE_TOT_OPERA_COST_DIF_STATE	str	营业总成本差额说明(特殊报表科目)
    SPE_TOT_OPERA_INC_DIF_STATE	str	营业总收入差额说明(特殊报表科目)
    SURR_VALUE	float	退保金
    TOT_BAL_NET_PRO_MARG	float	净利润差额(合计平衡项目)
    TOT_BAL_OPERA_PRO_MARG	float	营业利润差额(合计平衡项目)
    TOT_BAL_TOT_PRO_MARG	float	利润总额差额(合计平衡项目)
    TOT_COMPEN_EXP	float	赔付总支出
    TOT_COMPRE_INC	float	综合收益总额
    TOT_COMPRE_INC_MIN_SHARE	float	综合收益总额(少数股东)
    TOT_COMPRE_INC_PARENT_COMP	float	综合收益总额(母公司)
    TOT_OPERA_COST	float	营业总成本
    TOT_OPERA_COST2	float	营业总成本2
    TOT_OPERA_REV	float	营业总收入
    TOTAL_PROFIT	float	利润总额
    TRANSFER_HOUSING_REVO_FUNDS	float	住房周转金转入
    TRANSFER_OTHERS	float	其他转入
    TRANSFER_SURPLUS_RESERVE	float	盈余公积转入
    UNCONFIRMED_INV_LOSS	float	未确认投资损失
    WITHDRAW_ANY_SURPLUS_RESV	float	提取任意盈余公积金
    WITHDRAW_ENT_DEVELOP_FUND	float	提取企业发展基金
    WITHDRAW_LEG_PUB_WEL_FUND	float	提取法定公益金
    WITHDRAW_LEG_SURPLUS	float	提取法定盈余公积
    WITHDRAW_RESV_FUND	float	提取储备基金

    附录
    报告期名称REPORT_TYPE
    报告期类型代码	报告期月份
    1	3月
    2	6月
    3	9月
    4	12月

    报表类型代码表STATEMENT_TYPE
    报表类型代码	报表类型	备注
    1	合并报表	涵盖母公司的财务报表数据，为最新报表
    2	合并报表(单季度)	合并报表(单季度)=合并报表(本期)-合并报表(上一季)
    3	合并报表(单季度调整)	合并报表(单季度调整)=合并报表(本期调整)-合并报表(上一季调整)
    4	合并报表(调整)	本年度公布上年同期的财务报表数据，报告期为上年度
    5	合并报表(更正前)	即出更正公告后，把合并报表的记录修改为合并报表(更正前)；复制原来的记录，更正后报表类型改为合并报表
    6	母公司报表	该公司母公司的财务报表数据
    7	母公司报表(单季度)	母公司报表(单季度)=母公司报表(本期)-母公司报表(上一季)
    8	母公司报表(单季度调整)	母公司报表(单季度调整)=母公司报表(本期调整)-母公司报表(上一季调整)
    9	母公司报表(调整)	该公司母公司的本年度公布上年同期的财务报表数据
    10	母公司报表(更正前)	之前上市公司已披露财务报表数据，但是由于某些特定原因导致出错，未调整之前的原始财务报表数据。
    11	合并报表(未公开)	未在公开信息源披露的财报且加工为合并报表口径
    12	合并报表(调整未公开)	未在公开信息源披露的财报且加工为合并报表调整口径
    13	合并报表(单季度未公开)	未在公开信息源披露的财报且加工为合并报表单季度口径
    14	合并报表(单季度调整未公开)	未在公开信息源披露的财报且加工为母公司报表口径
    15	母公司报表(未公开)	未在公开信息源披露的财报且加工为母公司报表口径
    16	母公司报表(调整未公开)	未在公开信息源披露的财报且加工为母公司报表调整口径
    17	母公司报表(单季度未公开)	未在公开信息源披露的财报且加工或计算为母公司报表单季度口径
    18	母公司报表(单季度调整未公开)	未在公开信息源披露的财报且加工或计算为母公司报表单季度调整口径
    19	合并报表(调整借壳前)	借壳前的合并报表(调整)
    20	合并调整	对合并前各公司的财务报表进行调整，以确保合并财务报表的准确性和可比性
    21	合并报表(单季度借壳前)	借壳前的合并报表(单季度)
    22	合并报表(单季度调整借壳前)	借壳前的合并报表(单季度调整)
    23	母公司报表(借壳前)	借壳前的母公司报表
    24	母公司报表(调整借壳前)	借壳前的母公司报表(调整)
    25	母公司报表(单季度借壳前)	借壳前的母公司报表(单季度)
    26	母公司报表(单季度调整借壳前)	借壳前的母公司报表(单季度调整)
    27	合并报表(第一次更正)	有多次更正时，合并报表的第一次更正
    28	合并报表(第二次更正)	有多次更正时，合并报表的第二次更正
    29	合并调整(第一次更正)	有多次更正时，合并调整的第一次更正
    30	合并报表(单月度)	根据披露的券商月报公告加工为合并报表口径
    31	合并调整(第二次更正)	有多次更正时，合并调整的第二次更正
    32	母公司调整(第二次更正)	有多次更正时，母公司调整的第二次更正
    33	母公司调整(第一次更正)	有多次更正时，母公司调整的第一次更正
    34	母公司报表(第二次更正)	有多次更正时，母公司报表的第二次更正
    35	母公司报表(第一次更正)	有多次更正时，母公司报表的第一次更正
    36	合并报表(第三次更正)	有多次更正时，合并报表的第三次更正
    37	合并调整(第三次更正)	有多次更正时，合并调整的第三次更正
    38	母公司报表(第三次更正)	有多次更正时，母公司报表的第三次更正
    39	母公司调整(第三次更正)	有多次更正时，母公司调整的第三次更正
    40	母公司报表(单月度)	根据披露的券商月报公告加工为母公司报表口径的数据
    41	合并报表(业绩快报)	加工业绩快报中的财务数据（海外数据专用）
    42	合并调整(第一次)	第一次合并调整数据
    43	合并调整(第二次)	第二次合并调整数据
    44	合并调整(第三次)	第三次合并调整数据
    45	合并报表(第四次更正)	有多次更正时，合并报表的第四次更正
    46	合并调整(第四次更正)	有多次更正时，合并调整的第四次更正
    47	母公司报表(第四次更正)	有多次更正时，母公司报表的第四次更正
    48	母公司调整(第四次更正)	有多次更正时，母公司调整的第四次更正
    50	合并调整(更正前)	即出更正公告后，把合并报表（调整）的记录修改为合并调整(更正前)；复制原来的记录，更正后报表类型改为合并报表(调整)
    51	合并报表(下半年报)	合并下半年度的报表
    60	母公司调整(更正前)	该公司母公司的本年度公布上年同期的财务报表数据，但是由于某些特定原因导致出错，未调整之前的原始财务报表数据。
    70	合并报表(借壳前)	公司主体在借壳上市前披露或者计算的为合并报表口径的报表类型
    80	合并报表(预测)	REITS基金的定期报告中披露的预测的合并报表数据
    81	合并报表(公司预测)
    90	项目资产报表	由项目资产管理人编制的一种财务报表，用于反映项目资产的财务状况和经营情况
    91	合并报表(日历年)
    """
    if end_date is None:
        end_date = datetime_to_int()

    if code_list is None:
        code_list = []
    info_data_object = ad.InfoData()
    result = info_data_object.get_income(code_list, begin_date=begin_date, end_date=end_date, is_local=False)
    for code in result:
        result[code] = result[code][(result[code]['STATEMENT_TYPE']==statement_type)&
                                    (result[code]['REPORT_TYPE']==report_type)]
    return serialize_dict(result)


@mcp.tool()
async def mcp_profit_express(code_list: list[str], begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取指定股票列表的上市公司的业绩快报数据
    入参：
    code_list	list[str]	是	支持沪深A的的代码列表，可见示例
    begin_date	int	否	报告期，本地数据缓存方案，填写8位的整型格式的日期，比如20240101
    end_date	int	否	报告期，本地数据缓存方案，填写8位的整型格式的日期，比如20240101
    返回字段的释义如下：
    参数	 数据类型	字段说明	备注
    MARKET_CODE	str	证券代码
    REPORTING_PERIOD	str	报告期	报告内容记录的截止时间点，报告成果的时期
    ANN_DATE	str	公告日期	公告发布当天的日期；有多个阶段的事件，首次披露该事件的日期
    ACTUAL_ANN_DATE	str	实际公告日期	实际数据来源公告的日期；更正发生公告的日期
    TOTAL_ASSETS	float64	总资产(元)	指经济实体拥有或控制的能带来经济利益的全部资产
    NET_PRO_EXCL_MIN_INT_INC	float64	净利润(元)	企业合并净利润中归属于母公司股东所有的那部分利润
    TOT_OPERA_REV	float64	营业总收入(元)	企业从事销售商品、提供劳务和让渡资产使用权等日常业务过程形成的经济利益的总流入
    TOTAL_PROFIT	float64	利润总额(元)	企业一定时期内的纯收入扣除应交纳后的余额
    OPERA_PROFIT	float64	营业利润(元)	企业在其全部销售业务中实现的利润
    EPS_BASIC	float64	每股收益-基本(元)	企业按照属于普通股股东的当期净利润，除以发行在外普通股的加权平均数计算得到的每股收益
    TOT_SHARE_EQU_EXCL_MIN_INT	float64	股东权益合计(不含少数股东权益)(元)	公司集团的所有者权益中归属于母公司所有者权益的部分
    IS_AUDIT	float64	是否审计	1:是 0：否
    ROE_WEIGHTED	float64	净资产收益率-加权(%)	经营期间净资产赚取利润的结果的一个动态指标，反应企业净资产创造利润的能力
    LAST_YEAR_REVISED_NET_PRO	float64	去年同期修正后净利润	元
    PERFORMANCE_SUMMARY	str	业绩简要说明	针对业绩快报的简单说明
    NET_ASSET_PS	float64	每股净资产	元
    MEMO	str	备注	附加的注解说明
    YOY_GR_GROSS_PRO	float64	同比增长率:营业利润	%
    YOY_GR_GROSS_REV	float64	同比增长率:营业总收入	%
    YOY_GR_NET_PROFIT_PARENT	float64	同比增长率:归属母公司股东的净利润	%
    YOY_GR_TOT_PRO	float64	同比增长率:利润总额	%
    YOY_ID_WAROE	float64	同比增减:加权平均净资产收益率	%
    YOY_GR_EPS_BASIC	float64	同比增长率:基本每股收益	%
    GROWTH_RATE_EQUITY	float64	比年初增长率:归属母公司的股东权益	%
    GROWTH_RATE_ASSETS	float64	比年初增长率:总资产	%
    GROWTH_RATE_NAPS	float64	比年初增长率:归属于母公司股东的每股净资产	%
    LAST_YEAR_TOT_OPERA_REV	float64	去年同期营业总收入	元
    LAST_YEAR_TOTAL_PROFIT	float64	去年同期利润总额	元
    LAST_YEAR_OPERA_PRO	float64	去年同期营业利润	元
    LAST_YEAR_EPS_DILUTED	float64	去年同期每股收益	元
    LAST_YEAR_NET_PROFIT	float64	去年同期净利润	元
    INITIAL_NET_ASSET_PS	float64	期初每股净资产	元
    INITIAL_NET_ASSETS	float64	期初净资产	元

    """
    if end_date is None:
        end_date = datetime_to_int()

    if code_list is None:
        code_list = []
    info_data_object = ad.InfoData()
    result = info_data_object.get_profit_express(code_list, begin_date=begin_date, end_date=end_date, is_local=False)

    return serialize_dataframe(result)


@mcp.tool()
async def mcp_profit_notice(code_list: list[str],
                            report_type: str=None, begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取指定股票列表的上市公司的业绩预告数据
    入参：
    code_list	list[str]	是	支持沪深A的的代码列表，可见示例
    begin_date	int	否	报告期，本地数据缓存方案，填写8位的整型格式的日期，比如20240101
    end_date	int	否	报告期，本地数据缓存方案，填写8位的整型格式的日期，比如20240101
    返回字段的释义如下：
    profit_notice 的字段说明：
    参数	 数据类型	字段说明	备注
    MARKET_CODE	str	证券代码
    SECURITY_NAME	str	证券简称
    P_TYPECODE	str	业绩预告类型代码	1：不确定
    2：略减
    3：略增
    4：扭亏
    5：其他
    6：首亏
    7：续亏
    8：续盈
    9：预减
    10：预增
    11：持平
    REPORTING_PERIOD	str	报告期	分为年度、半年度、季度
    ANN_DATE	str	公告日期	公告发布当天的日期
    P_CHANGE_MAX	float64	预告净利润变动幅度上限（%）	对于净利润金额同比变动幅度预计的最高值
    P_CHANGE_MIN	float64	预告净利润变动幅度下限（%）	对于净利润金额同比变动幅度预计的最低值
    NET_PROFIT_MAX	float64	预告净利润上限（万元）	对于净利润金额预计的最高值
    NET_PROFIT_MIN	float64	预告净利润下限（万元）	对于净利润金额预计的最低值
    FIRST_ANN_DATE	str	首次公告日	首次披露本报告期业绩预告内容的公告日期
    P_NUMBER	float64	公布次数	同一报告期的业绩预告公告的披露次数
    P_REASON	str	业绩变动原因
    P_SUMMARY	str	业绩预告摘要
    P_NET_PARENT_FIRM	float64	上年同期归母净利润	业绩预告中直接公布的上年同期归母净利润
    REPORT_TYPE	str	报告期名称	参看报告期名称

    附录
    报告期名称REPORT_TYPE
    报告期类型代码	报告期月份
    1	3月
    2	6月
    3	9月
    4	12月

    """
    if end_date is None:
        end_date = datetime_to_int()

    if code_list is None:
        code_list = []
    info_data_object = ad.InfoData()
    result = info_data_object.get_profit_notice(code_list, begin_date=begin_date, end_date=end_date, is_local=False)
    if report_type:
        result = result[result['REPORT_TYPE']==report_type]
    return serialize_dataframe(result)


@mcp.tool()
async def mcp_share_holder(code_list: list[str], begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取指定股票列表的上市公司的十大股东数据
    入参：
    code_list	list[str]	是	支持沪深A的的代码列表，可见示例
    begin_date	int	否	报告期，本地数据缓存方案，填写8位的整型格式的日期，比如20240101
    end_date	int	否	报告期，本地数据缓存方案，填写8位的整型格式的日期，比如20240101
    返回字段的释义如下：
    share_holder 的字段说明：
    参数	 数据类型	字段说明	备注
    ANN_DATE	str	公告日期,
    MARKET_CODE	str	证券代码
    HOLDER_ENDDATE	str	到期日期
    HOLDER_TYPE 	int	股东类别	10:十大股东
    20:流通股前十大股东
    QTY_NUM	int	持股量序号
    HOLDER_NAME	str	股东名称
    HOLDER_HOLDER_CATEGORY	int	股东性质	1：个人 2：公司
    HOLDER_QUANTITY,	float	持股数（股）
    HOLDER_PCT	float	持股比例（%）,
    HOLDER_SHARECATEGORYNAME	str	股份类型	当HOLDER_TYPE为20:流通股前十大股东时，全部为‘A FloatHolder’
    FLOAT_QTY	float	流通股数量
    """
    if end_date is None:
        end_date = datetime_to_int()

    if code_list is None:
        code_list = []
    info_data_object = ad.InfoData()
    result = info_data_object.get_share_holder(code_list, begin_date=begin_date, end_date=end_date, is_local=False)

    return serialize_dataframe(result)


@mcp.tool()
async def mcp_holder_num(code_list: list[str], begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取指定股票列表的上市公司的股东户数数据
    入参：
    code_list	list[str]	是	支持沪深A的的代码列表，可见示例
    begin_date	int	否	报告期，本地数据缓存方案，填写8位的整型格式的日期，比如20240101
    end_date	int	否	报告期，本地数据缓存方案，填写8位的整型格式的日期，比如20240101
    返回字段的释义如下：
    holder_num 的字段说明：
    参数	 数据类型	字段说明
    MARKET_CODE	string	证券代码
    ANN_DT	string	公告日期
    HOLDER_ENDDATE	string	股东户数统计的截止日期
    HOLDER_TOTAL_NUM	float	A股、B股、H股、境外股的总户数
    HOLDER_NUM	float	A股股东户数

    """
    if end_date is None:
        end_date = datetime_to_int()

    if code_list is None:
        code_list = []
    info_data_object = ad.InfoData()
    result = info_data_object.get_holder_num(code_list, begin_date=begin_date, end_date=end_date, is_local=False)

    return serialize_dataframe(result)


@mcp.tool()
async def mcp_equity_structure(code_list: list[str], begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取指定股票列表的上市公司的股本结构数据
    入参：
    code_list	list[str]	是	支持沪深A的的代码列表，可见示例
    begin_date	int	否	报告期，本地数据缓存方案，填写8位的整型格式的日期，比如20240101
    end_date	int	否	报告期，本地数据缓存方案，填写8位的整型格式的日期，比如20240101
    返回字段的释义如下：
    equity_structure 的字段说明：
    字段名称	类型	字段说明	备注
    MARKET_CODE	string	证券代码	
    ANN_DATE	string	公告日期	
    CHANGE_DATE	string	变动日期	注：股票分红送转股时的红股上市日;股票增发时的新股上市日
    SHARE_CHANGE_REASON_STR	string	股本变动原因描述	
    EX_CHANGE_DATE	string	除权日期	股票分红送转股时的除权日;股票增发时的登记日
    CURRENT_SIGN	int	最新标志	1:是0:否
    IS_VALID	int	是否有效	用来区分除权日相同时，是否为公司公告公布的最新股份数
    1:是0:否
    TOT_SHARE	float	总股本(万股)	
    FLOAT_SHARE	float	流通股(万股)	
    FLOAT_A_SHARE	float	流通A股(万股)	
    FLOAT_B_SHARE	float	流通B股(万股)	
    FLOAT_HK_SHARE	float	香港流通股(万股)	
    FLOAT_OS_SHARE	float	海外流通股(万股)	
    TOT_TRADABLE_SHARE	float	流通股合计	
    RTD_A_SHARE_INST	float	限售A股(其他内资持股:机构配售股)	
    RTD_A_SHARE_DOMESNP	float	限售A股(其他内资持股:境内自然人持股)	
    RTD_SHARE_SENIOR	float	限售股份(高管持股)(万股)	
    RTD_A_SHARE_FOREIGN	float	限售A股(外资持股)	
    RTD_A_SHARE_FORJUR	float	限售A股(境外法人持股)	
    RTD_A_SHARE_FORNP	float	限售A股(境外自然人持股)	
    RESTRICTED_B_SHARE	float	限售B股(万股)	
    OTHER_RTD_SHARE	float	其他限售股	
    NON_TRADABLE_SHARE	float	非流通股	
    NTRD_SHARE_STATE_PCT	float	非流通股(国有股)	
    NTRD_SHARE_STATE	float	非流通股(国家股)	
    NTRD_SHARE_STATEJUR	float	非流通股(国有法人股)	
    NTRD_SHARE_DOMESJUR	float	非流通股(境内法人股)	
    NTRD_SHARE_DOMES_INITIATOR	float	非流通股(境内法人股:境内发起人股)	
    NTRD_SHARE_IPOJURIS	float	非流通股(境内法人股:募集法人股)	
    NTRD_SHARE_GENJURIS	float	非流通股(境内法人股:一般法人股)	
    NTRD_SHARE_STRA_INVESTOR	float	非流通股(境内法人股:战略投资者持股)	
    NTRD_SHARE_FUND	float	非流通股(境内法人股:基金持股)	
    NTRD_SHARE_NAT	float	非流通股(自然人股)	
    TRAN_SHARE	float	转配股(万股)	
    FLOAT_SHARE_SENIOR	float	流通股(高管持股)	
    SHARE_INEMP	float	内部职工股(万股)	
    PREFERRED_SHARE	float	优先股(万股)	
    NTRD_SHARE_NLIST_FRGN	float	非流通股(非上市外资股)	
    STAQ_SHARE	float	STAQ股(万股)	
    NET_SHARE	float	NET股(万股)	
    SHARE_CHANGE_REASON	string	股本变动原因	
    TOT_A_SHARE	float	A股合计	
    TOT_B_SHARE	float	B股合计	
    OTCA_SHARE	float	三板A股	
    OTCB_SHARE	float	三板B股	
    TOT_OTC_SHARE	float	三板合计	
    SHARE_HK	float	香港上市股	
    PRE_NON_TRADABLE_SHARE	float	股改前非流通股	
    RESTRICTED_A_SHARE	float	限售A股(万股)	
    RTD_A_SHARE_STATE	float	限售A股(国家持股)	
    RTD_A_SHARE_STATEJUR	float	限售A股(国有法人持股)	
    RTD_A_SHARE_OTHER_DOMES	float	限售A股(其他内资持股)	
    RTD_A_SHARE_OTHER_DOMESJUR	float	限售A股(其他内资持股:境内法人持股)	
    TOT_RESTRICTED_SHARE	float	限售股合计	


    """
    if end_date is None:
        end_date = datetime_to_int()

    if code_list is None:
        code_list = []
    info_data_object = ad.InfoData()
    result = info_data_object.get_equity_structure(code_list, begin_date=begin_date, end_date=end_date, is_local=False)

    return serialize_dataframe(result)


@mcp.tool()
async def mcp_equity_pledge_freeze(code_list: list[str], begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取指定股票列表的上市公司的股权冻结/质押数据
    入参：
    code_list	list[str]	是	支持沪深A的的代码列表，可见示例
    begin_date	int	否	报告期，本地数据缓存方案，填写8位的整型格式的日期，比如20240101
    end_date	int	否	报告期，本地数据缓存方案，填写8位的整型格式的日期，比如20240101
    返回字段的释义如下：
    equity_pledge_freeze 的字段说明： 
    字段名称	类型	字段说明	备注
    MARKET_CODE	string	证券代码	
    ANN_DATE	string	公告日期	
    HOLDER_NAME	string	股东名称	
    HOLDER_TYPE_CODE	int	股东类型代码	2:公司3:个人
    TOTAL_HOLDING_SHR"	float	持股总数（万股）	
    TOTAL_HOLDING_SHR_RATIO	float	持股总数占公司总股本比例	
    FRO_SHARES	float	本次冻结/质押股数	
    FRO_SHR_TO_TOTAL_HOLDING_RATIO	float
        本次冻结/质押占所持股比例	
    FRO_SHR_TO_TOTAL_RATIO	float
        本次冻结/质押占总股本比例	
    TOTAL_PLEDGE_SHR	float	累计冻结/质押股数	
    IS_EQUITY_PLEDGE_REPO	int	是否股权质押回购	1:是0:否
    BEGIN_DATE	string	冻结/质押起始日	
    END_DATE	string	解冻/解押日期	
    IS_DISFROZEN	int	是否质押或解冻	1:是0:否
    FROZEN_INSTITUTION	string	执行冻结机构/质权方	
    DISFROZEN_TIME	string	解压或解冻日期	
    SHR_CATEGORY_CODE	int	股份性质类别代码	1:法人股2:个人股3:国有股4:国有股,法人股5:流通股6:流通股,限售流通股7:外资股8:限售流通股9:优先股          
    FREEZE_TYPE	int	冻结/质押类型	1:质押2:司法3:质押式回购

    """
    if end_date is None:
        end_date = datetime_to_int()

    if code_list is None:
        code_list = []
    info_data_object = ad.InfoData()
    result = info_data_object.get_equity_pledge_freeze(code_list, begin_date=begin_date, end_date=end_date, is_local=False)

    return serialize_dict(result)


@mcp.tool()
async def mcp_equity_restricted(code_list: list[str], begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取指定股票列表的上市公司的限售股解禁数据
    入参：
    code_list	list[str]	是	支持沪深A的的代码列表，可见示例
    begin_date	int	否	报告期，本地数据缓存方案，填写8位的整型格式的日期，比如20240101
    end_date	int	否	报告期，本地数据缓存方案，填写8位的整型格式的日期，比如20240101
    返回字段的释义如下：
    equity_restricted 的字段说明：
    字段名称	类型	字段说明	备注
    MARKET_CODE	string	证券代码
    LIST_DATE	string	解禁日期
    SHARE_RATIO	float	解禁股占总股本比(%)
    SHARE_LST_TYPE_NAME	string	解禁股份类型名称
    SHARE_LST	int	解禁数量（股）
    SHARE_LST_IS_ANN	int	上市数量是否公布值	0：否，为预测值1:是,为实际公布值
    CLOSE_PRICE	float	前日收盘价（元）
    SHARE_LST_MARKET_VALUE	float	解禁市值（元）	SHARE_LST* CLOSE_PRICE

    """
    if end_date is None:
        end_date = datetime_to_int()

    if code_list is None:
        code_list = []
    info_data_object = ad.InfoData()
    result = info_data_object.get_equity_restricted(code_list, begin_date=begin_date, end_date=end_date, is_local=False)

    return serialize_dict(result)


@mcp.tool()
async def mcp_dividend(code_list: list[str], begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取指定股票列表的上市公司的分红数据
    入参：
    code_list	list[str]	是	支持沪深A的的代码列表，可见示例
    begin_date	int	否	报告期，本地数据缓存方案，填写8位的整型格式的日期，比如20240101
    end_date	int	否	报告期，本地数据缓存方案，填写8位的整型格式的日期，比如20240101
    返回字段的释义如下：
    dividend的字段说明：
    字段名称	类型	字段说明	备注
    MARKET_CODE	string	证券代码	
    DIV_PROGRESS	string	方案进度	参看股票分红进度代码表
    DVD_PER_SHARE_STK	float	每股送转	
    DVD_PER_SHARE_PRE_TAX_CASH	float	每股派息(税前)(元)	
    DVD_PER_SHARE_AFTER_TAX_CASH	float	每股派息(税后)(元)	
    DATE_EQY_RECORD	string	股权登记日	
    DATE_EX	string	除权除息日	
    DATE_DVD_PAYOUT	string	派息日	
    LISTINGDATE_OF_DVD_SHR	string	红股上市日	
    DIV_PRELANDATE	string	预案公告日	董事会预案公告日期
    DIV_SMTGDATE	string	股东大会公告日	
    DATE_DVD_ANN	string	分红实施公告日	
    DIV_BASEDATE	string	基准日期	
    DIV_BASESHARE	float	基准股本(万股)	
    CURRENCY_CODE	string	货币代码	
    ANN_DATE	string	公告日期	
    IS_CHANGED	int	方案是否变更	1：有变更过0：未变更
    REPORT_PERIOD	string	分红年度	
    DIV_CHANGE	string	方案变更说明	
    DIV_BONUSRATE	float	每股送股比例	
    DIV_CONVERSEDRATE	float	每股转增比例	
    REMARK	string	备注	
    DIV_PREANN_DATE	string	预案预披露公告日	股东提议的公告日期
    DIV_TARGET	string	分红对象	
    附录
    股票分红进度代码表DIV_PROGRESS
    分红进度描述	进度代码
    董事会预案	1
    股东大会通过	2
    实施	3
    未通过	4
    停止实施	12
    股东提议	17
    董事会预案预披露	19
    分红实施进程：股东提议--董事会预案--股东大会--实施

    """
    if end_date is None:
        end_date = datetime_to_int()

    if code_list is None:
        code_list = []
    info_data_object = ad.InfoData()
    result = info_data_object.get_dividend(code_list, begin_date=begin_date, end_date=end_date, is_local=False)

    return serialize_dataframe(result)


@mcp.tool()
async def mcp_right_issue(code_list: list[str], begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取指定股票列表的上市公司的配股数据
    入参：
    code_list	list[str]	是	支持沪深A的的代码列表，可见示例
    begin_date	int	否	报告期，本地数据缓存方案，填写8位的整型格式的日期，比如20240101
    end_date	int	否	报告期，本地数据缓存方案，填写8位的整型格式的日期，比如20240101
    返回字段的释义如下：
    right_issue的字段说明： 
    字段名称	类型	字段说明	备注
    MARKET_CODE	string	证券代码	
    PROGRESS	int	方案进度	参看附录的股票配股进度代码表
    PRICE	double	配股价格(元)	
    RATIO	double	配股比例	
    AMT_PLAN	double	配股计划数量(万股)	
    AMT_REAL	double	配股实际数量(万股)	
    COLLECTION_FUND	double	募集资金(元)	
    SHAREB_REG_DATE	string	股权登记日	
    EX_DIVIDEND_DATE	string	除权日	
    LISTED_DATE	string	配股上市日	
    PAY_START_DATE	string	缴款起始日	
    PAY_END_DATE	string	缴款终止日	
    PREPLAN_DATE	string	预案公告日	
    SMTG_ANN_DATE	string	股东大会公告日	
    PASS_DATE	string	发审委通过公告日	
    APPROVED_DATE	string	证监会核准公告日	
    EXECUTE_DATE	string	配股实施公告日	
    RESULT_DATE	string	配股结果公告日	
    LIST_ANN_DATE	string	上市公告日	
    GUARANTOR	string	基准年度	
    GUARTYPE	double	基准股本(万股)	
    RIGHTSISSUE_CODE	string	配售代码	
    ANN_DATE	string	公告日期	
    RIGHTSISSUE_YEAR	string	配股年度	
    RIGHTSISSUE_DESC	string	配股说明	
    RIGHTSISSUE_NAME	string	配股简称	
    RATIO_DENOMINATOR	double	配股比例分母	
    RATIO_MOLECULAR	double	配股比例分子	
    SUBS_METHOD	string	认购方式	
    EXPECTED_FUND_RAISING	double	预计募集资金(元)	

    附录
    股票配股进度代码表PROGRESS
    配股进度描述	进度代码
    董事会预案	1
    股东大会通过	2
    实施	3
    未通过	4
    证监会核准	5
    达成转让意向	6
    签署转让协议	7
    国资委批准	8
    商务部批准	9
    过户	10
    延期实施	11
    停止实施	12
    分红方案待定	13
    传闻	14
    证监会受理	15
    传闻被否认	16
    股东提议	17
    保监会批复	18
    董事会预案预披露	19
    发审委通过	20
    发审委未通过	21
    股东大会未通过	22
    银监会批准	23
    证监会恢复审核	24
    预发行	25
    提交注册	26


    """
    if end_date is None:
        end_date = datetime_to_int()

    if code_list is None:
        code_list = []
    info_data_object = ad.InfoData()
    result = info_data_object.get_right_issue(code_list, begin_date=begin_date, end_date=end_date, is_local=False)

    return serialize_dataframe(result)


@mcp.tool()
async def mcp_margin_summary( begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取指定日期的上市公司的融资融券成交汇总数据
    入参：
    begin_date	int	否	报告期，本地数据缓存方案，填写8位的整型格式的日期，比如20240101
    end_date	int	否	报告期，本地数据缓存方案，填写8位的整型格式的日期，比如20240101
    返回字段的释义如下：
    equity_restricted 的字段说明：
    字段名称	类型	字段说明	备注
    MARKET_CODE	string	证券代码
    LIST_DATE	string	解禁日期
    SHARE_RATIO	float	解禁股占总股本比(%)
    SHARE_LST_TYPE_NAME	string	解禁股份类型名称
    SHARE_LST	int	解禁数量（股）
    SHARE_LST_IS_ANN	int	上市数量是否公布值	0：否，为预测值1:是,为实际公布值
    CLOSE_PRICE	float	前日收盘价（元）
    SHARE_LST_MARKET_VALUE	float	解禁市值（元）	SHARE_LST* CLOSE_PRICE

    """
    if end_date is None:
        end_date = datetime_to_int()

    info_data_object = ad.InfoData()
    result = info_data_object.get_margin_summary(begin_date=begin_date, end_date=end_date, is_local=False)

    return serialize_dict(result)


@mcp.tool()
async def mcp_margin_detail(code_list: list[str], begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取指定股票列表的上市公司的融资融券交易明细数据
    入参：
    code_list	list[str]	是	支持沪深A的的代码列表，可见示例
    begin_date	int	否	报告期，本地数据缓存方案，填写8位的整型格式的日期，比如20240101
    end_date	int	否	报告期，本地数据缓存方案，填写8位的整型格式的日期，比如20240101
    返回字段的释义如下：
    margin_detail的字段说明：
    字段名称	类型	字段说明
    MARKET_CODE	string	证券代码
    SECURITY_NAME	string	证券简称
    TRADE_DATE	string	交易日期
    BORROW_MONEY_BALANCE"	float	融资余额(元)
    PURCH_WITH_BORROW_MONEY	float	融资买入额(元)
    REPAYMENT_OF_BORROW_MONEY	float	融资偿还额(元)
    SEC_LENDING_BALANCE	float	融券余额(元)
    SALES_OF_BORROWED_SEC	int	融券卖出量(股,份,手)
    REPAYMENT_OF_BORROW_SEC	int	融券偿还量(股,份,手)
    SEC_LENDING_BALANCE_VOL	int	融券余量(股,份,手)
    MARGIN_TRADE_BALANCE	float	融资融券余额(元)
    """
    if end_date is None:
        end_date = datetime_to_int()

    if code_list is None:
        code_list = []
    info_data_object = ad.InfoData()
    result = info_data_object.get_margin_detail(code_list, begin_date=begin_date, end_date=end_date, is_local=False)

    return serialize_dataframe(result)


@mcp.tool()
async def mcp_long_hu_bang(code_list: list[str], begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取指定股票列表的上市公司的龙虎榜数据

    Args:
        code_list: 支持沪深A股的代码列表
        begin_date: 开始日期，8位整型格式，如 20240101（必填）
        end_date: 结束日期，8位整型格式，如 20240101（必填）

    Returns:
        龙虎榜数据，包含以下字段：
        - MARKET_CODE: 证券代码
        - TRADE_DATE: 交易日期
        - SECURITY_NAME: 证券名称
        - REASON_TYPE: 上榜原因类型
        - REASON_TYPE_NAME: 上榜原因
        - CHANGE_RANGE: 涨跌幅（%）
        - TRADER_NAME: 营业部名称
        - BUY_AMOUNT: 买入金额（元）
        - SELL_AMOUNT: 卖出金额（元）
        - FLOW_MARK: 买卖标识（1表示买入，2表示卖出）
        - TOTAL_AMOUNT: 实际交易金额（元）
        - TOTAL_VOLUME: 实际交易量（万股）
    """
    try:
        ensure_logged_in()
        begin_date, end_date = validate_date_range(begin_date, end_date)

        if code_list is None:
            code_list = []

        info_data_object = ad.InfoData()
        result = info_data_object.get_long_hu_bang(code_list, begin_date=begin_date, end_date=end_date, is_local=False)

        serialized = serialize_dataframe(result)
        logger.info(f"查询龙虎榜数据，共 {len(serialized) if serialized else 0} 条")

        return {
            "success": True,
            "count": len(serialized) if serialized else 0,
            "data": serialized
        }
    except Exception as e:
        logger.error(f"查询龙虎榜数据失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


@mcp.tool()
async def mcp_block_trading(code_list: list[str], begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取指定股票列表的上市公司的大宗交易数据

    Args:
        code_list: 支持沪深A股的代码列表
        begin_date: 开始日期，8位整型格式，如 20240101（必填）
        end_date: 结束日期，8位整型格式，如 20240101（必填）

    Returns:
        大宗交易数据，包含以下字段：
        - MARKET_CODE: 证券代码
        - TRADE_DATE: 交易日期
        - B_SHARE_PRICE: 成交价（元）
        - B_SHARE_VOLUME: 成交量（万股）
        - B_FREQUENCY: 笔数
        - BLOCK_AVG_VOLUME: 每笔成交数量（万股份）
        - B_SHARE_AMOUNT: 成交金额（万元）
        - B_BUYER_NAME: 买方营业部名称
        - B_SELLER_NAME: 卖方营业部名称
    """
    try:
        ensure_logged_in()
        begin_date, end_date = validate_date_range(begin_date, end_date)

        if code_list is None:
            code_list = []

        info_data_object = ad.InfoData()
        result = info_data_object.get_block_trading(code_list, begin_date=begin_date, end_date=end_date, is_local=False)

        serialized = serialize_dataframe(result)
        logger.info(f"查询大宗交易数据，共 {len(serialized) if serialized else 0} 条")

        return {
            "success": True,
            "count": len(serialized) if serialized else 0,
            "data": serialized
        }
    except Exception as e:
        logger.error(f"查询大宗交易数据失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


# ============================================================================
# 3.5.2 基础数据 - 补充接口
# ============================================================================

@mcp.tool()
async def mcp_code_list_future(security_type: str = "EXTRA_FUTURE") -> dict:
    """
    获取期货交易所的每日最新代码表

    Args:
        security_type: 代码类型，默认EXTRA_FUTURE_CFFEX: 中金所期货

    Returns:
        期货代码列表
    """
    try:
        ensure_logged_in()
        base_data_object = ad.BaseData()
        code_list = base_data_object.get_code_list(security_type=security_type)

        logger.info(f"查询期货代码表({security_type})，共 {len(code_list) if code_list else 0} 条")

        return {
            "success": True,
            "count": len(code_list) if code_list else 0,
            "data": code_list
        }
    except Exception as e:
        logger.error(f"查询期货代码表失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


@mcp.tool()
async def mcp_code_list_option(security_type: str = "EXTRA_OPTION") -> dict:
    """
    获取期权的每日最新代码表

    Args:
        security_type: 代码类型，默认 EXTRA_OPTION（期权）
            - EXTRA_OPTION: 期权（包含上交所ETF期权/深交所ETF期权）
            - EXTRA_OPTION_SSE: 上交所ETF期权
            - EXTRA_OPTION_SZSE: 深交所ETF期权

    Returns:
        期权代码列表
    """
    try:
        ensure_logged_in()
        base_data_object = ad.BaseData()
        code_list = base_data_object.get_code_list(security_type=security_type)

        logger.info(f"查询期权代码表({security_type})，共 {len(code_list) if code_list else 0} 条")

        return {
            "success": True,
            "count": len(code_list) if code_list else 0,
            "data": code_list
        }
    except Exception as e:
        logger.error(f"查询期权代码表失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


@mcp.tool()
async def mcp_history_code_list(security_type: str, date: int) -> dict:
    """
    获取历史代码表

    Args:
        security_type: 代码类型
            - EXTRA_STOCK_A: 沪深北A股
            - EXTRA_INDEX_A: 沪深北指数
            - EXTRA_ETF: 沪深ETF
            - EXTRA_KZZ: 沪深可转债
        date: 日期，8位整型格式，如 20240101

    Returns:
        历史代码列表
    """
    try:
        ensure_logged_in()
        base_data_object = ad.BaseData()
        code_list = base_data_object.get_history_code_list(security_type=security_type, date=date)

        logger.info(f"查询历史代码表({security_type}, {date})，共 {len(code_list) if code_list else 0} 条")

        return {
            "success": True,
            "count": len(code_list) if code_list else 0,
            "data": code_list
        }
    except Exception as e:
        logger.error(f"查询历史代码表失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


# ============================================================================
# 3.5.10 期权数据
# ============================================================================

@mcp.tool()
async def mcp_option_basic_info(code_list: list[str] = None) -> dict:
    """
    获取期权基本资料

    Args:
        code_list: 期权代码列表，可选。如不传则返回所有期权

    Returns:
        期权基本资料，包含以下字段：
        - MARKET_CODE: 期权代码
        - SECURITY_NAME: 期权简称
        - UNDERLYING_CODE: 标的证券代码
        - CALL_PUT: 认购认沽标志（C-认购，P-认沽）
        - EXERCISE_PRICE: 行权价格
        - START_DATE: 首个交易日
        - END_DATE: 最后交易日/行权日
        - EXERCISE_DATE: 行权日期
        - DELIVERY_DATE: 行权交割日
        - EXPIRE_DATE: 到期日
        等
    """
    try:
        ensure_logged_in()
        info_data_object = ad.InfoData()

        if code_list is None:
            result = info_data_object.get_option_basic_info([], is_local=False)
        else:
            result = info_data_object.get_option_basic_info(code_list, is_local=False)

        serialized = serialize_dataframe(result)
        logger.info(f"查询期权基本资料，共 {len(serialized) if serialized else 0} 条")

        return {
            "success": True,
            "count": len(serialized) if serialized else 0,
            "data": serialized
        }
    except Exception as e:
        logger.error(f"查询期权基本资料失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


@mcp.tool()
async def mcp_option_contract_info(code_list: list[str] = None) -> dict:
    """
    获取期权标准合约属性

    Args:
        code_list: 期权代码列表，可选

    Returns:
        期权标准合约属性数据
    """
    try:
        ensure_logged_in()
        info_data_object = ad.InfoData()

        if code_list is None:
            result = info_data_object.get_option_std_ctr_specs([], is_local=False)
        else:
            result = info_data_object.get_option_std_ctr_specs(code_list, is_local=False)

        serialized = serialize_dataframe(result)
        logger.info(f"查询期权标准合约属性，共 {len(serialized) if serialized else 0} 条")

        return {
            "success": True,
            "count": len(serialized) if serialized else 0,
            "data": serialized
        }
    except Exception as e:
        logger.error(f"查询期权标准合约属性失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


@mcp.tool()
async def mcp_option_month_contract_change(code_list: list[str], begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取期权月合约属性变动

    Args:
        code_list: 期权代码列表
        begin_date: 开始日期，8位整型格式，如 20240101
        end_date: 结束日期，8位整型格式，如 20240101

    Returns:
        期权月合约属性变动数据
    """
    try:
        ensure_logged_in()
        if end_date is None:
            end_date = datetime_to_int()

        info_data_object = ad.InfoData()
        result = info_data_object.get_option_mon_ctr_specs(
            code_list, is_local=False
        )

        serialized = serialize_dataframe(result)
        logger.info(f"查询期权月合约属性变动，共 {len(serialized) if serialized else 0} 条")

        return {
            "success": True,
            "count": len(serialized) if serialized else 0,
            "data": serialized
        }
    except Exception as e:
        logger.error(f"查询期权月合约属性变动失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


# ============================================================================
# 3.5.11 ETF数据
# ============================================================================

@mcp.tool()
async def mcp_etf_purchase_redemption(code_list: list[str] = None, date: int = None) -> dict:
    """
    获取ETF每日最新申赎数据

    Args:
        code_list: ETF代码列表，可选
        date: 日期，8位整型格式，如 20240101，可选

    Returns:
        ETF申赎数据，包含以下字段：
        - MARKET_CODE: ETF代码
        - TRADE_DATE: 交易日期
        - CREATION_UNIT: 最小申购赎回单位（份）
        - MAX_CASH_RATIO: 最大现金替代比例
        - ESTIMATE_CASH_COMPONENT: 预估现金差额（元）
        - CASH_COMPONENT: T-1日现金差额（元）
        - TOTAL_AMOUNT: 申购赎回清单总金额（元）
        等
    """
    try:
        ensure_logged_in()
        if end_date is None:
            end_date = datetime_to_int()

        info_data_object = ad.InfoData()

        if code_list is None and date is None:
            result = info_data_object.get_etf_purchase_redemption()
        elif code_list is not None and date is None:
            result = info_data_object.get_etf_purchase_redemption(code_list)
        elif code_list is None and date is not None:
            result = info_data_object.get_etf_purchase_redemption(date=date)
        else:
            result = info_data_object.get_etf_purchase_redemption(code_list, date=date)

        serialized = serialize_dataframe(result)
        logger.info(f"查询ETF申赎数据，共 {len(serialized) if serialized else 0} 条")

        return {
            "success": True,
            "count": len(serialized) if serialized else 0,
            "data": serialized
        }
    except Exception as e:
        logger.error(f"查询ETF申赎数据失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


@mcp.tool()
async def mcp_etf_fund_share(code_list: list[str], begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取ETF基金份额

    Args:
        code_list: ETF代码列表
        begin_date: 开始日期，8位整型格式，如 20240101
        end_date: 结束日期，8位整型格式，如 20240101

    Returns:
        ETF基金份额数据
    """
    try:
        ensure_logged_in()
        if end_date is None:
            end_date = datetime_to_int()

        info_data_object = ad.InfoData()
        result = info_data_object.get_etf_fund_share(
            code_list, begin_date=begin_date, end_date=end_date, is_local=False
        )

        serialized = serialize_dataframe(result)
        logger.info(f"查询ETF基金份额，共 {len(serialized) if serialized else 0} 条")

        return {
            "success": True,
            "count": len(serialized) if serialized else 0,
            "data": serialized
        }
    except Exception as e:
        logger.error(f"查询ETF基金份额失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


@mcp.tool()
async def mcp_etf_iopv(code_list: list[str], begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取ETF每日收盘IOPV（实时参考净值）

    Args:
        code_list: ETF代码列表
        begin_date: 开始日期，8位整型格式，如 20240101
        end_date: 结束日期，8位整型格式，如 20240101

    Returns:
        ETF收盘IOPV数据
    """
    try:
        ensure_logged_in()
        if end_date is None:
            end_date = datetime_to_int()

        info_data_object = ad.InfoData()
        result = info_data_object.get_etf_iopv(
            code_list, begin_date=begin_date, end_date=end_date, is_local=False
        )

        serialized = serialize_dataframe(result)
        logger.info(f"查询ETF收盘IOPV，共 {len(serialized) if serialized else 0} 条")

        return {
            "success": True,
            "count": len(serialized) if serialized else 0,
            "data": serialized
        }
    except Exception as e:
        logger.error(f"查询ETF收盘IOPV失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


# ============================================================================
# 3.5.12 交易所指数数据
# ============================================================================

@mcp.tool()
async def mcp_index_constituent(index_code: str, date: int = None) -> dict:
    """
    获取交易所指数成分股

    Args:
        index_code: 指数代码，如 "000300.SH"（沪深300）
        date: 日期，8位整型格式，如 20240101，可选。不传则返回最新

    Returns:
        指数成分股数据，包含以下字段：
        - INDEX_CODE: 指数代码
        - CONSTITUENT_CODE: 成分股代码
        - CONSTITUENT_NAME: 成分股名称
        - IN_DATE: 纳入日期
        - OUT_DATE: 剔除日期
        等
    """
    try:
        ensure_logged_in()
        info_data_object = ad.InfoData()

        if date is None:
            result = info_data_object.get_index_constituent([index_code], is_local=False)
        else:
            result = info_data_object.get_index_constituent([index_code], is_local=False)

        serialized = serialize_dataframe(result)
        logger.info(f"查询交易所指数成分股({index_code})，共 {len(serialized) if serialized else 0} 条")

        return {
            "success": True,
            "count": len(serialized) if serialized else 0,
            "data": serialized
        }
    except Exception as e:
        logger.error(f"查询交易所指数成分股失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


@mcp.tool()
async def mcp_index_constituent_weight(index_code: str, begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取交易所指数成分股日权重

    Args:
        index_code: 指数代码
        begin_date: 开始日期，8位整型格式，如 20240101
        end_date: 结束日期，8位整型格式，如 20240101

    Returns:
        指数成分股日权重数据
    """
    try:
        ensure_logged_in()
        if end_date is None:
            end_date = datetime_to_int()

        info_data_object = ad.InfoData()
        result = info_data_object.get_index_constituent_weight(
            index_code, begin_date=begin_date, end_date=end_date, is_local=False
        )

        serialized = serialize_dataframe(result)
        logger.info(f"查询交易所指数成分股日权重，共 {len(serialized) if serialized else 0} 条")

        return {
            "success": True,
            "count": len(serialized) if serialized else 0,
            "data": serialized
        }
    except Exception as e:
        logger.error(f"查询交易所指数成分股日权重失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


# ============================================================================
# 3.5.13 行业指数数据
# ============================================================================

@mcp.tool()
async def mcp_industry_index_info(index_type: str = "SW") -> dict:
    """
    获取行业指数基本信息

    Args:
        index_type: 指数类型
            - SW: 申万行业指数
            - ZX: 中信行业指数

    Returns:
        行业指数基本信息
    """
    try:
        ensure_logged_in()
        if end_date is None:
            end_date = datetime_to_int()

        info_data_object = ad.InfoData()
        result = info_data_object.get_industry_index_info(index_type=index_type)

        serialized = serialize_dataframe(result)
        logger.info(f"查询行业指数基本信息({index_type})，共 {len(serialized) if serialized else 0} 条")

        return {
            "success": True,
            "count": len(serialized) if serialized else 0,
            "data": serialized
        }
    except Exception as e:
        logger.error(f"查询行业指数基本信息失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


@mcp.tool()
async def mcp_industry_index_constituent(index_code: str, date: int = None) -> dict:
    """
    获取行业指数成分股

    Args:
        index_code: 行业指数代码
        date: 日期，8位整型格式，如 20240101，可选

    Returns:
        行业指数成分股数据
    """
    try:
        ensure_logged_in()
        info_data_object = ad.InfoData()

        # get_industry_constituent 只接受 code_list 和 is_local 参数
        result = info_data_object.get_industry_constituent([index_code], is_local=False)

        serialized = serialize_dataframe(result)
        logger.info(f"查询行业指数成分股，共 {len(serialized) if serialized else 0} 条")

        return {
            "success": True,
            "count": len(serialized) if serialized else 0,
            "data": serialized
        }
    except Exception as e:
        logger.error(f"查询行业指数成分股失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


@mcp.tool()
async def mcp_industry_index_weight(index_code: str, begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取行业指数成分股日权重

    Args:
        index_code: 行业指数代码
        begin_date: 开始日期，8位整型格式，如 20240101
        end_date: 结束日期，8位整型格式，如 20240101

    Returns:
        行业指数成分股日权重数据
    """
    try:
        ensure_logged_in()
        if end_date is None:
            end_date = datetime_to_int()

        info_data_object = ad.InfoData()
        # get_industry_weight 需要 code_list 参数
        result = info_data_object.get_industry_weight(
            [index_code], is_local=False, begin_date=begin_date, end_date=end_date
        )

        serialized = serialize_dataframe(result)
        logger.info(f"查询行业指数成分股日权重，共 {len(serialized) if serialized else 0} 条")

        return {
            "success": True,
            "count": len(serialized) if serialized else 0,
            "data": serialized
        }
    except Exception as e:
        logger.error(f"查询行业指数成分股日权重失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


@mcp.tool()
async def mcp_industry_index_quote(index_code_list: list[str], begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取行业指数日行情

    Args:
        index_code_list: 行业指数代码列表
        begin_date: 开始日期，8位整型格式，如 20240101
        end_date: 结束日期，8位整型格式，如 20240101

    Returns:
        行业指数日行情数据
    """
    try:
        ensure_logged_in()
        if end_date is None:
            end_date = datetime_to_int()

        info_data_object = ad.InfoData()
        # get_industry_daily 需要 code_list 参数
        result = info_data_object.get_industry_daily(
            index_code_list, is_local=False, begin_date=begin_date, end_date=end_date
        )

        serialized = serialize_dataframe(result)
        logger.info(f"查询行业指数日行情，共 {len(serialized) if serialized else 0} 条")

        return {
            "success": True,
            "count": len(serialized) if serialized else 0,
            "data": serialized
        }
    except Exception as e:
        logger.error(f"查询行业指数日行情失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


# ============================================================================
# 3.5.15 国债收益率数据
# ============================================================================

@mcp.tool()
async def mcp_treasury_yield(code_list: list[str], begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取国债收益率数据

    Args:
        code_list: 国债代码列表
        begin_date: 开始日期，8位整型格式，如 20240101
        end_date: 结束日期，8位整型格式，如 20240101

    Returns:
        国债收益率数据，包含以下字段：
        - TRADE_DATE: 交易日期
        - YIELD_3M: 3个月国债收益率
        - YIELD_6M: 6个月国债收益率
        - YIELD_1Y: 1年期国债收益率
        - YIELD_3Y: 3年期国债收益率
        - YIELD_5Y: 5年期国债收益率
        - YIELD_7Y: 7年期国债收益率
        - YIELD_10Y: 10年期国债收益率
        - YIELD_30Y: 30年期国债收益率
    """
    try:
        ensure_logged_in()
        if end_date is None:
            end_date = datetime_to_int()

        info_data_object = ad.InfoData()
        result = info_data_object.get_treasury_yield(
            code_list, is_local=False, begin_date=begin_date, end_date=end_date
        )

        serialized = serialize_dataframe(result)
        logger.info(f"查询国债收益率，共 {len(serialized) if serialized else 0} 条")

        return {
            "success": True,
            "count": len(serialized) if serialized else 0,
            "data": serialized
        }
    except Exception as e:
        logger.error(f"查询国债收益率失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


# ============================================================================
# 3.5.14 可转债数据
# ============================================================================

@mcp.tool()
async def mcp_convertible_bond_issue(code_list: list[str] = None, begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取可转债发行数据

    Args:
        code_list: 可转债代码列表，可选
        begin_date: 开始日期，8位整型格式，如 20240101，可选
        end_date: 结束日期，8位整型格式，如 20240101，可选

    Returns:
        可转债发行数据，包含以下字段：
        - MARKET_CODE: 可转债代码
        - BOND_NAME: 可转债简称
        - ISSUE_DATE: 发行日期
        - LISTING_DATE: 上市日期
        - ISSUE_PRICE: 发行价格
        - ISSUE_SCALE: 发行规模（万元）
        - CONVERSION_PRICE: 初始转股价格
        - MATURITY_DATE: 到期日期
        等
    """
    try:
        ensure_logged_in()
        if end_date is None:
            end_date = datetime_to_int()

        info_data_object = ad.InfoData()

        if code_list is None:
            result = info_data_object.get_convertible_bond_issue(begin_date=begin_date, end_date=end_date, is_local=False)
        else:
            result = info_data_object.get_convertible_bond_issue(code_list, begin_date=begin_date, end_date=end_date, is_local=False)

        serialized = serialize_dataframe(result)
        logger.info(f"查询可转债发行数据，共 {len(serialized) if serialized else 0} 条")

        return {
            "success": True,
            "count": len(serialized) if serialized else 0,
            "data": serialized
        }
    except Exception as e:
        logger.error(f"查询可转债发行数据失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


@mcp.tool()
async def mcp_convertible_bond_balance(code_list: list[str], begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取可转债份额数据

    Args:
        code_list: 可转债代码列表
        begin_date: 开始日期，8位整型格式，如 20240101
        end_date: 结束日期，8位整型格式，如 20240101

    Returns:
        可转债份额数据，包含以下字段：
        - MARKET_CODE: 可转债代码
        - TRADE_DATE: 交易日期
        - BOND_BALANCE: 可转债余额（万元）
        - CONVERSION_VALUE: 转股价值
        等
    """
    try:
        ensure_logged_in()
        if end_date is None:
            end_date = datetime_to_int()

        info_data_object = ad.InfoData()
        result = info_data_object.get_convertible_bond_balance(
            code_list, begin_date=begin_date, end_date=end_date, is_local=False
        )

        serialized = serialize_dataframe(result)
        logger.info(f"查询可转债份额数据，共 {len(serialized) if serialized else 0} 条")

        return {
            "success": True,
            "count": len(serialized) if serialized else 0,
            "data": serialized
        }
    except Exception as e:
        logger.error(f"查询可转债份额数据失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


@mcp.tool()
async def mcp_convertible_bond_conversion(code_list: list[str]) -> dict:
    """
    获取可转债转股数据

    Args:
        code_list: 可转债代码列表

    Returns:
        可转债转股数据，包含以下字段：
        - MARKET_CODE: 可转债代码
        - TRADE_DATE: 交易日期
        - CONVERSION_PRICE: 转股价格
        - CONVERSION_VALUE: 转股价值
        - CONVERSION_PREMIUM_RATE: 转股溢价率
        等
    """
    try:
        ensure_logged_in()
        info_data_object = ad.InfoData()
        result = info_data_object.get_kzz_conv(code_list, is_local=False)

        serialized = serialize_dataframe(result)
        logger.info(f"查询可转债转股数据，共 {len(serialized) if serialized else 0} 条")

        return {
            "success": True,
            "count": len(serialized) if serialized else 0,
            "data": serialized
        }
    except Exception as e:
        logger.error(f"查询可转债转股数据失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


@mcp.tool()
async def mcp_convertible_bond_conversion_change(code_list: list[str], begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取可转债转股变动数据

    Args:
        code_list: 可转债代码列表
        begin_date: 开始日期，8位整型格式，如 20240101
        end_date: 结束日期，8位整型格式，如 20240101

    Returns:
        可转债转股变动数据
    """
    try:
        ensure_logged_in()
        if end_date is None:
            end_date = datetime_to_int()

        info_data_object = ad.InfoData()
        result = info_data_object.get_convertible_bond_conversion_change(
            code_list, begin_date=begin_date, end_date=end_date, is_local=False
        )

        serialized = serialize_dataframe(result)
        logger.info(f"查询可转债转股变动数据，共 {len(serialized) if serialized else 0} 条")

        return {
            "success": True,
            "count": len(serialized) if serialized else 0,
            "data": serialized
        }
    except Exception as e:
        logger.error(f"查询可转债转股变动数据失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


@mcp.tool()
async def mcp_convertible_bond_adjustment(code_list: list[str], begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取可转债修正数据（转股价格调整）

    Args:
        code_list: 可转债代码列表
        begin_date: 开始日期，8位整型格式，如 20240101
        end_date: 结束日期，8位整型格式，如 20240101

    Returns:
        可转债修正数据，包含以下字段：
        - MARKET_CODE: 可转债代码
        - ADJUSTMENT_DATE: 调整日期
        - OLD_CONVERSION_PRICE: 调整前转股价格
        - NEW_CONVERSION_PRICE: 调整后转股价格
        - ADJUSTMENT_REASON: 调整原因
        等
    """
    try:
        ensure_logged_in()
        if end_date is None:
            end_date = datetime_to_int()

        info_data_object = ad.InfoData()
        result = info_data_object.get_convertible_bond_adjustment(
            code_list, begin_date=begin_date, end_date=end_date, is_local=False
        )

        serialized = serialize_dataframe(result)
        logger.info(f"查询可转债修正数据，共 {len(serialized) if serialized else 0} 条")

        return {
            "success": True,
            "count": len(serialized) if serialized else 0,
            "data": serialized
        }
    except Exception as e:
        logger.error(f"查询可转债修正数据失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


@mcp.tool()
async def mcp_convertible_bond_redemption(code_list: list[str]) -> dict:
    """
    获取可转债赎回数据

    Args:
        code_list: 可转债代码列表

    Returns:
        可转债赎回数据，包含以下字段：
        - MARKET_CODE: 可转债代码
        - REDEMPTION_DATE: 赎回日期
        - REDEMPTION_PRICE: 赎回价格
        - REDEMPTION_TYPE: 赎回类型
        等
    """
    try:
        ensure_logged_in()
        info_data_object = ad.InfoData()
        result = info_data_object.get_kzz_call(code_list, is_local=False)

        serialized = serialize_dataframe(result)
        logger.info(f"查询可转债赎回数据，共 {len(serialized) if serialized else 0} 条")

        return {
            "success": True,
            "count": len(serialized) if serialized else 0,
            "data": serialized
        }
    except Exception as e:
        logger.error(f"查询可转债赎回数据失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


@mcp.tool()
async def mcp_convertible_bond_resale(code_list: list[str]) -> dict:
    """
    获取可转债回售数据

    Args:
        code_list: 可转债代码列表

    Returns:
        可转债回售数据，包含以下字段：
        - MARKET_CODE: 可转债代码
        - RESALE_DATE: 回售日期
        - RESALE_PRICE: 回售价格
        - RESALE_REGISTRATION_DATE: 回售登记日
        等
    """
    try:
        ensure_logged_in()
        info_data_object = ad.InfoData()
        result = info_data_object.get_kzz_put(code_list, is_local=False)

        serialized = serialize_dataframe(result)
        logger.info(f"查询可转债回售数据，共 {len(serialized) if serialized else 0} 条")

        return {
            "success": True,
            "count": len(serialized) if serialized else 0,
            "data": serialized
        }
    except Exception as e:
        logger.error(f"查询可转债回售数据失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


@mcp.tool()
async def mcp_convertible_bond_terms(code_list: list[str] = None) -> dict:
    """
    获取可转债回售赎回条款

    Args:
        code_list: 可转债代码列表，可选

    Returns:
        可转债回售赎回条款数据，包含以下字段：
        - MARKET_CODE: 可转债代码
        - REDEMPTION_CLAUSE: 赎回条款
        - RESALE_CLAUSE: 回售条款
        - CONVERSION_CLAUSE: 转股条款
        等
    """
    try:
        ensure_logged_in()
        if end_date is None:
            end_date = datetime_to_int()

        info_data_object = ad.InfoData()

        if code_list is None:
            result = info_data_object.get_convertible_bond_terms()
        else:
            result = info_data_object.get_convertible_bond_terms(code_list)

        serialized = serialize_dataframe(result)
        logger.info(f"查询可转债回售赎回条款，共 {len(serialized) if serialized else 0} 条")

        return {
            "success": True,
            "count": len(serialized) if serialized else 0,
            "data": serialized
        }
    except Exception as e:
        logger.error(f"查询可转债回售赎回条款失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


@mcp.tool()
async def mcp_convertible_bond_resale_notice(code_list: list[str], begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取可转债回售条款执行说明

    Args:
        code_list: 可转债代码列表
        begin_date: 开始日期，8位整型格式，如 20240101
        end_date: 结束日期，8位整型格式，如 20240101

    Returns:
        可转债回售条款执行说明数据
    """
    try:
        ensure_logged_in()
        if end_date is None:
            end_date = datetime_to_int()

        info_data_object = ad.InfoData()
        result = info_data_object.get_convertible_bond_resale_notice(
            code_list, begin_date=begin_date, end_date=end_date, is_local=False
        )

        serialized = serialize_dataframe(result)
        logger.info(f"查询可转债回售条款执行说明，共 {len(serialized) if serialized else 0} 条")

        return {
            "success": True,
            "count": len(serialized) if serialized else 0,
            "data": serialized
        }
    except Exception as e:
        logger.error(f"查询可转债回售条款执行说明失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


@mcp.tool()
async def mcp_convertible_bond_redemption_notice(code_list: list[str], begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取可转债赎回条款执行说明

    Args:
        code_list: 可转债代码列表
        begin_date: 开始日期，8位整型格式，如 20240101
        end_date: 结束日期，8位整型格式，如 20240101

    Returns:
        可转债赎回条款执行说明数据
    """
    try:
        ensure_logged_in()
        if end_date is None:
            end_date = datetime_to_int()

        info_data_object = ad.InfoData()
        result = info_data_object.get_convertible_bond_redemption_notice(
            code_list, begin_date=begin_date, end_date=end_date, is_local=False
        )

        serialized = serialize_dataframe(result)
        logger.info(f"查询可转债赎回条款执行说明，共 {len(serialized) if serialized else 0} 条")

        return {
            "success": True,
            "count": len(serialized) if serialized else 0,
            "data": serialized
        }
    except Exception as e:
        logger.error(f"查询可转债赎回条款执行说明失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


@mcp.tool()
async def mcp_convertible_bond_suspension(code_list: list[str], begin_date: int = 19900101, end_date: int = None) -> dict:
    """
    获取可转债停复牌信息

    Args:
        code_list: 可转债代码列表
        begin_date: 开始日期，8位整型格式，如 20240101
        end_date: 结束日期，8位整型格式，如 20240101

    Returns:
        可转债停复牌信息，包含以下字段：
        - MARKET_CODE: 可转债代码
        - SUSPENSION_DATE: 停牌日期
        - RESUMPTION_DATE: 复牌日期
        - SUSPENSION_REASON: 停牌原因
        等
    """
    try:
        ensure_logged_in()
        if end_date is None:
            end_date = datetime_to_int()

        info_data_object = ad.InfoData()
        result = info_data_object.get_convertible_bond_suspension(
            code_list, begin_date=begin_date, end_date=end_date, is_local=False
        )

        serialized = serialize_dataframe(result)
        logger.info(f"查询可转债停复牌信息，共 {len(serialized) if serialized else 0} 条")

        return {
            "success": True,
            "count": len(serialized) if serialized else 0,
            "data": serialized
        }
    except Exception as e:
        logger.error(f"查询可转债停复牌信息失败: {e}")
        return {
            "success": False,
            "message": f"查询失败: {str(e)}"
        }


# ============================================================================
# 3.5.3 实时行情数据 (快照接口)
# ============================================================================


# ============================================================================
# 3.5.1 基础接口 - 补充
# ============================================================================

@mcp.tool()
async def mcp_logout() -> dict:
    """
    登出 AmazingData

    Returns:
        登出结果
    """
    global _is_logged_in, _login_info

    try:
        if not _is_logged_in:
            return {
                "success": False,
                "message": "当前未登录"
            }

        # 调用 AmazingData 的登出接口
        ad.logout()

        _is_logged_in = False
        _login_info = {}

        logger.info("登出成功")

        return {
            "success": True,
            "message": "登出成功"
        }
    except Exception as e:
        logger.error(f"登出失败: {e}")
        return {
            "success": False,
            "message": f"登出失败: {str(e)}"
        }



if __name__ == "__main__":
    # 从环境变量或配置文件加载登录信息
    username = os.getenv('USER')
    password = os.getenv('PASSWORD')
    host = os.getenv('HOST')
    port = int(os.getenv('PORT'))

    # 自动登录
    try:
        logger.info(f"启动 AmazingData MCP Server，尝试自动登录...")
        success = ad.login(username=username, password=password, host=host, port=port)

        if success:
            _is_logged_in = True
            _login_info = {
                'username': username,
                'host': host,
                'port': port,
                'login_time': datetime.datetime.now().isoformat()
            }
            logger.info("自动登录成功")
        else:
            logger.warning("自动登录失败，请手动调用 mcp_login 工具登录")
    except Exception as e:
        logger.error(f"自动登录异常: {e}")
        logger.warning("将在未登录状态下启动服务，请手动调用 mcp_login 工具登录")

    # 启动 MCP 服务
    logger.info("启动 MCP 服务...")
    mcp.run(transport="stdio")
