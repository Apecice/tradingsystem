#!/usr/bin/env python3
import argparse
import csv
import json
import os
import sys
import time
import threading
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any

import requests

try:
	from dotenv import load_dotenv  # type: ignore
	load_dotenv()
except Exception:
	pass

try:
	from dateutil import parser as date_parser  # type: ignore
except Exception:
	print("缺少依赖: python-dateutil。请先安装 requirements.txt 中的依赖。", file=sys.stderr)
	raise

try:
	import pandas as pd  # type: ignore
except Exception:
	print("缺少依赖: pandas。请先安装 requirements.txt 中的依赖。", file=sys.stderr)
	raise


ALPHAVANTAGE_API_URL = "https://www.alphavantage.co/query"


class RateLimiter:
	"""简单的每分钟请求次数限流器，避免触发 Alpha Vantage 免费额度限制。"""

	def __init__(self, calls_per_minute: int = 3) -> None:
		self.interval_seconds = 60.0 / max(1, calls_per_minute)
		self._lock = threading.Lock()
		self._last_call_ts = 0.0

	def wait(self) -> None:
		with self._lock:
			now = time.monotonic()
			wait_seconds = self.interval_seconds - (now - self._last_call_ts)
			if wait_seconds > 0:
				time.sleep(wait_seconds)
			self._last_call_ts = time.monotonic()


def normalize_a_share_symbol(symbol: str) -> str:
	"""将 A 股代码映射为 Alpha Vantage 支持的后缀"""
	s = symbol.strip().upper()
	if not s:
		return s
	if "." in s:
		if s.endswith(".SH"):
			return s[:-3] + ".SHH"
		if s.endswith(".SZ"):
			return s[:-3] + ".SHZ"
		return s
	if s[0] == "6":
		return s + ".SHH"
	if s[0] in ("0", "3"):
		return s + ".SHZ"
	return s


def fetch_alpha_vantage_data(
	function: str,
	symbol: str,
	api_key: str,
	rate_limiter: RateLimiter,
	session: requests.Session,
	extra_params: Optional[Dict[str, Any]] = None,
	max_retries: int = 3,
	timeout_s: int = 20,
) -> Dict[str, Any]:
	"""通用 Alpha Vantage API 调用函数"""
	params = {
		"function": function,
		"symbol": symbol,
		"apikey": api_key,
		"datatype": "json",
	}
	if extra_params:
		params.update(extra_params)

	for attempt in range(1, max_retries + 1):
		rate_limiter.wait()
		try:
			resp = session.get(ALPHAVANTAGE_API_URL, params=params, timeout=timeout_s)
			if resp.status_code != 200:
				print(f"[{symbol}] HTTP {resp.status_code}，重试 {attempt}/{max_retries}...", file=sys.stderr)
				time.sleep(min(30, attempt * 2))
				continue
			
			data = resp.json()

			# 检查限流提示
			if "Note" in data or "Information" in data:
				wait_more = min(60, 15 * attempt)
				print(f"[{symbol}] 命中限流提示，等待 {wait_more}s 后重试...", file=sys.stderr)
				time.sleep(wait_more)
				continue

			if "Error Message" in data:
				raise ValueError(f"{symbol} API 返回错误: {data['Error Message']}")

			return data

		except requests.RequestException as ex:
			print(f"[{symbol}] 网络异常: {ex}. 重试 {attempt}/{max_retries}...", file=sys.stderr)
			time.sleep(min(30, attempt * 2))

	raise RuntimeError(f"{symbol} {function} 请求失败，超过最大重试次数 {max_retries}。")


def fetch_quote_endpoint(symbol: str, api_key: str, rate_limiter: RateLimiter, session: requests.Session) -> Dict[str, Any]:
	"""获取实时报价信息"""
	return fetch_alpha_vantage_data(
		function="GLOBAL_QUOTE",
		symbol=symbol,
		api_key=api_key,
		rate_limiter=rate_limiter,
		session=session,
	)


def fetch_company_overview(symbol: str, api_key: str, rate_limiter: RateLimiter, session: requests.Session) -> Dict[str, Any]:
	"""获取公司基本信息"""
	return fetch_alpha_vantage_data(
		function="OVERVIEW",
		symbol=symbol,
		api_key=api_key,
		rate_limiter=rate_limiter,
		session=session,
	)


def fetch_news_sentiment(symbol: str, api_key: str, rate_limiter: RateLimiter, session: requests.Session) -> Dict[str, Any]:
	"""获取新闻情感分析（最近一周）"""
	return fetch_alpha_vantage_data(
		function="NEWS_SENTIMENT",
		symbol=symbol,
		api_key=api_key,
		rate_limiter=rate_limiter,
		session=session,
		extra_params={"time_from": "20240801T0000", "limit": "50"}
	)


def fetch_daily_data(symbol: str, api_key: str, rate_limiter: RateLimiter, session: requests.Session) -> Dict[str, Any]:
	"""获取最近一周的日线数据"""
	return fetch_alpha_vantage_data(
		function="TIME_SERIES_DAILY",
		symbol=symbol,
		api_key=api_key,
		rate_limiter=rate_limiter,
		session=session,
		extra_params={"outputsize": "compact"}
	)


def parse_quote_data(quote_data: Dict[str, Any], symbol: str) -> Dict[str, Any]:
	"""解析实时报价数据"""
	global_quote = quote_data.get("Global Quote", {})
	if not global_quote:
		return {}
	
	return {
		"symbol": symbol,
		"current_price": float(global_quote.get("05. price", 0)),
		"change": float(global_quote.get("09. change", 0)),
		"change_percent": global_quote.get("10. change percent", "0%").replace("%", ""),
		"volume": global_quote.get("06. volume", "0"),
		"previous_close": float(global_quote.get("08. previous close", 0)),
		"open": float(global_quote.get("02. open", 0)),
		"high": float(global_quote.get("03. high", 0)),
		"low": float(global_quote.get("04. low", 0)),
		"latest_trading_day": global_quote.get("07. latest trading day", ""),
	}


def parse_company_overview(overview_data: Dict[str, Any], symbol: str) -> Dict[str, Any]:
	"""解析公司基本信息"""
	if not overview_data or "Symbol" not in overview_data:
		return {}
	
	return {
		"symbol": symbol,
		"company_name": overview_data.get("Name", ""),
		"sector": overview_data.get("Sector", ""),
		"industry": overview_data.get("Industry", ""),
		"description": overview_data.get("Description", "")[:200] + "..." if overview_data.get("Description", "") else "",
		"market_cap": overview_data.get("MarketCapitalization", ""),
		"pe_ratio": overview_data.get("PERatio", ""),
		"dividend_yield": overview_data.get("DividendYield", ""),
		"eps": overview_data.get("EPS", ""),
		"beta": overview_data.get("Beta", ""),
	}


def parse_news_sentiment(news_data: Dict[str, Any], symbol: str) -> Dict[str, Any]:
	"""解析新闻情感数据"""
	if not news_data or "feed" not in news_data:
		return {}
	
	feed = news_data.get("feed", [])
	if not feed:
		return {}
	
	# 统计最近一周的新闻
	recent_news = []
	total_sentiment = 0
	positive_count = 0
	negative_count = 0
	
	for item in feed[:10]:  # 取最近10条
		news_date = item.get("time_published", "")
		if news_date:
			try:
				news_datetime = datetime.strptime(news_date, "%Y%m%dT%H%M%S")
				if news_datetime.date() >= (datetime.now() - timedelta(days=7)).date():
					recent_news.append({
						"title": item.get("title", ""),
						"summary": item.get("summary", "")[:100] + "..." if item.get("summary", "") else "",
						"sentiment": item.get("overall_sentiment_label", ""),
						"date": news_datetime.strftime("%Y-%m-%d"),
					})
					
					sentiment_score = item.get("overall_sentiment_score", 0)
					total_sentiment += sentiment_score
					
					if item.get("overall_sentiment_label", "") == "positive":
						positive_count += 1
					elif item.get("overall_sentiment_label", "") == "negative":
						negative_count += 1
			except:
				continue
	
	return {
		"symbol": symbol,
		"recent_news_count": len(recent_news),
		"avg_sentiment_score": round(total_sentiment / len(recent_news), 3) if recent_news else 0,
		"positive_news_count": positive_count,
		"negative_news_count": negative_count,
		"recent_news": recent_news,
	}


def parse_daily_data_for_week(daily_data: Dict[str, Any], symbol: str) -> Dict[str, Any]:
	"""解析最近一周的日线数据"""
	time_series = daily_data.get("Time Series (Daily)", {})
	if not time_series:
		return {}
	
	# 获取最近7个交易日的数据
	dates = sorted(time_series.keys(), reverse=True)[:7]
	if not dates:
		return {}
	
	week_data = []
	for date_str in dates:
		day_data = time_series[date_str]
		week_data.append({
			"date": date_str,
			"close": float(day_data.get("4. close", 0)),
			"volume": int(day_data.get("5. volume", 0)),
			"change": float(day_data.get("4. close", 0)) - float(day_data.get("1. open", 0)),
		})
	
	if len(week_data) >= 2:
		first_close = week_data[-1]["close"]
		last_close = week_data[0]["close"]
		week_change = last_close - first_close
		week_change_percent = (week_change / first_close * 100) if first_close > 0 else 0
	else:
		week_change = 0
		week_change_percent = 0
	
	return {
		"symbol": symbol,
		"week_change": round(week_change, 2),
		"week_change_percent": round(week_change_percent, 2),
		"week_data": week_data,
	}


def fetch_stock_comprehensive_info(
	symbol: str,
	api_key: str,
	rate_limiter: RateLimiter,
	session: requests.Session,
) -> Dict[str, Any]:
	"""获取股票的综合信息"""
	print(f"正在获取 {symbol} 的综合信息...", file=sys.stderr)
	
	result = {"symbol": symbol}
	
	try:
		# 1. 获取实时报价
		quote_data = fetch_quote_endpoint(symbol, api_key, rate_limiter, session)
		result.update(parse_quote_data(quote_data, symbol))
	except Exception as e:
		print(f"[{symbol}] 获取实时报价失败: {e}", file=sys.stderr)
	
	try:
		# 2. 获取公司基本信息
		overview_data = fetch_company_overview(symbol, api_key, rate_limiter, session)
		result.update(parse_company_overview(overview_data, symbol))
	except Exception as e:
		print(f"[{symbol}] 获取公司信息失败: {e}", file=sys.stderr)
	
	try:
		# 3. 获取新闻情感分析
		news_data = fetch_news_sentiment(symbol, api_key, rate_limiter, session)
		result.update(parse_news_sentiment(news_data, symbol))
	except Exception as e:
		print(f"[{symbol}] 获取新闻数据失败: {e}", file=sys.stderr)
	
	try:
		# 4. 获取最近一周日线数据
		daily_data = fetch_daily_data(symbol, api_key, rate_limiter, session)
		result.update(parse_daily_data_for_week(daily_data, symbol))
	except Exception as e:
		print(f"[{symbol}] 获取日线数据失败: {e}", file=sys.stderr)
	
	return result


def ensure_output_dir(path: str) -> None:
	dirname = os.path.dirname(os.path.abspath(path))
	if dirname and not os.path.exists(dirname):
		os.makedirs(dirname, exist_ok=True)


def main(argv: Optional[List[str]] = None) -> None:
	parser = argparse.ArgumentParser(
		description="获取A股股票的综合信息：今日收盘价、基本信息、概念分类、涨跌幅、新闻关联等"
	)
	parser.add_argument(
		"--symbols",
		"-s",
		nargs="+",
		required=True,
		help="A股股票代码，如 600519 000001；脚本会自动标准化为 .SHH/.SHZ"
	)
	parser.add_argument(
		"--api-key",
		"-k",
		help="Alpha Vantage API Key；也可通过环境变量 ALPHAVANTAGE_API_KEY 提供"
	)
	parser.add_argument(
		"--output",
		"-o",
		default=os.path.join("data", f"a_share_info_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"),
		help="输出 JSON 路径（默认 data/a_share_info_时间戳.json）"
	)
	parser.add_argument(
		"--calls-per-minute",
		type=int,
		default=3,
		help="限流：每分钟最大请求数（建议≤3，避免触发限制）"
	)
	parser.add_argument(
		"--no-proxy",
		action="store_true",
		help="忽略系统代理环境变量，直接直连 Alpha Vantage"
	)
	parser.add_argument(
		"--timeout",
		type=int,
		default=20,
		help="单次请求超时秒数，默认 20"
	)
	args = parser.parse_args(argv)

	api_key = "DBW1NM92621232126K" #args.api_key or os.getenv("ALPHAVANTAGE_API_KEY")
	if not api_key:
		print("请提供 Alpha Vantage API Key（--api-key 或环境变量 ALPHAVANTAGE_API_KEY）", file=sys.stderr)
		sys.exit(2)

	rate_limiter = RateLimiter(calls_per_minute=args.calls_per_minute)
	session = requests.Session()
	session.headers.update({
		"User-Agent": "fetch-market/1.0 (+https://www.alphavantage.co/)"
	})
	if args.no_proxy:
		session.trust_env = False

	all_results = []
	for raw_symbol in args.symbols:
		symbol = normalize_a_share_symbol(raw_symbol)
		result = fetch_stock_comprehensive_info(symbol, api_key, rate_limiter, session)
		all_results.append(result)
		print(f"完成 {symbol}", file=sys.stderr)

	ensure_output_dir(args.output)
	
	# 保存为 JSON 格式（包含详细结构）
	with open(args.output, 'w', encoding='utf-8') as f:
		json.dump(all_results, f, ensure_ascii=False, indent=2)
	
	# 同时生成简化的 CSV 格式
	csv_output = args.output.replace('.json', '.csv')
	simplified_data = []
	for result in all_results:
		simplified_data.append({
			"股票代码": result.get("symbol", ""),
			"公司名称": result.get("company_name", ""),
			"行业": result.get("industry", ""),
			"当前价格": result.get("current_price", 0),
			"涨跌幅": result.get("change_percent", "0"),
			"周涨跌幅": result.get("week_change_percent", 0),
			"成交量": result.get("volume", "0"),
			"市值": result.get("market_cap", ""),
			"市盈率": result.get("pe_ratio", ""),
			"最近新闻数": result.get("recent_news_count", 0),
			"情感评分": result.get("avg_sentiment_score", 0),
		})
	
	df = pd.DataFrame(simplified_data)
	df.to_csv(csv_output, index=False, encoding='utf-8-sig')
	
	print(f"已写出详细数据: {args.output}")
	print(f"已写出简化CSV: {csv_output}")
	print(f"共处理 {len(all_results)} 只股票")


if __name__ == "__main__":
	main() 