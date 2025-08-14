#!/usr/bin/env python3
import argparse
import csv
import os
import sys
import time
import threading
from datetime import datetime, date
from typing import List, Optional

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

	def __init__(self, calls_per_minute: int = 5) -> None:
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
	"""将 A 股代码映射为 Alpha Vantage 支持的后缀：
	- 纯数字：6 开头 -> .SHH；0/3 开头 -> .SHZ
	- 兼容 .SH/.SZ，自动改为 .SHH/.SHZ
	"""
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


def fetch_alpha_vantage_daily(
	symbol: str,
	api_key: str,
	outputsize: str,
	rate_limiter: RateLimiter,
	session: requests.Session,
	function_name: str = "TIME_SERIES_DAILY",
	max_retries: int = 5,
	timeout_s: int = 30,
) -> dict:
	params = {
		"function": function_name,
		"symbol": symbol,
		"outputsize": outputsize,
		"datatype": "json",
		"apikey": api_key,
	}

	for attempt in range(1, max_retries + 1):
		rate_limiter.wait()
		try:
			resp = session.get(ALPHAVANTAGE_API_URL, params=params, timeout=timeout_s)
			if resp.status_code != 200:
				time.sleep(min(60, attempt * 2))
				continue
			data = resp.json()

			if "Note" in data or "Information" in data:
				wait_more = min(75, 10 * attempt)
				print(f"[{symbol}] 命中限流提示，等待 {wait_more}s 后重试...", file=sys.stderr)
				time.sleep(wait_more)
				continue

			if "Error Message" in data:
				raise ValueError(f"{symbol} API 返回错误: {data['Error Message']}")

			if "Time Series (Daily)" in data:
				return data

			time.sleep(min(60, attempt * 2))
		except requests.RequestException as ex:
			print(f"[{symbol}] 网络异常: {ex}. 正在重试({attempt}/{max_retries})...", file=sys.stderr)
			time.sleep(min(60, attempt * 2))

	raise RuntimeError(f"{symbol} 请求失败，超过最大重试次数 {max_retries}。")


def parse_daily_to_dataframe(symbol: str, payload: dict) -> pd.DataFrame:
	ts = payload.get("Time Series (Daily)", {})
	rows = []
	for d_str, values in ts.items():
		rows.append(
			{
				"symbol": symbol,
				"date": datetime.strptime(d_str, "%Y-%m-%d").date(),
				"open": float(values.get("1. open", "nan")),
				"high": float(values.get("2. high", "nan")),
				"low": float(values.get("3. low", "nan")),
				"close": float(values.get("4. close", "nan")),
				"adjusted_close": float(values.get("5. adjusted close", values.get("4. close", "nan"))),
				"volume": int(float(values.get("6. volume", values.get("5. volume", 0)))),
				"dividend_amount": float(values.get("7. dividend amount", 0.0)),
				"split_coefficient": float(values.get("8. split coefficient", 1.0)),
			}
		)
	return pd.DataFrame(rows)


def parse_date(d: Optional[str]) -> Optional[date]:
	if not d:
		return None
	return date_parser.parse(d).date()


def ensure_output_dir(path: str) -> None:
	dirname = os.path.dirname(os.path.abspath(path))
	if dirname and not os.path.exists(dirname):
		os.makedirs(dirname, exist_ok=True)


def main(argv: Optional[List[str]] = None) -> None:
	parser = argparse.ArgumentParser(
		description="合规抓取A股指定代码的日线K线数据（可选复权），使用 Alpha Vantage 官方 API"
	)
	parser.add_argument(
		"--symbols",
		"-s",
		nargs="+",
		required=True,
		help="A股股票代码，如 600519 000001 或 600519.SHH 000001.SHZ；脚本会自动标准化为 .SHH/.SHZ"
	)
	parser.add_argument(
		"--api-key",
		"-k",
		help="Alpha Vantage API Key；也可通过环境变量 ALPHAVANTAGE_API_KEY 提供"
	)
	parser.add_argument(
		"--start",
		help="开始日期 YYYY-MM-DD"
	)
	parser.add_argument(
		"--end",
		help="结束日期 YYYY-MM-DD"
	)
	parser.add_argument(
		"--output",
		"-o",
		default=os.path.join("data", f"a_shares_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"),
		help="输出 CSV 路径（默认 data/a_shares_时间戳.csv）"
	)
	parser.add_argument(
		"--outputsize",
		choices=["compact", "full"],
		default="full",
		help="compact 约最近100条，full 为全量"
	)
	parser.add_argument(
		"--calls-per-minute",
		type=int,
		default=5,
		help="限流：每分钟最大请求数（免费额度建议≤5）"
	)
	parser.add_argument(
		"--adjusted",
		action="store_true",
		help="使用复权日线 TIME_SERIES_DAILY_ADJUSTED；默认使用非复权日线 TIME_SERIES_DAILY"
	)
	parser.add_argument(
		"--no-proxy",
		action="store_true",
		help="忽略系统代理环境变量（如 HTTP(S)_PROXY），直接直连 Alpha Vantage"
	)
	parser.add_argument(
		"--timeout",
		type=int,
		default=30,
		help="单次请求超时秒数，默认 30"
	)
	args = parser.parse_args(argv)

	api_key = "DBW1NM92621232126K" #args.api_key or os.getenv("ALPHAVANTAGE_API_KEY")
	if not api_key:
		print("请提供 Alpha Vantage API Key（--api-key 或环境变量 ALPHAVANTAGE_API_KEY）", file=sys.stderr)
		sys.exit(2)

	start_date = parse_date(args.start)
	end_date = parse_date(args.end)
	if start_date and end_date and start_date > end_date:
		print("开始日期不能晚于结束日期", file=sys.stderr)
		sys.exit(2)

	rate_limiter = RateLimiter(calls_per_minute=args.calls_per_minute)
	session = requests.Session()
	session.headers.update({
		"User-Agent": "fetch-market/1.0 (+https://www.alphavantage.co/)"
	})
	# 忽略系统代理（如需直连）
	if args.no_proxy:
		session.trust_env = False

	function_name = "TIME_SERIES_DAILY_ADJUSTED" if args.adjusted else "TIME_SERIES_DAILY"

	frames: List[pd.DataFrame] = []
	for raw_symbol in args.symbols:
		symbol = normalize_a_share_symbol(raw_symbol)
		print(f"拉取 {symbol}...", file=sys.stderr)
		payload = fetch_alpha_vantage_daily(
			symbol=symbol,
			api_key=api_key,
			outputsize=args.outputsize,
			rate_limiter=rate_limiter,
			session=session,
			function_name=function_name,
			timeout_s=args.timeout,
		)
		df = parse_daily_to_dataframe(symbol, payload)
		if df.empty:
			print(f"{symbol} 未返回有效数据", file=sys.stderr)
			continue
		if start_date:
			df = df[df["date"] >= start_date]
		if end_date:
			df = df[df["date"] <= end_date]
		frames.append(df)

	if not frames:
		print("没有可用数据可写出。", file=sys.stderr)
		sys.exit(1)

	result = pd.concat(frames, ignore_index=True)
	result.sort_values(["symbol", "date"], inplace=True)

	ensure_output_dir(args.output)
	result.to_csv(args.output, index=False, quoting=csv.QUOTE_MINIMAL)
	print(f"已写出: {args.output}  （共 {len(result)} 行）")


if __name__ == "__main__":
	main() 