## 合规抓取股票数据脚本（Alpha Vantage）

本仓库提供一个使用 Alpha Vantage 官方开放接口的 Python 脚本，合规地抓取股票日频复权数据（TIME_SERIES_DAILY_ADJUSTED）。

- 数据来源：Alpha Vantage 官方 API（遵守其使用条款）
- 特点：限流控制、异常/限流重试、按日期过滤、CSV 输出

### 1. 注册并获取 API Key

前往 Alpha Vantage 官网注册并获取 API Key：`https://www.alphavantage.co/`

将 Key 设置为环境变量，或在运行时通过参数传入：

- 环境变量：`ALPHAVANTAGE_API_KEY`  
- 运行参数：`--api-key YOUR_KEY`

### 2. 安装依赖

```bash
python3 -m pip install -r requirements.txt
```

### 3. 使用方法

```bash
python fetch_stocks.py \
  --symbols AAPL MSFT \
  --start 2023-01-01 \
  --end 2023-12-31 \
  --output data/aapl_msft_2023.csv \
  --outputsize full
```

参数说明：
- `--symbols/-s`：股票代码列表，例如 `AAPL MSFT`（美股）。部分市场需带交易所后缀，例如上证：`600519.SHH`，深证：`000001.SZ`
- `--api-key/-k`：Alpha Vantage API Key。也可使用环境变量 `ALPHAVANTAGE_API_KEY`
- `--start`/`--end`：日期范围（YYYY-MM-DD）。不填则不过滤
- `--output/-o`：输出 CSV 文件路径（默认 `data/stocks_时间戳.csv`）
- `--outputsize`：`compact`（约最近100条）或 `full`（全量历史）
- `--calls-per-minute`：每分钟最大请求数（免费额度建议 ≤ 5）

### 4. 合规与限流说明

- 本脚本仅使用官方公开接口，不绕过验证、不进行页面爬取和解析，遵循服务条款
- 免费额度通常限制每分钟约 5 次请求、每天约 500 次请求。脚本内置简单的限流与限流提示重试机制
- 若出现 `Note`/`Information` 等限流提示，脚本会自动等待后重试

### 5. 输出示例

CSV 列包含：`symbol,date,open,high,low,close,adjusted_close,volume,dividend_amount,split_coefficient`

### 6. 常见问题

- 报错“请提供 API Key”：请添加 `--api-key` 或设置环境变量 `ALPHAVANTAGE_API_KEY`
- 数据为空：
  - 检查代码是否包含正确的交易所后缀
  - 检查日期范围是否过于严格
  - 检查是否触发了限流或超过当日配额

### 7. 环境变量示例

可复制 `.env.example` 为 `.env`，并填入真实 Key（如安装了 `python-dotenv` 将自动加载）：

```bash
cp .env.example .env
``` 