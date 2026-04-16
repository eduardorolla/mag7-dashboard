"""
data_fetcher.py — Módulo de coleta de dados das Magnificent 7.

Estratégia de dados (3 camadas):
  1. FMP (Financial Modeling Prep) — API gratuita com key, confiável em datacenters
  2. Yahoo Finance API direta — funciona às vezes, depende do IP
  3. Dados demo — fallback offline com valores estáticos realistas

Responsável por buscar todos os indicadores do plano de monitoramento:
  1. Valuation: PEG, ROIC, FCF Yield, Forward P/E
  2. Crescimento/Risco: CAPEX/Receita, EBITDA Margin, P/S
  3. Sentimento/Real-Time: RSI (14d), Beta, Put/Call Ratio
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import logging
import traceback
import random
import requests
import time as _time
import os

# yfinance opcional — FMP é a fonte primária agora
try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False

logger = logging.getLogger(__name__)

# As Magnificent 7 com seus tickers
MAG7_TICKERS = {
    "AAPL": "Apple",
    "MSFT": "Microsoft",
    "GOOGL": "Alphabet",
    "AMZN": "Amazon",
    "NVDA": "NVIDIA",
    "META": "Meta Platforms",
    "TSLA": "Tesla",
}

# Limiares de alerta conforme o plano
ALERT_THRESHOLDS = {
    "peg_ratio": {"warning": 2.0, "danger": 2.2},
    "roic": {"warning": 22.0, "danger": 20.0},
    "fcf_yield": {"warning": 3.5, "danger": 3.0},
    "forward_pe_deviation": {"warning": 15.0, "danger": 20.0},
    "rsi": {"overbought": 70, "oversold": 30},
    "beta": {"warning": 1.3, "danger": 1.5},
    "cloud_growth": {"warning": 18.0, "danger": 15.0},
}


# ==================== FMP (Financial Modeling Prep) API ====================
# Fonte primária: funciona em datacenters, 250 calls/dia grátis.
# Registre-se em https://site.financialmodelingprep.com/developer e defina FMP_API_KEY.

FMP_API_KEY = os.environ.get("FMP_API_KEY", "")
FMP_BASE = "https://financialmodelingprep.com/api/v3"

_fmp_session = requests.Session()
_fmp_session.headers.update({
    "User-Agent": "Mag7Dashboard/1.0",
    "Accept": "application/json",
})


def _fmp_available() -> bool:
    return bool(FMP_API_KEY)


def _fmp_get(endpoint: str, params: dict = None) -> Optional[Any]:
    if not FMP_API_KEY:
        return None
    url = f"{FMP_BASE}/{endpoint}"
    p = {"apikey": FMP_API_KEY}
    if params:
        p.update(params)
    try:
        resp = _fmp_session.get(url, params=p, timeout=15)
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 403:
            logger.warning("FMP 403: API key inválida ou limite excedido")
        else:
            logger.warning(f"FMP {resp.status_code} para {endpoint}")
    except Exception as e:
        logger.warning(f"FMP erro para {endpoint}: {e}")
    return None


def _fmp_bulk_quote(tickers: List[str]) -> Optional[List[Dict]]:
    symbols = ",".join(tickers)
    return _fmp_get(f"quote/{symbols}")


def _fmp_key_metrics(ticker: str) -> Optional[List[Dict]]:
    return _fmp_get(f"key-metrics-ttm/{ticker}")


def _fmp_ratios(ticker: str) -> Optional[List[Dict]]:
    return _fmp_get(f"ratios-ttm/{ticker}")


def _fmp_cashflow(ticker: str) -> Optional[List[Dict]]:
    return _fmp_get(f"cash-flow-statement/{ticker}", {"period": "annual", "limit": 1})


def _fmp_historical(ticker: str, from_date: str, to_date: str) -> Optional[Dict]:
    return _fmp_get(f"historical-price-full/{ticker}", {"from": from_date, "to": to_date})


def _safe_pct(val, threshold=10):
    """Converte valor que pode ser decimal (0.33) ou porcentagem (33.0) para porcentagem."""
    if val is None:
        return None
    if abs(val) < threshold:
        return round(val * 100, 2)
    return round(val, 2)


def _parse_fmp_to_stock(quote: Dict, metrics: Optional[Dict], ratios: Optional[Dict],
                         cashflow: Optional[Dict], sp500_prices: pd.Series) -> Dict[str, Any]:
    """Converte dados FMP para o formato do dashboard."""
    ticker_symbol = quote.get("symbol", "")

    result = {
        "ticker": ticker_symbol,
        "name": MAG7_TICKERS.get(ticker_symbol, quote.get("name", ticker_symbol)),
        "current_price": quote.get("price"),
        "market_cap": quote.get("marketCap"),
        "currency": "USD",
        "sector": quote.get("sector") or "Technology",
        "updated_at": datetime.now().isoformat(),
    }

    result["trailing_pe"] = quote.get("pe")
    result["forward_pe"] = None
    result["price_to_sales"] = None
    result["peg_ratio"] = None
    result["roic"] = None
    result["fcf_yield"] = None
    result["dividend_yield"] = None
    result["ebitda_margin"] = None
    result["revenue_growth"] = None
    result["capex_to_revenue"] = None

    # Métricas TTM (key-metrics-ttm)
    if metrics:
        result["peg_ratio"] = metrics.get("pegRatioTTM")
        roe = metrics.get("roeTTM")
        if roe is not None:
            result["roic"] = _safe_pct(roe)
        fcfy = metrics.get("freeCashFlowYieldTTM")
        if fcfy is not None:
            result["fcf_yield"] = _safe_pct(fcfy)
        dy = metrics.get("dividendYieldTTM")
        if dy is not None:
            result["dividend_yield"] = _safe_pct(dy, threshold=1)
        ps = metrics.get("priceToSalesRatioTTM")
        if ps is not None:
            result["price_to_sales"] = round(ps, 2)
        fpe = metrics.get("peRatioTTM")  # FMP key-metrics has peRatioTTM
        if fpe is not None:
            result["forward_pe"] = round(fpe, 2)

    # Ratios TTM — complementa o que faltou
    if ratios:
        em = ratios.get("ebitdaMarginTTM")
        if em is not None:
            result["ebitda_margin"] = _safe_pct(em)
        if result["price_to_sales"] is None:
            ps2 = ratios.get("priceToSalesRatioTTM")
            if ps2 is not None:
                result["price_to_sales"] = round(ps2, 2)
        if result["peg_ratio"] is None:
            peg2 = ratios.get("pegRatioTTM")
            if peg2 is not None:
                result["peg_ratio"] = round(peg2, 2)
        if result["forward_pe"] is None:
            pe2 = ratios.get("peRatioTTM")
            if pe2 is not None:
                result["forward_pe"] = round(pe2, 2)

    # CAPEX / Revenue do cashflow statement
    if cashflow:
        capex = cashflow.get("capitalExpenditure")
        rev = cashflow.get("revenue")
        if capex and rev and rev > 0:
            result["capex_to_revenue"] = round(abs(capex) / rev * 100, 2)
        rg = cashflow.get("revenueGrowth")
        if rg is not None:
            result["revenue_growth"] = _safe_pct(rg)

    # 52-week range e beta do quote
    result["fifty_two_week_high"] = quote.get("yearHigh")
    result["fifty_two_week_low"] = quote.get("yearLow")
    result["avg_volume"] = quote.get("avgVolume")
    result["put_call_ratio"] = None

    # Beta from FMP quote
    beta_val = quote.get("beta")
    result["beta_90d"] = round(beta_val, 2) if beta_val is not None else None

    # RSI será calculado depois (do histórico)
    result["rsi_14"] = None

    # Alertas
    result["peg_alert"] = get_alert_level("peg_ratio", result.get("peg_ratio"))
    result["fcf_yield_alert"] = get_alert_level("fcf_yield", result.get("fcf_yield"))
    result["roic_alert"] = get_alert_level("roic", result.get("roic"))
    result["rsi_alert"] = get_alert_level("rsi", result.get("rsi_14"))
    result["beta_alert"] = get_alert_level("beta", result.get("beta_90d"))
    result["bubble_risk"] = calculate_bubble_risk(result)

    return result


# ==================== YAHOO FINANCE DIRECT API ====================

_YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Upgrade-Insecure-Requests": "1",
}
_yahoo_session = requests.Session()
_yahoo_session.headers.update(_YAHOO_HEADERS)
_yahoo_crumb = None
_yahoo_crumb_ts = 0


def _get_yahoo_crumb() -> Optional[str]:
    global _yahoo_crumb, _yahoo_crumb_ts
    if _yahoo_crumb and (_time.time() - _yahoo_crumb_ts < 1800):
        return _yahoo_crumb
    hosts = ["query1.finance.yahoo.com", "query2.finance.yahoo.com"]
    for host in hosts:
        try:
            _yahoo_session.get("https://fc.yahoo.com", timeout=10, allow_redirects=True)
            resp = _yahoo_session.get(f"https://{host}/v1/test/getcrumb", timeout=10)
            if resp.status_code == 200 and resp.text and "Too Many" not in resp.text:
                _yahoo_crumb = resp.text.strip()
                _yahoo_crumb_ts = _time.time()
                logger.info(f"Yahoo crumb obtido via {host}")
                return _yahoo_crumb
        except Exception as e:
            logger.warning(f"Falha crumb via {host}: {e}")
    return None


def _yahoo_api_quote(ticker_symbol: str) -> Optional[Dict]:
    modules = "price,summaryDetail,defaultKeyStatistics,financialData"
    url = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker_symbol}"
    crumb = _get_yahoo_crumb()
    params = {"modules": modules}
    if crumb:
        params["crumb"] = crumb
    try:
        resp = _yahoo_session.get(url, params=params, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            result = data.get("quoteSummary", {}).get("result", [])
            if result:
                return result[0]
    except Exception as e:
        logger.warning(f"Yahoo API v10 falhou para {ticker_symbol}: {e}")
    return None


def _yahoo_api_history(ticker_symbol: str, period: str = "6mo") -> Optional[pd.DataFrame]:
    period_map = {"1mo": "1mo", "3mo": "3mo", "6mo": "6mo", "1y": "1y", "2y": "2y", "5y": "5y"}
    yperiod = period_map.get(period, "6mo")
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker_symbol}"
    params = {"range": yperiod, "interval": "1d", "includePrePost": "false"}
    crumb = _get_yahoo_crumb()
    if crumb:
        params["crumb"] = crumb
    try:
        resp = _yahoo_session.get(url, params=params, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            chart = data.get("chart", {}).get("result", [])
            if chart:
                timestamps = chart[0].get("timestamp", [])
                indicators = chart[0].get("indicators", {}).get("quote", [{}])[0]
                closes = indicators.get("close", [])
                volumes = indicators.get("volume", [])
                if timestamps and closes:
                    df = pd.DataFrame({
                        "Close": closes,
                        "Volume": volumes,
                    }, index=pd.to_datetime(timestamps, unit="s"))
                    df = df.dropna(subset=["Close"])
                    return df
    except Exception as e:
        logger.warning(f"Yahoo chart API falhou para {ticker_symbol}: {e}")
    return None


def _parse_yahoo_api_to_stock(ticker_symbol: str, api_data: Dict, sp500_prices: pd.Series) -> Dict[str, Any]:
    result = {
        "ticker": ticker_symbol,
        "name": MAG7_TICKERS.get(ticker_symbol, ticker_symbol),
        "currency": "USD",
        "updated_at": datetime.now().isoformat(),
    }

    price = api_data.get("price", {})
    detail = api_data.get("summaryDetail", {})
    stats = api_data.get("defaultKeyStatistics", {})
    fin = api_data.get("financialData", {})

    def raw(d, key):
        v = d.get(key, {})
        if isinstance(v, dict):
            return v.get("raw")
        return v if v is not None else None

    result["current_price"] = raw(price, "regularMarketPrice")
    result["market_cap"] = raw(price, "marketCap")
    result["sector"] = price.get("sector")
    result["forward_pe"] = raw(stats, "forwardPE") or raw(detail, "forwardPE")
    result["trailing_pe"] = raw(detail, "trailingPE")
    result["price_to_sales"] = raw(detail, "priceToSalesTrailing12Months")

    result["peg_ratio"] = raw(stats, "pegRatio") or raw(fin, "pegRatio") or raw(detail, "pegRatio")
    if result["peg_ratio"] is None:
        fpe = result["forward_pe"]
        eg = raw(stats, "earningsQuarterlyGrowth") or raw(fin, "earningsGrowth")
        if fpe and eg and eg > 0:
            result["peg_ratio"] = round(fpe / (eg * 100), 2)

    result["fifty_two_week_high"] = raw(detail, "fiftyTwoWeekHigh")
    result["fifty_two_week_low"] = raw(detail, "fiftyTwoWeekLow")

    fcf = raw(fin, "freeCashflow")
    mcap = result["market_cap"]
    result["fcf_yield"] = round((fcf / mcap) * 100, 2) if fcf and mcap and mcap > 0 else None

    roe = raw(fin, "returnOnEquity")
    result["roic"] = round(roe * 100, 2) if roe else None

    em = raw(fin, "ebitdaMargins")
    result["ebitda_margin"] = round(em * 100, 2) if em else None

    rg = raw(fin, "revenueGrowth")
    result["revenue_growth"] = round(rg * 100, 2) if rg else None

    result["capex_to_revenue"] = None
    try:
        ocf = raw(fin, "operatingCashflow")
        fcf_val = raw(fin, "freeCashflow")
        rev = raw(fin, "totalRevenue")
        if ocf and fcf_val and rev and rev > 0:
            capex = abs(ocf - fcf_val)
            if capex > 0:
                result["capex_to_revenue"] = round((capex / rev) * 100, 2)
    except Exception:
        pass

    result["dividend_yield"] = raw(detail, "dividendYield")
    if result["dividend_yield"]:
        result["dividend_yield"] = round(result["dividend_yield"] * 100, 2)

    yahoo_beta = raw(detail, "beta") or raw(stats, "beta3Year")

    # RSI e Beta via histórico
    hist_df = _yahoo_api_history(ticker_symbol, "3mo")
    if hist_df is not None and not hist_df.empty:
        result["rsi_14"] = calculate_rsi(hist_df["Close"])
        result["beta_90d"] = calculate_beta(hist_df["Close"], sp500_prices)
    else:
        result["rsi_14"] = None
        result["beta_90d"] = None

    if result.get("beta_90d") is None and yahoo_beta is not None:
        result["beta_90d"] = round(float(yahoo_beta), 2)

    result["put_call_ratio"] = None
    result["avg_volume"] = None

    result["peg_alert"] = get_alert_level("peg_ratio", result.get("peg_ratio"))
    result["fcf_yield_alert"] = get_alert_level("fcf_yield", result.get("fcf_yield"))
    result["roic_alert"] = get_alert_level("roic", result.get("roic"))
    result["rsi_alert"] = get_alert_level("rsi", result.get("rsi_14"))
    result["beta_alert"] = get_alert_level("beta", result.get("beta_90d"))
    result["bubble_risk"] = calculate_bubble_risk(result)

    return result


# ==================== CÁLCULOS ====================

def calculate_rsi(prices: pd.Series, period: int = 14) -> Optional[float]:
    if prices is None or len(prices) < period + 1:
        return None
    delta = prices.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return round(float(rsi.iloc[-1]), 2)


def calculate_beta(stock_prices: pd.Series, market_prices: pd.Series, window: int = 90) -> Optional[float]:
    if stock_prices is None or market_prices is None:
        return None
    if len(stock_prices) < window or len(market_prices) < window:
        return None
    stock_returns = stock_prices.pct_change().dropna().tail(window)
    market_returns = market_prices.pct_change().dropna().tail(window)
    aligned = pd.DataFrame({"stock": stock_returns, "market": market_returns}).dropna()
    if len(aligned) < 20:
        return None
    covariance = aligned["stock"].cov(aligned["market"])
    market_variance = aligned["market"].var()
    if market_variance == 0:
        return None
    return round(covariance / market_variance, 2)


def get_alert_level(metric: str, value: Optional[float]) -> str:
    if value is None:
        return "gray"
    if metric == "peg_ratio":
        if value > ALERT_THRESHOLDS["peg_ratio"]["danger"]:
            return "red"
        elif value > ALERT_THRESHOLDS["peg_ratio"]["warning"]:
            return "yellow"
        return "green"
    elif metric == "roic":
        if value < ALERT_THRESHOLDS["roic"]["danger"]:
            return "red"
        elif value < ALERT_THRESHOLDS["roic"]["warning"]:
            return "yellow"
        return "green"
    elif metric == "fcf_yield":
        if value < ALERT_THRESHOLDS["fcf_yield"]["danger"]:
            return "red"
        elif value < ALERT_THRESHOLDS["fcf_yield"]["warning"]:
            return "yellow"
        return "green"
    elif metric == "rsi":
        if value > ALERT_THRESHOLDS["rsi"]["overbought"]:
            return "red"
        elif value < ALERT_THRESHOLDS["rsi"]["oversold"]:
            return "red"
        return "green"
    elif metric == "beta":
        if value > ALERT_THRESHOLDS["beta"]["danger"]:
            return "red"
        elif value > ALERT_THRESHOLDS["beta"]["warning"]:
            return "yellow"
        return "green"
    return "gray"


def calculate_bubble_risk(stock: Dict[str, Any]) -> Dict[str, Any]:
    """Calcula o Índice de Risco de Bolha (0-100)."""
    scores = {}
    weights = {}

    ps = stock.get("price_to_sales")
    if ps is not None:
        if ps <= 4:
            scores["ps"] = 0
        elif ps <= 12:
            scores["ps"] = (ps - 4) / 8 * 40
        else:
            scores["ps"] = 40 + (ps - 12) / 13 * 60
        scores["ps"] = min(scores["ps"], 100)
        weights["ps"] = 0.25
    else:
        weights["ps"] = 0

    peg = stock.get("peg_ratio")
    if peg is not None and peg > 0:
        if peg <= 1.0:
            scores["peg"] = 0
        elif peg <= 2.0:
            scores["peg"] = (peg - 1.0) / 1.0 * 30
        else:
            scores["peg"] = 30 + (peg - 2.0) / 2.0 * 70
        scores["peg"] = min(scores["peg"], 100)
        weights["peg"] = 0.20
    else:
        weights["peg"] = 0

    fcf = stock.get("fcf_yield")
    rev_growth = stock.get("revenue_growth")
    if fcf is not None:
        base_score = min(max((6.0 - fcf) / 6.0 * 100, 0), 100)
        if rev_growth is not None and rev_growth > 10:
            growth_discount = min((rev_growth - 10) / 50 * 0.4, 0.4)
            base_score = base_score * (1 - growth_discount)
        scores["fcf"] = round(base_score, 1)
        weights["fcf"] = 0.15
    else:
        weights["fcf"] = 0

    fpe = stock.get("forward_pe")
    if fpe is not None:
        if fpe <= 20:
            scores["fpe"] = 0
        elif fpe <= 40:
            scores["fpe"] = (fpe - 20) / 20 * 40
        else:
            scores["fpe"] = 40 + (fpe - 40) / 40 * 60
        scores["fpe"] = min(scores["fpe"], 100)
        weights["fpe"] = 0.15
    else:
        weights["fpe"] = 0

    rsi = stock.get("rsi_14")
    if rsi is not None:
        if rsi <= 55:
            scores["rsi"] = 0
        elif rsi <= 70:
            scores["rsi"] = (rsi - 55) / 15 * 50
        else:
            scores["rsi"] = 50 + (rsi - 70) / 15 * 50
        scores["rsi"] = min(scores["rsi"], 100)
        weights["rsi"] = 0.10
    else:
        weights["rsi"] = 0

    pc = stock.get("put_call_ratio")
    if pc is not None:
        scores["pc"] = min(max((1.0 - pc) / 0.6 * 100, 0), 100)
        weights["pc"] = 0.10
    else:
        weights["pc"] = 0

    beta = stock.get("beta_90d")
    if beta is not None:
        scores["beta"] = min(max((beta - 1.0) / 1.2 * 100, 0), 100)
        weights["beta"] = 0.05
    else:
        weights["beta"] = 0

    total_weight = sum(weights.values())
    if total_weight == 0:
        return {"score": None, "level": "gray", "components": {}}

    weighted_score = sum(scores.get(k, 0) * (w / total_weight) for k, w in weights.items())
    final_score = round(weighted_score, 1)

    if final_score >= 70:
        level = "extreme"
    elif final_score >= 50:
        level = "high"
    elif final_score >= 30:
        level = "moderate"
    else:
        level = "low"

    component_names = {
        "ps": "P/S Ratio", "peg": "PEG Ratio", "fcf": "FCF Yield",
        "fpe": "Forward P/E", "rsi": "RSI", "pc": "Put/Call", "beta": "Beta"
    }
    top_contributor = max(scores, key=lambda k: scores[k] * weights.get(k, 0)) if scores else None

    return {
        "score": final_score,
        "level": level,
        "top_risk": component_names.get(top_contributor, "N/A"),
        "top_risk_score": round(scores.get(top_contributor, 0), 1) if top_contributor else None,
        "components": {component_names.get(k, k): round(v, 1) for k, v in scores.items()},
    }


def safe_get(info: dict, key: str, default=None):
    val = info.get(key, default)
    if val is None or (isinstance(val, float) and (np.isnan(val) or np.isinf(val))):
        return default
    return val


# ==================== FETCH FUNCTIONS ====================

def fetch_single_stock(ticker_symbol: str, sp500_prices: pd.Series) -> Dict[str, Any]:
    """Busca dados de uma ação via yfinance (fallback se FMP não disponível)."""
    ticker = yf.Ticker(ticker_symbol)
    info = ticker.info

    result = {
        "ticker": ticker_symbol,
        "name": MAG7_TICKERS.get(ticker_symbol, info.get("shortName", ticker_symbol)),
        "current_price": safe_get(info, "currentPrice") or safe_get(info, "regularMarketPrice"),
        "market_cap": safe_get(info, "marketCap"),
        "currency": safe_get(info, "currency", "USD"),
        "sector": safe_get(info, "sector"),
        "updated_at": datetime.now().isoformat(),
    }

    result["peg_ratio"] = safe_get(info, "pegRatio")
    result["peg_alert"] = get_alert_level("peg_ratio", result["peg_ratio"])
    result["forward_pe"] = safe_get(info, "forwardPE")
    result["trailing_pe"] = safe_get(info, "trailingPE")
    result["price_to_sales"] = safe_get(info, "priceToSalesTrailing12Months")

    fcf = safe_get(info, "freeCashflow")
    mcap = safe_get(info, "marketCap")
    result["fcf_yield"] = round((fcf / mcap) * 100, 2) if fcf and mcap and mcap > 0 else None
    result["fcf_yield_alert"] = get_alert_level("fcf_yield", result["fcf_yield"])

    result["roic"] = safe_get(info, "returnOnEquity")
    if result["roic"] is not None:
        result["roic"] = round(result["roic"] * 100, 2)
    result["roic_alert"] = get_alert_level("roic", result["roic"])

    ebitda_margin = safe_get(info, "ebitdaMargins")
    result["ebitda_margin"] = round(ebitda_margin * 100, 2) if ebitda_margin is not None else None

    rev_growth = safe_get(info, "revenueGrowth")
    result["revenue_growth"] = round(rev_growth * 100, 2) if rev_growth is not None else None

    try:
        cashflow = ticker.cashflow
        income = ticker.income_stmt
        if cashflow is not None and not cashflow.empty and income is not None and not income.empty:
            capex = None
            revenue = None
            for capex_name in ["Capital Expenditure", "CapitalExpenditure"]:
                if capex_name in cashflow.index:
                    capex = abs(float(cashflow.loc[capex_name].iloc[0]))
                    break
            for rev_name in ["Total Revenue", "TotalRevenue"]:
                if rev_name in income.index:
                    revenue = float(income.loc[rev_name].iloc[0])
                    break
            result["capex_to_revenue"] = round((capex / revenue) * 100, 2) if capex and revenue and revenue > 0 else None
        else:
            result["capex_to_revenue"] = None
    except Exception:
        result["capex_to_revenue"] = None

    try:
        hist = ticker.history(period="3mo")
        if hist is not None and not hist.empty:
            result["rsi_14"] = calculate_rsi(hist["Close"])
            result["beta_90d"] = calculate_beta(hist["Close"], sp500_prices)
        else:
            result["rsi_14"] = None
            result["beta_90d"] = None
    except Exception:
        result["rsi_14"] = None
        result["beta_90d"] = None

    result["rsi_alert"] = get_alert_level("rsi", result["rsi_14"])
    if result["beta_90d"] is None:
        result["beta_90d"] = safe_get(info, "beta")
    result["beta_alert"] = get_alert_level("beta", result["beta_90d"])

    result["put_call_ratio"] = None
    result["fifty_two_week_high"] = safe_get(info, "fiftyTwoWeekHigh")
    result["fifty_two_week_low"] = safe_get(info, "fiftyTwoWeekLow")
    result["avg_volume"] = safe_get(info, "averageVolume")
    result["dividend_yield"] = safe_get(info, "dividendYield")
    if result["dividend_yield"] is not None:
        result["dividend_yield"] = round(result["dividend_yield"] * 100, 2)

    result["bubble_risk"] = calculate_bubble_risk(result)
    return result


def fetch_sp500_prices() -> pd.Series:
    """Busca preços históricos do S&P 500 para cálculo de Beta."""
    # Tentativa 1: FMP
    if _fmp_available():
        try:
            to_date = datetime.now().strftime("%Y-%m-%d")
            from_date = (datetime.now() - timedelta(days=200)).strftime("%Y-%m-%d")
            data = _fmp_historical("^GSPC", from_date, to_date)
            if data and "historical" in data:
                hist = data["historical"]
                if hist:
                    df = pd.DataFrame(hist)
                    df["date"] = pd.to_datetime(df["date"])
                    df = df.sort_values("date").set_index("date")
                    logger.info(f"S&P 500 obtido via FMP ({len(df)} pontos)")
                    return df["close"].rename("Close")
        except Exception as e:
            logger.warning(f"FMP S&P 500 falhou: {e}")

    # Tentativa 2: Yahoo API direta
    df = _yahoo_api_history("^GSPC", "6mo")
    if df is not None and not df.empty:
        logger.info(f"S&P 500 obtido via Yahoo API ({len(df)} pontos)")
        return df["Close"]

    # Tentativa 3: yfinance
    if YFINANCE_AVAILABLE:
        try:
            sp500 = yf.Ticker("^GSPC")
            hist = sp500.history(period="6mo")
            if hist is not None and not hist.empty:
                return hist["Close"]
        except Exception as e:
            logger.warning(f"yfinance S&P 500 falhou: {e}")

    logger.warning("Não conseguiu buscar S&P 500 por nenhum método")
    return pd.Series()


def fetch_price_history(ticker_symbol: str, period: str = "1y") -> list:
    """Busca histórico de preços para gráficos."""
    period_days = {"1mo": 30, "3mo": 90, "6mo": 180, "1y": 365, "2y": 730, "5y": 1825}
    days = period_days.get(period, 365)

    # Tentativa 1: FMP
    if _fmp_available():
        try:
            to_date = datetime.now().strftime("%Y-%m-%d")
            from_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            data = _fmp_historical(ticker_symbol, from_date, to_date)
            if data and "historical" in data and data["historical"]:
                hist = data["historical"]
                hist.sort(key=lambda x: x["date"])
                records = [{"date": d["date"], "close": round(float(d["close"]), 2), "volume": int(d.get("volume", 0))} for d in hist]
                if records:
                    logger.info(f"Histórico {ticker_symbol} ({period}) via FMP: {len(records)} pontos")
                    return records
        except Exception as e:
            logger.warning(f"FMP histórico falhou para {ticker_symbol}: {e}")

    # Tentativa 2: Yahoo API direta
    df = _yahoo_api_history(ticker_symbol, period)
    if df is not None and not df.empty:
        return [{"date": date.strftime("%Y-%m-%d"), "close": round(float(row["Close"]), 2),
                 "volume": int(row["Volume"]) if pd.notna(row.get("Volume")) else 0}
                for date, row in df.iterrows()]

    # Tentativa 3: yfinance
    if YFINANCE_AVAILABLE:
        try:
            ticker = yf.Ticker(ticker_symbol)
            hist = ticker.history(period=period)
            if hist is not None and not hist.empty:
                return [{"date": date.strftime("%Y-%m-%d"), "close": round(float(row["Close"]), 2),
                         "volume": int(row["Volume"])} for date, row in hist.iterrows()]
        except Exception as e:
            logger.warning(f"yfinance histórico falhou para {ticker_symbol}: {e}")

    return _generate_demo_history(ticker_symbol, period)


def _check_connectivity() -> bool:
    return YFINANCE_AVAILABLE


# ==================== DADOS DEMO ====================
DEMO_DATA = {
    "AAPL": {"name": "Apple", "current_price": 223.45, "market_cap": 3.41e12, "peg_ratio": 2.35, "forward_pe": 28.1, "trailing_pe": 33.5, "price_to_sales": 8.7, "fcf_yield": 3.42, "roic": 58.2, "ebitda_margin": 33.8, "capex_to_revenue": 2.8, "revenue_growth": 5.1, "rsi_14": 62.3, "beta_90d": 1.21, "put_call_ratio": 0.78, "fifty_two_week_high": 242.10, "fifty_two_week_low": 171.30, "dividend_yield": 0.44, "sector": "Technology"},
    "MSFT": {"name": "Microsoft", "current_price": 445.80, "market_cap": 3.31e12, "peg_ratio": 2.18, "forward_pe": 32.4, "trailing_pe": 36.2, "price_to_sales": 13.9, "fcf_yield": 2.85, "roic": 35.6, "ebitda_margin": 52.1, "capex_to_revenue": 15.2, "revenue_growth": 13.8, "rsi_14": 58.7, "beta_90d": 1.05, "put_call_ratio": 0.65, "fifty_two_week_high": 468.35, "fifty_two_week_low": 362.90, "dividend_yield": 0.72, "sector": "Technology"},
    "GOOGL": {"name": "Alphabet", "current_price": 174.20, "market_cap": 2.14e12, "peg_ratio": 1.42, "forward_pe": 21.8, "trailing_pe": 24.1, "price_to_sales": 6.3, "fcf_yield": 4.15, "roic": 28.4, "ebitda_margin": 38.2, "capex_to_revenue": 12.1, "revenue_growth": 14.2, "rsi_14": 55.1, "beta_90d": 1.12, "put_call_ratio": 0.72, "fifty_two_week_high": 191.75, "fifty_two_week_low": 142.50, "dividend_yield": 0.48, "sector": "Communication Services"},
    "AMZN": {"name": "Amazon", "current_price": 205.30, "market_cap": 2.18e12, "peg_ratio": 1.85, "forward_pe": 35.2, "trailing_pe": 42.8, "price_to_sales": 3.5, "fcf_yield": 2.92, "roic": 17.8, "ebitda_margin": 25.4, "capex_to_revenue": 11.8, "revenue_growth": 10.5, "rsi_14": 64.8, "beta_90d": 1.18, "put_call_ratio": 0.81, "fifty_two_week_high": 221.40, "fifty_two_week_low": 161.20, "dividend_yield": None, "sector": "Consumer Cyclical"},
    "NVDA": {"name": "NVIDIA", "current_price": 138.50, "market_cap": 3.39e12, "peg_ratio": 1.25, "forward_pe": 30.5, "trailing_pe": 55.2, "price_to_sales": 25.8, "fcf_yield": 2.18, "roic": 82.5, "ebitda_margin": 64.9, "capex_to_revenue": 3.2, "revenue_growth": 78.4, "rsi_14": 71.5, "beta_90d": 1.68, "put_call_ratio": 0.92, "fifty_two_week_high": 153.20, "fifty_two_week_low": 86.40, "dividend_yield": 0.03, "sector": "Technology"},
    "META": {"name": "Meta Platforms", "current_price": 612.75, "market_cap": 1.55e12, "peg_ratio": 1.58, "forward_pe": 23.6, "trailing_pe": 27.4, "price_to_sales": 10.2, "fcf_yield": 3.78, "roic": 33.1, "ebitda_margin": 49.5, "capex_to_revenue": 19.8, "revenue_growth": 18.2, "rsi_14": 59.4, "beta_90d": 1.32, "put_call_ratio": 0.68, "fifty_two_week_high": 638.40, "fifty_two_week_low": 442.10, "dividend_yield": 0.33, "sector": "Communication Services"},
    "TSLA": {"name": "Tesla", "current_price": 272.15, "market_cap": 873e9, "peg_ratio": 3.85, "forward_pe": 85.2, "trailing_pe": 145.6, "price_to_sales": 8.9, "fcf_yield": 1.12, "roic": 12.4, "ebitda_margin": 17.8, "capex_to_revenue": 8.5, "revenue_growth": 7.2, "rsi_14": 73.2, "beta_90d": 1.92, "put_call_ratio": 1.15, "fifty_two_week_high": 315.80, "fifty_two_week_low": 138.80, "dividend_yield": None, "sector": "Consumer Cyclical"},
}


def _generate_demo_history(ticker_symbol: str, period: str = "1y") -> list:
    demo = DEMO_DATA.get(ticker_symbol)
    if not demo:
        return []
    period_days = {"1mo": 22, "3mo": 66, "6mo": 132, "1y": 252, "2y": 504, "5y": 1260}
    days = period_days.get(period, 252)
    current_price = demo["current_price"]
    prices = [current_price]
    volatility = 0.015 * (demo.get("beta_90d", 1.0) or 1.0)
    random.seed(hash(ticker_symbol + period))
    for i in range(days - 1):
        change = random.gauss(0.0003, volatility)
        prices.append(prices[-1] / (1 + change))
    prices.reverse()
    records = []
    start_date = datetime.now() - timedelta(days=days)
    for i, price in enumerate(prices):
        date = start_date + timedelta(days=i * (days / len(prices)))
        if date.weekday() < 5:
            records.append({"date": date.strftime("%Y-%m-%d"), "close": round(price, 2), "volume": random.randint(20_000_000, 120_000_000)})
    return records


def _generate_demo_stock(ticker_symbol: str) -> Dict[str, Any]:
    demo = DEMO_DATA.get(ticker_symbol)
    if not demo:
        return {}
    result = {"ticker": ticker_symbol, "name": demo["name"], "current_price": demo["current_price"],
              "market_cap": demo["market_cap"], "currency": "USD", "sector": demo["sector"],
              "updated_at": datetime.now().isoformat(), "is_demo": True}
    for key in ["peg_ratio", "forward_pe", "trailing_pe", "price_to_sales", "fcf_yield", "roic",
                "ebitda_margin", "capex_to_revenue", "revenue_growth", "rsi_14", "beta_90d",
                "put_call_ratio", "fifty_two_week_high", "fifty_two_week_low", "dividend_yield"]:
        result[key] = demo.get(key)
    result["peg_alert"] = get_alert_level("peg_ratio", result["peg_ratio"])
    result["fcf_yield_alert"] = get_alert_level("fcf_yield", result["fcf_yield"])
    result["roic_alert"] = get_alert_level("roic", result["roic"])
    result["rsi_alert"] = get_alert_level("rsi", result["rsi_14"])
    result["beta_alert"] = get_alert_level("beta", result["beta_90d"])
    result["bubble_risk"] = calculate_bubble_risk(result)
    return result


# ==================== MAIN FETCH ====================

def fetch_all_mag7() -> Dict[str, Any]:
    """Busca dados de todas as Magnificent 7. Estratégia: FMP → Yahoo → yfinance → demo."""
    logger.info("Iniciando coleta de dados das Magnificent 7...")

    tickers = list(MAG7_TICKERS.keys())
    stocks = []
    errors = []
    live_count = 0
    data_source = "demo"

    sp500_prices = fetch_sp500_prices()

    # ======= ESTRATÉGIA 1: FMP (batch) =======
    if _fmp_available():
        logger.info("[FMP] API key detectada, buscando dados...")
        try:
            quotes = _fmp_bulk_quote(tickers)
            if quotes and isinstance(quotes, list) and len(quotes) > 0:
                logger.info(f"[FMP] Bulk quote retornou {len(quotes)} tickers")
                quote_map = {q["symbol"]: q for q in quotes if "symbol" in q}

                for ticker_symbol in tickers:
                    q = quote_map.get(ticker_symbol)
                    if not q or q.get("price") is None:
                        continue

                    _time.sleep(0.3)
                    metrics_list = _fmp_key_metrics(ticker_symbol)
                    metrics = metrics_list[0] if metrics_list and isinstance(metrics_list, list) else None

                    ratios_list = _fmp_ratios(ticker_symbol)
                    ratios = ratios_list[0] if ratios_list and isinstance(ratios_list, list) else None

                    cashflow_list = _fmp_cashflow(ticker_symbol)
                    cashflow = cashflow_list[0] if cashflow_list and isinstance(cashflow_list, list) else None

                    try:
                        stock_data = _parse_fmp_to_stock(q, metrics, ratios, cashflow, sp500_prices)

                        # RSI from historical prices
                        if stock_data.get("rsi_14") is None:
                            try:
                                to_d = datetime.now().strftime("%Y-%m-%d")
                                from_d = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
                                hist_data = _fmp_historical(ticker_symbol, from_d, to_d)
                                if hist_data and "historical" in hist_data and hist_data["historical"]:
                                    hist = hist_data["historical"]
                                    hist.sort(key=lambda x: x["date"])
                                    close_series = pd.Series(
                                        [h["close"] for h in hist],
                                        index=pd.to_datetime([h["date"] for h in hist])
                                    )
                                    stock_data["rsi_14"] = calculate_rsi(close_series)
                                    stock_data["rsi_alert"] = get_alert_level("rsi", stock_data["rsi_14"])

                                    if not sp500_prices.empty:
                                        calc_beta = calculate_beta(close_series, sp500_prices)
                                        if calc_beta is not None:
                                            stock_data["beta_90d"] = calc_beta
                                            stock_data["beta_alert"] = get_alert_level("beta", stock_data["beta_90d"])

                                    stock_data["bubble_risk"] = calculate_bubble_risk(stock_data)
                            except Exception as e:
                                logger.debug(f"FMP RSI calc error {ticker_symbol}: {e}")

                        if stock_data.get("current_price") is not None:
                            live_count += 1
                            logger.info(f"  ✓ {ticker_symbol}: ${stock_data['current_price']} (FMP)")
                            stocks.append(stock_data)
                    except Exception as e:
                        logger.warning(f"  ✗ FMP parse {ticker_symbol}: {e}")

                if live_count > 0:
                    data_source = "fmp"
        except Exception as e:
            logger.warning(f"[FMP] Bulk fetch falhou: {e}")

    # ======= ESTRATÉGIA 2: Yahoo API / yfinance =======
    can_use_yfinance = _check_connectivity()
    remaining_tickers = [t for t in tickers if not any(s.get("ticker") == t for s in stocks)]

    if remaining_tickers and can_use_yfinance:
        logger.info(f"[Yahoo/yfinance] Tentando {len(remaining_tickers)} tickers restantes...")
        for idx, ticker_symbol in enumerate(remaining_tickers):
            stock_data = None
            if idx > 0:
                _time.sleep(1.5)

            try:
                api_data = _yahoo_api_quote(ticker_symbol)
                if api_data:
                    stock_data = _parse_yahoo_api_to_stock(ticker_symbol, api_data, sp500_prices)
                    if stock_data and stock_data.get("current_price") is not None:
                        live_count += 1
                        logger.info(f"  ✓ {ticker_symbol}: ${stock_data['current_price']} (Yahoo API)")
                    else:
                        stock_data = None
            except Exception as e:
                logger.warning(f"  ✗ Yahoo API {ticker_symbol}: {e}")

            if stock_data is None:
                try:
                    stock_data = fetch_single_stock(ticker_symbol, sp500_prices)
                    if stock_data and stock_data.get("current_price") is not None:
                        live_count += 1
                        logger.info(f"  ✓ {ticker_symbol}: ${stock_data['current_price']} (yfinance)")
                    else:
                        stock_data = None
                except Exception as e:
                    logger.warning(f"  ✗ yfinance {ticker_symbol}: {e}")

            if stock_data:
                stocks.append(stock_data)

        if live_count > 0 and data_source == "demo":
            data_source = "yahoo"

    # ======= FALLBACK: Demo =======
    fetched_tickers = {s["ticker"] for s in stocks}
    for ticker_symbol in tickers:
        if ticker_symbol not in fetched_tickers:
            try:
                stocks.append(_generate_demo_stock(ticker_symbol))
            except Exception as e2:
                errors.append({"ticker": ticker_symbol, "error": str(e2)})

    ticker_order = {t: i for i, t in enumerate(tickers)}
    stocks.sort(key=lambda s: ticker_order.get(s.get("ticker", ""), 99))

    # Market summary
    rsi_values = [s["rsi_14"] for s in stocks if s.get("rsi_14") is not None]
    avg_rsi = round(np.mean(rsi_values), 2) if rsi_values else None
    all_overbought = all(r > 70 for r in rsi_values) if rsi_values else False

    bubble_scores = [(s.get("bubble_risk", {}).get("score"), s.get("market_cap", 0) or 0)
                     for s in stocks if s.get("bubble_risk", {}).get("score") is not None]
    if bubble_scores:
        total_cap = sum(cap for _, cap in bubble_scores)
        base_avg = sum(score * cap / total_cap for score, cap in bubble_scores) if total_cap > 0 else np.mean([s for s, _ in bubble_scores])

        elevated_count = sum(1 for score, _ in bubble_scores if score >= 35)
        total_count = len(bubble_scores)
        convergence_premium = (elevated_count / total_count) * 15 if elevated_count > total_count * 0.5 else 0

        score_values = [s for s, _ in bubble_scores]
        std_dev = np.std(score_values)
        concentration_premium = (10 - std_dev) / 10 * 5 if std_dev < 10 and base_avg > 30 else 0

        avg_bubble = round(min(base_avg + convergence_premium + concentration_premium, 100), 1)
        convergence_detail = {
            "base_score": round(base_avg, 1), "convergence_premium": round(convergence_premium, 1),
            "concentration_premium": round(concentration_premium, 1),
            "elevated_count": elevated_count, "total_count": total_count,
        }
    else:
        avg_bubble = None
        convergence_detail = None

    market_summary = {
        "avg_rsi": avg_rsi, "all_overbought": all_overbought,
        "total_market_cap": sum(s.get("market_cap", 0) or 0 for s in stocks),
        "stocks_count": len(stocks), "errors_count": len(errors),
        "bubble_risk_index": avg_bubble, "convergence_detail": convergence_detail,
    }

    return {
        "stocks": stocks, "market_summary": market_summary,
        "thresholds": ALERT_THRESHOLDS, "errors": errors,
        "updated_at": datetime.now().isoformat(),
        "is_demo": live_count == 0, "live_count": live_count,
        "total_count": len(MAG7_TICKERS), "data_source": data_source,
    }
