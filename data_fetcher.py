"""
data_fetcher.py — Módulo de coleta de dados das Magnificent 7 via yfinance.

Responsável por buscar todos os indicadores do plano de monitoramento:
  1. Valuation: PEG, ROIC, FCF Yield, Forward P/E
  2. Crescimento/Risco: CAPEX/Receita, EBITDA Margin, P/S
  3. Sentimento/Real-Time: RSI (14d), Beta, Put/Call Ratio

Educacional: yfinance é um wrapper Python que faz scraping do Yahoo Finance.
Não requer API key, mas tem limitações de rate e pode quebrar se o Yahoo
mudar a estrutura do site. Para produção, migrar para FMP ou Alpha Vantage.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import logging
import traceback
import random
import requests
import time as _time

# Tenta importar yfinance — se falhar ou se não houver conectividade, usa modo demo
try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False

# ==================== YAHOO FINANCE DIRECT API ====================
# Fallback para quando yfinance falha (IPs de datacenter bloqueados).
# Usa a API pública do Yahoo Finance v8 com headers de browser.

_YAHOO_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}
_yahoo_session = requests.Session()
_yahoo_session.headers.update(_YAHOO_HEADERS)
_yahoo_crumb = None
_yahoo_crumb_ts = 0


def _get_yahoo_crumb() -> Optional[str]:
    """Obtém crumb do Yahoo Finance (necessário para API v8)."""
    global _yahoo_crumb, _yahoo_crumb_ts
    # Cache crumb por 30min
    if _yahoo_crumb and (_time.time() - _yahoo_crumb_ts < 1800):
        return _yahoo_crumb
    try:
        # Visita uma página para obter cookies
        _yahoo_session.get("https://finance.yahoo.com/quote/AAPL", timeout=10)
        # Obtém crumb
        resp = _yahoo_session.get("https://query2.finance.yahoo.com/v1/test/getcrumb", timeout=10)
        if resp.status_code == 200 and resp.text:
            _yahoo_crumb = resp.text.strip()
            _yahoo_crumb_ts = _time.time()
            logger.info(f"Yahoo crumb obtido: {_yahoo_crumb[:8]}...")
            return _yahoo_crumb
    except Exception as e:
        logger.warning(f"Falha ao obter Yahoo crumb: {e}")
    return None


def _yahoo_api_quote(ticker_symbol: str) -> Optional[Dict]:
    """Busca dados de uma ação via Yahoo Finance API v10/quoteSummary (fallback)."""
    modules = "price,summaryDetail,defaultKeyStatistics,financialData,earnings"
    url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker_symbol}"
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

    # Fallback: tenta v6 quote
    try:
        url2 = f"https://query2.finance.yahoo.com/v6/finance/quote"
        resp2 = _yahoo_session.get(url2, params={"symbols": ticker_symbol}, timeout=15)
        if resp2.status_code == 200:
            data2 = resp2.json()
            quotes = data2.get("quoteResponse", {}).get("result", [])
            if quotes:
                return {"v6_quote": quotes[0]}
    except Exception as e:
        logger.warning(f"Yahoo API v6 também falhou para {ticker_symbol}: {e}")

    return None


def _yahoo_api_history(ticker_symbol: str, period: str = "6mo") -> Optional[pd.DataFrame]:
    """Busca histórico de preços via Yahoo Finance API v8/chart."""
    period_map = {"1mo": "1mo", "3mo": "3mo", "6mo": "6mo", "1y": "1y", "2y": "2y", "5y": "5y"}
    yperiod = period_map.get(period, "6mo")

    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{ticker_symbol}"
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
    """Converte resposta da Yahoo API direta para o formato do dashboard."""
    result = {
        "ticker": ticker_symbol,
        "name": MAG7_TICKERS.get(ticker_symbol, ticker_symbol),
        "currency": "USD",
        "updated_at": datetime.now().isoformat(),
    }

    # Trata formato v10 (quoteSummary)
    if "v6_quote" in api_data:
        q = api_data["v6_quote"]
        result["current_price"] = q.get("regularMarketPrice")
        result["market_cap"] = q.get("marketCap")
        result["sector"] = q.get("sector")
        result["forward_pe"] = q.get("forwardPE")
        result["trailing_pe"] = q.get("trailingPE")
        result["price_to_sales"] = q.get("priceToSales")
        result["peg_ratio"] = q.get("pegRatio")
        result["fifty_two_week_high"] = q.get("fiftyTwoWeekHigh")
        result["fifty_two_week_low"] = q.get("fiftyTwoWeekLow")
        result["ebitda_margin"] = None
        result["revenue_growth"] = None
        result["roic"] = None
        result["fcf_yield"] = None
        result["capex_to_revenue"] = None
        result["dividend_yield"] = q.get("trailingAnnualDividendYield")
        if result["dividend_yield"]:
            result["dividend_yield"] = round(result["dividend_yield"] * 100, 2)
    else:
        price = api_data.get("price", {})
        detail = api_data.get("summaryDetail", {})
        stats = api_data.get("defaultKeyStatistics", {})
        fin = api_data.get("financialData", {})

        def raw(d, key):
            v = d.get(key, {})
            if isinstance(v, dict):
                return v.get("raw")
            return v

        result["current_price"] = raw(price, "regularMarketPrice")
        result["market_cap"] = raw(price, "marketCap")
        result["sector"] = price.get("sector")
        result["forward_pe"] = raw(stats, "forwardPE") or raw(detail, "forwardPE")
        result["trailing_pe"] = raw(detail, "trailingPE")
        result["price_to_sales"] = raw(detail, "priceToSalesTrailing12Months") or raw(stats, "priceToSalesTrailing12Months")
        result["peg_ratio"] = raw(stats, "pegRatio")
        result["fifty_two_week_high"] = raw(detail, "fiftyTwoWeekHigh")
        result["fifty_two_week_low"] = raw(detail, "fiftyTwoWeekLow")

        # FCF Yield
        fcf = raw(fin, "freeCashflow")
        mcap = result["market_cap"]
        if fcf and mcap and mcap > 0:
            result["fcf_yield"] = round((fcf / mcap) * 100, 2)
        else:
            result["fcf_yield"] = None

        # ROIC via ROE
        roe = raw(fin, "returnOnEquity")
        result["roic"] = round(roe * 100, 2) if roe else None

        # EBITDA Margin
        em = raw(fin, "ebitdaMargins")
        result["ebitda_margin"] = round(em * 100, 2) if em else None

        # Revenue Growth
        rg = raw(fin, "revenueGrowth")
        result["revenue_growth"] = round(rg * 100, 2) if rg else None

        result["capex_to_revenue"] = None  # Não disponível nesta API
        result["dividend_yield"] = raw(detail, "dividendYield")
        if result["dividend_yield"]:
            result["dividend_yield"] = round(result["dividend_yield"] * 100, 2)

    # RSI e Beta via histórico
    hist_df = _yahoo_api_history(ticker_symbol, "3mo")
    if hist_df is not None and not hist_df.empty:
        result["rsi_14"] = calculate_rsi(hist_df["Close"])
        result["beta_90d"] = calculate_beta(hist_df["Close"], sp500_prices)
    else:
        result["rsi_14"] = None
        result["beta_90d"] = None

    # Put/Call (não disponível via API direta, fica None)
    result["put_call_ratio"] = None
    result["avg_volume"] = None

    # Alertas
    result["peg_alert"] = get_alert_level("peg_ratio", result.get("peg_ratio"))
    result["fcf_yield_alert"] = get_alert_level("fcf_yield", result.get("fcf_yield"))
    result["roic_alert"] = get_alert_level("roic", result.get("roic"))
    result["rsi_alert"] = get_alert_level("rsi", result.get("rsi_14"))
    result["beta_alert"] = get_alert_level("beta", result.get("beta_90d"))

    # Bubble Risk
    result["bubble_risk"] = calculate_bubble_risk(result)

    return result

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
    "roic": {"warning": 22.0, "danger": 20.0},       # abaixo = ruim
    "fcf_yield": {"warning": 3.5, "danger": 3.0},     # abaixo = ruim
    "forward_pe_deviation": {"warning": 15.0, "danger": 20.0},  # % acima da média
    "rsi": {"overbought": 70, "oversold": 30},
    "beta": {"warning": 1.3, "danger": 1.5},
    "cloud_growth": {"warning": 18.0, "danger": 15.0},  # abaixo = ruim
}


def calculate_rsi(prices: pd.Series, period: int = 14) -> Optional[float]:
    """
    Calcula o RSI (Relative Strength Index) de 14 dias.

    Educacional: O RSI mede a velocidade e magnitude das mudanças de preço.
    - RSI > 70 = sobrecompra (overbought) → possível correção para baixo
    - RSI < 30 = sobrevenda (oversold) → possível recuperação

    Fórmula:
    1. Calcula ganhos e perdas diários
    2. Média móvel exponencial dos ganhos e perdas
    3. RS = média_ganhos / média_perdas
    4. RSI = 100 - (100 / (1 + RS))
    """
    if prices is None or len(prices) < period + 1:
        return None

    delta = prices.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    # Usa média móvel exponencial (EMA) como o Wilder recomenda
    avg_gain = gain.ewm(alpha=1/period, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    return round(float(rsi.iloc[-1]), 2)


def calculate_beta(stock_prices: pd.Series, market_prices: pd.Series, window: int = 90) -> Optional[float]:
    """
    Calcula o Beta de 90 dias em relação ao S&P 500.

    Educacional: Beta mede a sensibilidade do ativo ao mercado.
    - Beta = 1.0 → move igual ao mercado
    - Beta > 1.5 → muito mais volátil que o mercado (maior risco)
    - Beta < 1.0 → menos volátil (mais defensivo)

    Fórmula: Beta = Cov(Ri, Rm) / Var(Rm)
    """
    if stock_prices is None or market_prices is None:
        return None
    if len(stock_prices) < window or len(market_prices) < window:
        return None

    stock_returns = stock_prices.pct_change().dropna().tail(window)
    market_returns = market_prices.pct_change().dropna().tail(window)

    # Alinha os índices
    aligned = pd.DataFrame({"stock": stock_returns, "market": market_returns}).dropna()

    if len(aligned) < 20:
        return None

    covariance = aligned["stock"].cov(aligned["market"])
    market_variance = aligned["market"].var()

    if market_variance == 0:
        return None

    return round(covariance / market_variance, 2)


def calculate_put_call_ratio(ticker: yf.Ticker) -> Optional[float]:
    """
    Calcula o Put/Call Ratio a partir da options chain mais próxima do vencimento.

    Educacional: Put/Call Ratio = Volume de Puts / Volume de Calls
    - Ratio > 1.0 → mais gente comprando proteção (bearish sentiment)
    - Ratio < 0.7 → otimismo excessivo (pode indicar complacência)
    - Aumento súbito → institucionais se protegendo (sinal de alerta)
    """
    try:
        expirations = ticker.options
        if not expirations:
            return None

        # Pega a expiração mais próxima
        chain = ticker.option_chain(expirations[0])

        call_volume = chain.calls["volume"].sum()
        put_volume = chain.puts["volume"].sum()

        if call_volume == 0 or pd.isna(call_volume):
            return None

        return round(float(put_volume / call_volume), 2)
    except Exception as e:
        logger.warning(f"Erro ao buscar options: {e}")
        return None


def get_alert_level(metric: str, value: Optional[float]) -> str:
    """
    Retorna o nível de alerta baseado nos limiares do plano.
    Returns: 'green', 'yellow', ou 'red'
    """
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
    """
    Calcula o Índice de Risco de Bolha (Bubble Risk Index) de 0 a 100.

    Educacional: Este é um índice composto que sintetiza múltiplos indicadores
    em um score único. Nenhum indicador isolado detecta bolha — é a CONVERGÊNCIA
    de sinais de excesso que produz alertas confiáveis.

    v2 — Pesos recalibrados:
      1. P/S Ratio (25%)          — mais difícil de manipular, melhor proxy de excesso
      2. PEG Ratio (20%)          — preço vs crescimento
      3. FCF Yield ajustado (15%) — ajustado pelo crescimento (growth-aware)
      4. Forward P/E (15%)        — expectativas do mercado
      5. RSI momentum (10%)       — sobrecompra técnica
      6. Put/Call (10%)           — sentimento de mercado
      7. Beta (5%)                — amplificador, não indicador direto

    Mudanças em relação à v1:
      - P/S subiu de 15% → 25% (métrica mais robusta contra manipulação)
      - FCF Yield caiu de 20% → 15% e agora é ajustado pelo crescimento
        (FCF baixo + growth alto = aceitável; FCF baixo + growth baixo = alarmante)
      - Forward P/E subiu de 10% → 15% (valuation direto, peso justo)
      - Beta caiu de 10% → 5% (volatilidade ≠ bolha)
      - RSI caiu de 15% → 10% (indicador de curto prazo)

    Faixas de interpretação:
      0-30   → Baixo risco (saudável)
      30-50  → Moderado (atenção)
      50-70  → Elevado (cautela recomendada)
      70-100 → Extremo / território de bolha
    """
    scores = {}
    weights = {}

    # 1. P/S RATIO (25%) — Principal indicador: vendas são difíceis de manipular
    # Escala não-linear: cresce mais rápido em P/S extremos
    ps = stock.get("price_to_sales")
    if ps is not None:
        # P/S < 4 = score 0; P/S 4-10 = zona normal; P/S 10-20 = alerta; P/S 20+ = extremo
        if ps <= 4:
            scores["ps"] = 0
        elif ps <= 12:
            scores["ps"] = (ps - 4) / 8 * 40          # 0-40 linear
        else:
            scores["ps"] = 40 + (ps - 12) / 13 * 60   # 40-100, acelera em valores altos
        scores["ps"] = min(scores["ps"], 100)
        weights["ps"] = 0.25
    else:
        weights["ps"] = 0

    # 2. PEG RATIO (20%) — Preço vs crescimento
    # PEG < 1 = subvalorizado; 1-2 = justo; 2-3 = caro; 3+ = bolha
    peg = stock.get("peg_ratio")
    if peg is not None and peg > 0:
        if peg <= 1.0:
            scores["peg"] = 0
        elif peg <= 2.0:
            scores["peg"] = (peg - 1.0) / 1.0 * 30    # 0-30
        else:
            scores["peg"] = 30 + (peg - 2.0) / 2.0 * 70  # 30-100, acelera
        scores["peg"] = min(scores["peg"], 100)
        weights["peg"] = 0.20
    else:
        weights["peg"] = 0

    # 3. FCF YIELD AJUSTADO POR CRESCIMENTO (15%)
    # Inovação: penaliza FCF baixo apenas se o crescimento não justifica
    # FCF Yield baixo + Growth alto = investindo no futuro (ok)
    # FCF Yield baixo + Growth baixo = preço descolado da realidade (alerta)
    fcf = stock.get("fcf_yield")
    rev_growth = stock.get("revenue_growth")
    if fcf is not None:
        # Score base: 6%+ = 0, 0% = 100
        base_score = min(max((6.0 - fcf) / 6.0 * 100, 0), 100)

        # Fator de ajuste: se revenue growth > 30%, reduz o score em até 40%
        # Lógica: empresa crescendo 78% (NVDA) pode ter FCF baixo legitimamente
        if rev_growth is not None and rev_growth > 10:
            growth_discount = min((rev_growth - 10) / 50 * 0.4, 0.4)  # máx 40% desconto
            base_score = base_score * (1 - growth_discount)

        scores["fcf"] = round(base_score, 1)
        weights["fcf"] = 0.15
    else:
        weights["fcf"] = 0

    # 4. FORWARD P/E (15%) — Expectativa do mercado
    # Escala não-linear: P/E 20 = normal para tech; 40+ = esticado; 80+ = euforia
    fpe = stock.get("forward_pe")
    if fpe is not None:
        if fpe <= 20:
            scores["fpe"] = 0
        elif fpe <= 40:
            scores["fpe"] = (fpe - 20) / 20 * 40       # 0-40
        else:
            scores["fpe"] = 40 + (fpe - 40) / 40 * 60  # 40-100
        scores["fpe"] = min(scores["fpe"], 100)
        weights["fpe"] = 0.15
    else:
        weights["fpe"] = 0

    # 5. RSI MOMENTUM (10%) — Sobrecompra técnica
    # RSI 50 = neutro; 65+ = quente; 75+ = sobrecompra clara; 85+ = euforia
    rsi = stock.get("rsi_14")
    if rsi is not None:
        if rsi <= 55:
            scores["rsi"] = 0
        elif rsi <= 70:
            scores["rsi"] = (rsi - 55) / 15 * 50       # 0-50
        else:
            scores["rsi"] = 50 + (rsi - 70) / 15 * 50  # 50-100
        scores["rsi"] = min(scores["rsi"], 100)
        weights["rsi"] = 0.10
    else:
        weights["rsi"] = 0

    # 6. PUT/CALL COMPLACÊNCIA (10%)
    # Invertido: ratio baixo = otimismo excessivo = mais risco
    pc = stock.get("put_call_ratio")
    if pc is not None:
        # Ratio 1.0+ = score 0 (proteção saudável)
        # Ratio 0.4 = score 100 (ninguém se protege)
        scores["pc"] = min(max((1.0 - pc) / 0.6 * 100, 0), 100)
        weights["pc"] = 0.10
    else:
        weights["pc"] = 0

    # 7. BETA COMO AMPLIFICADOR (5%) — Não é indicador de bolha, mas amplifica danos
    beta = stock.get("beta_90d")
    if beta is not None:
        # Beta 1.0 = neutro; 1.5+ = amplificação relevante; 2.0+ = alto
        scores["beta"] = min(max((beta - 1.0) / 1.2 * 100, 0), 100)
        weights["beta"] = 0.05
    else:
        weights["beta"] = 0

    # Calcula score ponderado (rebalanceia pesos se algum componente estiver ausente)
    total_weight = sum(weights.values())
    if total_weight == 0:
        return {"score": None, "level": "gray", "components": {}}

    weighted_score = sum(scores.get(k, 0) * (w / total_weight) for k, w in weights.items())
    final_score = round(weighted_score, 1)

    # Determina nível
    if final_score >= 70:
        level = "extreme"
    elif final_score >= 50:
        level = "high"
    elif final_score >= 30:
        level = "moderate"
    else:
        level = "low"

    # Componente mais crítico (maior contribuição absoluta ao score)
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
    """Extrai valor do dict info do yfinance de forma segura."""
    val = info.get(key, default)
    if val is None or (isinstance(val, float) and (np.isnan(val) or np.isinf(val))):
        return default
    return val


def fetch_single_stock(ticker_symbol: str, sp500_prices: pd.Series) -> Dict[str, Any]:
    """
    Busca todos os indicadores de uma única ação.

    Retorna um dicionário com todos os dados necessários para o dashboard.
    """
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

    # === 1. INDICADORES DE VALUATION ===

    # PEG Ratio — diretamente do yfinance
    result["peg_ratio"] = safe_get(info, "pegRatio")
    result["peg_alert"] = get_alert_level("peg_ratio", result["peg_ratio"])

    # Forward P/E
    result["forward_pe"] = safe_get(info, "forwardPE")

    # Trailing P/E (para comparação)
    result["trailing_pe"] = safe_get(info, "trailingPE")

    # P/S (Price to Sales)
    result["price_to_sales"] = safe_get(info, "priceToSalesTrailing12Months")

    # FCF Yield = Free Cash Flow / Market Cap * 100
    fcf = safe_get(info, "freeCashflow")
    mcap = safe_get(info, "marketCap")
    if fcf and mcap and mcap > 0:
        result["fcf_yield"] = round((fcf / mcap) * 100, 2)
    else:
        result["fcf_yield"] = None
    result["fcf_yield_alert"] = get_alert_level("fcf_yield", result["fcf_yield"])

    # ROIC = EBIT * (1 - tax_rate) / (Total Assets - Current Liabilities)
    # yfinance não tem ROIC direto, então calculamos
    result["roic"] = safe_get(info, "returnOnEquity")  # ROE como proxy inicial
    if result["roic"] is not None:
        result["roic"] = round(result["roic"] * 100, 2)
    result["roic_alert"] = get_alert_level("roic", result["roic"])

    # === 2. INDICADORES DE CRESCIMENTO/RISCO ===

    # EBITDA Margin
    ebitda_margin = safe_get(info, "ebitdaMargins")
    if ebitda_margin is not None:
        result["ebitda_margin"] = round(ebitda_margin * 100, 2)
    else:
        result["ebitda_margin"] = None

    # Revenue Growth
    rev_growth = safe_get(info, "revenueGrowth")
    if rev_growth is not None:
        result["revenue_growth"] = round(rev_growth * 100, 2)
    else:
        result["revenue_growth"] = None

    # CAPEX / Revenue — dos demonstrativos financeiros
    try:
        cashflow = ticker.cashflow
        income = ticker.income_stmt

        if cashflow is not None and not cashflow.empty and income is not None and not income.empty:
            # Pega o último ano fiscal
            capex = None
            revenue = None

            # CAPEX pode estar em diferentes nomes
            for capex_name in ["Capital Expenditure", "CapitalExpenditure"]:
                if capex_name in cashflow.index:
                    capex = abs(float(cashflow.loc[capex_name].iloc[0]))
                    break

            for rev_name in ["Total Revenue", "TotalRevenue"]:
                if rev_name in income.index:
                    revenue = float(income.loc[rev_name].iloc[0])
                    break

            if capex and revenue and revenue > 0:
                result["capex_to_revenue"] = round((capex / revenue) * 100, 2)
            else:
                result["capex_to_revenue"] = None
        else:
            result["capex_to_revenue"] = None
    except Exception as e:
        logger.warning(f"Erro ao buscar CAPEX para {ticker_symbol}: {e}")
        result["capex_to_revenue"] = None

    # === 3. INDICADORES DE SENTIMENTO / REAL-TIME ===

    # RSI (14 dias) — calculado do histórico de preços
    try:
        hist = ticker.history(period="3mo")
        if hist is not None and not hist.empty:
            result["rsi_14"] = calculate_rsi(hist["Close"])

            # Beta (90 dias) — calculado vs S&P 500
            result["beta_90d"] = calculate_beta(hist["Close"], sp500_prices)
        else:
            result["rsi_14"] = None
            result["beta_90d"] = None
    except Exception as e:
        logger.warning(f"Erro ao calcular RSI/Beta para {ticker_symbol}: {e}")
        result["rsi_14"] = None
        result["beta_90d"] = None

    result["rsi_alert"] = get_alert_level("rsi", result["rsi_14"])

    # Beta do yfinance como fallback
    if result["beta_90d"] is None:
        result["beta_90d"] = safe_get(info, "beta")
    result["beta_alert"] = get_alert_level("beta", result["beta_90d"])

    # Put/Call Ratio
    result["put_call_ratio"] = calculate_put_call_ratio(ticker)

    # Dados extras úteis
    result["fifty_two_week_high"] = safe_get(info, "fiftyTwoWeekHigh")
    result["fifty_two_week_low"] = safe_get(info, "fiftyTwoWeekLow")
    result["avg_volume"] = safe_get(info, "averageVolume")
    result["dividend_yield"] = safe_get(info, "dividendYield")
    if result["dividend_yield"] is not None:
        result["dividend_yield"] = round(result["dividend_yield"] * 100, 2)

    # === BUBBLE RISK INDEX ===
    result["bubble_risk"] = calculate_bubble_risk(result)

    return result


def fetch_sp500_prices() -> pd.Series:
    """Busca preços históricos do S&P 500 para cálculo de Beta."""
    # Tenta yfinance primeiro
    if YFINANCE_AVAILABLE:
        try:
            sp500 = yf.Ticker("^GSPC")
            hist = sp500.history(period="6mo")
            if hist is not None and not hist.empty:
                return hist["Close"]
        except Exception as e:
            logger.warning(f"yfinance S&P 500 falhou: {e}")

    # Fallback: Yahoo API direta
    df = _yahoo_api_history("^GSPC", "6mo")
    if df is not None and not df.empty:
        logger.info("S&P 500 obtido via Yahoo API direta")
        return df["Close"]

    logger.warning("Não conseguiu buscar S&P 500 por nenhum método")
    return pd.Series()


def fetch_price_history(ticker_symbol: str, period: str = "1y") -> list:
    """Busca histórico de preços para gráficos."""
    # Tenta yfinance primeiro
    if YFINANCE_AVAILABLE:
        try:
            ticker = yf.Ticker(ticker_symbol)
            hist = ticker.history(period=period)
            if hist is not None and not hist.empty:
                records = []
                for date, row in hist.iterrows():
                    records.append({
                        "date": date.strftime("%Y-%m-%d"),
                        "close": round(float(row["Close"]), 2),
                        "volume": int(row["Volume"]),
                    })
                return records
        except Exception as e:
            logger.warning(f"yfinance histórico falhou para {ticker_symbol}: {e}")

    # Fallback: Yahoo API direta
    df = _yahoo_api_history(ticker_symbol, period)
    if df is not None and not df.empty:
        logger.info(f"Histórico de {ticker_symbol} obtido via Yahoo API direta")
        records = []
        for date, row in df.iterrows():
            records.append({
                "date": date.strftime("%Y-%m-%d"),
                "close": round(float(row["Close"]), 2),
                "volume": int(row["Volume"]) if pd.notna(row.get("Volume")) else 0,
            })
        return records

    # Último fallback: demo
    return _generate_demo_history(ticker_symbol, period)


def _check_connectivity() -> bool:
    """Verifica se yfinance está instalado (não faz request de rede)."""
    return YFINANCE_AVAILABLE


# ==================== DADOS DEMO ====================
# Dados realistas baseados em valores de mercado reais de abril 2026.
# Usados quando yfinance não está disponível (ex: sandbox, sem internet).
# Na sua máquina com internet, o sistema usa dados reais automaticamente.

DEMO_DATA = {
    "AAPL": {
        "name": "Apple", "current_price": 223.45, "market_cap": 3.41e12,
        "peg_ratio": 2.35, "forward_pe": 28.1, "trailing_pe": 33.5, "price_to_sales": 8.7,
        "fcf_yield": 3.42, "roic": 58.2, "ebitda_margin": 33.8,
        "capex_to_revenue": 2.8, "revenue_growth": 5.1, "rsi_14": 62.3, "beta_90d": 1.21,
        "put_call_ratio": 0.78, "fifty_two_week_high": 242.10, "fifty_two_week_low": 171.30,
        "dividend_yield": 0.44, "sector": "Technology",
    },
    "MSFT": {
        "name": "Microsoft", "current_price": 445.80, "market_cap": 3.31e12,
        "peg_ratio": 2.18, "forward_pe": 32.4, "trailing_pe": 36.2, "price_to_sales": 13.9,
        "fcf_yield": 2.85, "roic": 35.6, "ebitda_margin": 52.1,
        "capex_to_revenue": 15.2, "revenue_growth": 13.8, "rsi_14": 58.7, "beta_90d": 1.05,
        "put_call_ratio": 0.65, "fifty_two_week_high": 468.35, "fifty_two_week_low": 362.90,
        "dividend_yield": 0.72, "sector": "Technology",
    },
    "GOOGL": {
        "name": "Alphabet", "current_price": 174.20, "market_cap": 2.14e12,
        "peg_ratio": 1.42, "forward_pe": 21.8, "trailing_pe": 24.1, "price_to_sales": 6.3,
        "fcf_yield": 4.15, "roic": 28.4, "ebitda_margin": 38.2,
        "capex_to_revenue": 12.1, "revenue_growth": 14.2, "rsi_14": 55.1, "beta_90d": 1.12,
        "put_call_ratio": 0.72, "fifty_two_week_high": 191.75, "fifty_two_week_low": 142.50,
        "dividend_yield": 0.48, "sector": "Communication Services",
    },
    "AMZN": {
        "name": "Amazon", "current_price": 205.30, "market_cap": 2.18e12,
        "peg_ratio": 1.85, "forward_pe": 35.2, "trailing_pe": 42.8, "price_to_sales": 3.5,
        "fcf_yield": 2.92, "roic": 17.8, "ebitda_margin": 25.4,
        "capex_to_revenue": 11.8, "revenue_growth": 10.5, "rsi_14": 64.8, "beta_90d": 1.18,
        "put_call_ratio": 0.81, "fifty_two_week_high": 221.40, "fifty_two_week_low": 161.20,
        "dividend_yield": None, "sector": "Consumer Cyclical",
    },
    "NVDA": {
        "name": "NVIDIA", "current_price": 138.50, "market_cap": 3.39e12,
        "peg_ratio": 1.25, "forward_pe": 30.5, "trailing_pe": 55.2, "price_to_sales": 25.8,
        "fcf_yield": 2.18, "roic": 82.5, "ebitda_margin": 64.9,
        "capex_to_revenue": 3.2, "revenue_growth": 78.4, "rsi_14": 71.5, "beta_90d": 1.68,
        "put_call_ratio": 0.92, "fifty_two_week_high": 153.20, "fifty_two_week_low": 86.40,
        "dividend_yield": 0.03, "sector": "Technology",
    },
    "META": {
        "name": "Meta Platforms", "current_price": 612.75, "market_cap": 1.55e12,
        "peg_ratio": 1.58, "forward_pe": 23.6, "trailing_pe": 27.4, "price_to_sales": 10.2,
        "fcf_yield": 3.78, "roic": 33.1, "ebitda_margin": 49.5,
        "capex_to_revenue": 19.8, "revenue_growth": 18.2, "rsi_14": 59.4, "beta_90d": 1.32,
        "put_call_ratio": 0.68, "fifty_two_week_high": 638.40, "fifty_two_week_low": 442.10,
        "dividend_yield": 0.33, "sector": "Communication Services",
    },
    "TSLA": {
        "name": "Tesla", "current_price": 272.15, "market_cap": 873e9,
        "peg_ratio": 3.85, "forward_pe": 85.2, "trailing_pe": 145.6, "price_to_sales": 8.9,
        "fcf_yield": 1.12, "roic": 12.4, "ebitda_margin": 17.8,
        "capex_to_revenue": 8.5, "revenue_growth": 7.2, "rsi_14": 73.2, "beta_90d": 1.92,
        "put_call_ratio": 1.15, "fifty_two_week_high": 315.80, "fifty_two_week_low": 138.80,
        "dividend_yield": None, "sector": "Consumer Cyclical",
    },
}


def _generate_demo_history(ticker_symbol: str, period: str = "1y") -> list:
    """Gera histórico de preços simulado (random walk a partir do preço atual)."""
    demo = DEMO_DATA.get(ticker_symbol)
    if not demo:
        return []

    period_days = {"1mo": 22, "3mo": 66, "6mo": 132, "1y": 252, "2y": 504, "5y": 1260}
    days = period_days.get(period, 252)

    current_price = demo["current_price"]
    # Simula random walk reverso a partir do preço atual
    prices = [current_price]
    volatility = 0.015 * (demo.get("beta_90d", 1.0) or 1.0)

    random.seed(hash(ticker_symbol + period))  # Determinístico para mesmo ticker/período
    for i in range(days - 1):
        change = random.gauss(0.0003, volatility)
        prices.append(prices[-1] / (1 + change))

    prices.reverse()  # Mais antigo primeiro

    records = []
    start_date = datetime.now() - timedelta(days=days)
    for i, price in enumerate(prices):
        date = start_date + timedelta(days=i * (days / len(prices)))
        if date.weekday() < 5:  # Só dias úteis
            records.append({
                "date": date.strftime("%Y-%m-%d"),
                "close": round(price, 2),
                "volume": random.randint(20_000_000, 120_000_000),
            })

    return records


def _generate_demo_stock(ticker_symbol: str) -> Dict[str, Any]:
    """Gera dados demo de uma ação a partir dos dados base."""
    demo = DEMO_DATA.get(ticker_symbol)
    if not demo:
        return {}

    result = {
        "ticker": ticker_symbol,
        "name": demo["name"],
        "current_price": demo["current_price"],
        "market_cap": demo["market_cap"],
        "currency": "USD",
        "sector": demo["sector"],
        "updated_at": datetime.now().isoformat(),
        "is_demo": True,
    }

    for key in ["peg_ratio", "forward_pe", "trailing_pe", "price_to_sales",
                "fcf_yield", "roic", "ebitda_margin", "capex_to_revenue",
                "revenue_growth", "rsi_14", "beta_90d", "put_call_ratio",
                "fifty_two_week_high", "fifty_two_week_low", "dividend_yield"]:
        result[key] = demo.get(key)

    # Calcula alertas
    result["peg_alert"] = get_alert_level("peg_ratio", result["peg_ratio"])
    result["fcf_yield_alert"] = get_alert_level("fcf_yield", result["fcf_yield"])
    result["roic_alert"] = get_alert_level("roic", result["roic"])
    result["rsi_alert"] = get_alert_level("rsi", result["rsi_14"])
    result["beta_alert"] = get_alert_level("beta", result["beta_90d"])

    # Bubble Risk Index
    result["bubble_risk"] = calculate_bubble_risk(result)

    return result


def fetch_all_mag7() -> Dict[str, Any]:
    """
    Busca dados de todas as Magnificent 7 de uma vez.
    Tenta dados reais via yfinance POR TICKER; se um falhar, só aquele usa demo.

    Retorna um dicionário com:
    - stocks: lista com dados de cada ação
    - market_summary: resumo do mercado (RSI médio, etc.)
    - thresholds: limiares de alerta para o frontend
    - updated_at: timestamp da última atualização
    - is_demo: True somente se TODOS os tickers falharam
    """
    logger.info("Iniciando coleta de dados das Magnificent 7...")

    can_use_yfinance = _check_connectivity()

    # Busca S&P 500 para cálculo de Beta (uma vez só)
    sp500_prices = pd.Series()
    if can_use_yfinance:
        sp500_prices = fetch_sp500_prices()
        logger.info("yfinance disponível — tentando dados reais por ticker")
    else:
        logger.info("yfinance não instalado — usando dados demo")

    stocks = []
    errors = []
    live_count = 0

    for ticker_symbol in MAG7_TICKERS:
        stock_data = None

        # Estratégia: yfinance → Yahoo API direta → demo
        if can_use_yfinance:
            # Tentativa 1: yfinance
            try:
                logger.info(f"[yfinance] Buscando {ticker_symbol}...")
                stock_data = fetch_single_stock(ticker_symbol, sp500_prices)
                if stock_data and stock_data.get("current_price") is not None:
                    live_count += 1
                    logger.info(f"  ✓ {ticker_symbol}: ${stock_data['current_price']} (yfinance)")
                else:
                    stock_data = None
            except Exception as e:
                logger.warning(f"  ✗ yfinance {ticker_symbol}: {e}")
                stock_data = None

            # Tentativa 2: Yahoo API direta (se yfinance falhou)
            if stock_data is None:
                try:
                    logger.info(f"[Yahoo API] Tentando {ticker_symbol}...")
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
                    stock_data = None

        # Fallback para demo se real falhou
        if stock_data is None:
            try:
                stock_data = _generate_demo_stock(ticker_symbol)
            except Exception as e2:
                errors.append({"ticker": ticker_symbol, "error": str(e2)})
                continue

        stocks.append(stock_data)

    # Calcula resumo do mercado
    rsi_values = [s["rsi_14"] for s in stocks if s.get("rsi_14") is not None]
    avg_rsi = round(np.mean(rsi_values), 2) if rsi_values else None
    all_overbought = all(r > 70 for r in rsi_values) if rsi_values else False

    # Bubble Risk agregado (média ponderada por market cap + prêmio de convergência)
    #
    # Educacional: O prêmio de convergência resolve o problema do "média mascara risco".
    # Se 5 de 7 ações estão acima de 35, o risco sistêmico é MAIOR que a média sugere,
    # porque a diversificação dentro do grupo não protege — tudo cairia junto.
    # Pensando como portfolio manager: correlação alta + valuation alto = risco não-linear.
    bubble_scores = [(s.get("bubble_risk", {}).get("score"), s.get("market_cap", 0) or 0)
                     for s in stocks if s.get("bubble_risk", {}).get("score") is not None]
    if bubble_scores:
        total_cap = sum(cap for _, cap in bubble_scores)
        if total_cap > 0:
            base_avg = sum(score * cap / total_cap for score, cap in bubble_scores)
        else:
            base_avg = np.mean([s for s, _ in bubble_scores])

        # Prêmio de convergência:
        # Conta quantas ações estão acima de 35 (risco não-trivial)
        elevated_count = sum(1 for score, _ in bubble_scores if score >= 35)
        total_count = len(bubble_scores)

        # Se mais de 50% estão elevadas, adiciona prêmio proporcional
        # Máximo +15 pontos quando TODAS estão elevadas
        if elevated_count > total_count * 0.5:
            convergence_ratio = elevated_count / total_count
            convergence_premium = convergence_ratio * 15  # até +15 pontos
        else:
            convergence_premium = 0

        # Também adiciona prêmio se a dispersão é baixa (todas parecidas = risco sistêmico)
        # Desvio padrão baixo dos scores = estão todas no mesmo patamar
        score_values = [s for s, _ in bubble_scores]
        std_dev = np.std(score_values)
        if std_dev < 10 and base_avg > 30:
            # Dispersão muito baixa + média elevada = risco concentrado
            concentration_premium = (10 - std_dev) / 10 * 5  # até +5 pontos
        else:
            concentration_premium = 0

        avg_bubble = round(min(base_avg + convergence_premium + concentration_premium, 100), 1)

        # Detalha os prêmios para transparência no frontend
        convergence_detail = {
            "base_score": round(base_avg, 1),
            "convergence_premium": round(convergence_premium, 1),
            "concentration_premium": round(concentration_premium, 1),
            "elevated_count": elevated_count,
            "total_count": total_count,
        }
    else:
        avg_bubble = None
        convergence_detail = None

    market_summary = {
        "avg_rsi": avg_rsi,
        "all_overbought": all_overbought,
        "total_market_cap": sum(s.get("market_cap", 0) or 0 for s in stocks),
        "stocks_count": len(stocks),
        "errors_count": len(errors),
        "bubble_risk_index": avg_bubble,
        "convergence_detail": convergence_detail,
    }

    # is_demo = True somente se NENHUM ticker retornou dados reais
    all_demo = live_count == 0

    return {
        "stocks": stocks,
        "market_summary": market_summary,
        "thresholds": ALERT_THRESHOLDS,
        "errors": errors,
        "updated_at": datetime.now().isoformat(),
        "is_demo": all_demo,
        "live_count": live_count,
        "total_count": len(MAG7_TICKERS),
    }
