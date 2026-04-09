"""
main.py — Servidor FastAPI do Dashboard Magnificent 7.

Endpoints:
  GET /              → Serve o frontend (dashboard completo)
  GET /api/dashboard → Dados completos de todas as M7
  GET /api/stock/{ticker} → Dados de uma ação específica
  GET /api/history/{ticker}?period=1y → Histórico de preços
  GET /api/tickers   → Lista de tickers
  GET /api/cache/clear → Limpa cache
  GET /health        → Health check (usado pelo Render)

Para rodar localmente:
  pip install -r requirements.txt
  python main.py
  Abra http://localhost:8000

Para deploy no Render:
  - Push para GitHub
  - Conecte o repo ao Render
  - Ele detecta o Dockerfile automaticamente
"""

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import logging
import os
import time

from data_fetcher import fetch_all_mag7, fetch_single_stock, fetch_price_history, fetch_sp500_prices, MAG7_TICKERS

# Configura logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Magnificent 7 Dashboard API",
    description="API para monitoramento das Magnificent 7 stocks",
    version="1.0.0",
)

# CORS — permite que o frontend se comunique com a API
# Educacional: CORS (Cross-Origin Resource Sharing) é um mecanismo de segurança
# dos browsers. Em produção, o ideal seria restringir allow_origins ao seu domínio.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Cache simples em memória para não bombardear o Yahoo Finance
# Educacional: Em produção com múltiplas instâncias, usaríamos Redis.
# Com uma instância no Render free tier, um dict em memória funciona perfeitamente.
_cache = {}
CACHE_TTL = int(os.environ.get("CACHE_TTL", 300))  # 5 min padrão, configurável via env var


def get_cached(key: str):
    """Retorna dados do cache se ainda válidos."""
    if key in _cache:
        data, timestamp = _cache[key]
        if time.time() - timestamp < CACHE_TTL:
            return data
    return None


def set_cached(key: str, data):
    """Armazena dados no cache."""
    _cache[key] = (data, time.time())


# ==================== API ENDPOINTS ====================

@app.get("/api/dashboard")
async def get_dashboard():
    """
    Retorna dados completos de todas as Magnificent 7.
    Usa cache de 5 minutos para evitar rate limiting do Yahoo Finance.
    """
    cached = get_cached("dashboard")
    if cached:
        logger.info("Retornando dados do cache")
        return cached

    try:
        data = fetch_all_mag7()
        set_cached("dashboard", data)
        return data
    except Exception as e:
        logger.error(f"Erro ao buscar dashboard: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stock/{ticker}")
async def get_stock(ticker: str):
    """Retorna dados de uma ação específica."""
    ticker = ticker.upper()
    if ticker not in MAG7_TICKERS:
        raise HTTPException(status_code=404, detail=f"Ticker {ticker} não faz parte das Magnificent 7")

    cache_key = f"stock_{ticker}"
    cached = get_cached(cache_key)
    if cached:
        return cached

    try:
        sp500_prices = fetch_sp500_prices()
        data = fetch_single_stock(ticker, sp500_prices)
        set_cached(cache_key, data)
        return data
    except Exception as e:
        logger.error(f"Erro ao buscar {ticker}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/history/{ticker}")
async def get_history(ticker: str, period: str = "1y"):
    """
    Retorna histórico de preços para gráficos.
    Períodos válidos: 1mo, 3mo, 6mo, 1y, 2y, 5y
    """
    ticker = ticker.upper()
    valid_periods = ["1mo", "3mo", "6mo", "1y", "2y", "5y"]

    if period not in valid_periods:
        raise HTTPException(status_code=400, detail=f"Período inválido. Use: {valid_periods}")

    cache_key = f"history_{ticker}_{period}"
    cached = get_cached(cache_key)
    if cached:
        return cached

    try:
        data = fetch_price_history(ticker, period)
        set_cached(cache_key, data)
        return data
    except Exception as e:
        logger.error(f"Erro ao buscar histórico de {ticker}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/tickers")
async def get_tickers():
    """Retorna a lista de tickers das Magnificent 7."""
    return MAG7_TICKERS


@app.get("/api/cache/clear")
async def clear_cache():
    """Limpa o cache para forçar refresh dos dados."""
    _cache.clear()
    return {"message": "Cache limpo com sucesso"}


# ==================== HEALTH CHECK ====================
# Educacional: O Render (e outros PaaS) pinga este endpoint para saber se
# o serviço está vivo. Se retornar erro, o Render reinicia o container.

@app.get("/health")
async def health_check():
    """Health check para o Render e monitoramento."""
    return {
        "status": "healthy",
        "cache_entries": len(_cache),
        "cache_ttl": CACHE_TTL,
    }


# ==================== FRONTEND ====================

@app.get("/")
async def serve_frontend():
    """Serve o arquivo HTML do frontend."""
    frontend_path = os.path.join(os.path.dirname(__file__), "frontend", "index.html")
    if os.path.exists(frontend_path):
        return FileResponse(frontend_path)
    return JSONResponse(
        status_code=404,
        content={"message": "Frontend não encontrado. Verifique se frontend/index.html existe."}
    )


# Serve arquivos estáticos (CSS, JS, imagens)
frontend_dir = os.path.join(os.path.dirname(__file__), "frontend")
if os.path.exists(frontend_dir):
    app.mount("/static", StaticFiles(directory=frontend_dir), name="static")


if __name__ == "__main__":
    # Educacional: O Render define a variável PORT automaticamente.
    # Localmente, se PORT não existir, usa 8000.
    port = int(os.environ.get("PORT", 8000))
    print("\n" + "=" * 60)
    print("  MAGNIFICENT 7 DASHBOARD")
    print(f"  Abra http://localhost:{port} no seu browser")
    print("=" * 60 + "\n")
    uvicorn.run(app, host="0.0.0.0", port=port)
