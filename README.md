# Magnificent 7 — Dashboard de Monitoramento

Dashboard interativo para monitoramento das "Magnificent 7" (AAPL, MSFT, GOOGL, AMZN, NVDA, META, TSLA) com indicadores de valuation, risco e sentimento, além de um índice proprietário de Risco de Bolha.

## Funcionalidades

- **Cards individuais** com métricas-chave e alertas visuais por ação
- **Tabela comparativa** ordenável com todos os indicadores
- **Gráficos** de valuation, radar, market cap e eficiência operacional
- **Performance** histórica comparativa (1m a 5y)
- **Índice de Risco de Bolha** — score composto 0-100 com gauge visual e prêmio de convergência

## Stack

- **Backend:** Python 3.11 + FastAPI + yfinance
- **Frontend:** Vanilla HTML/CSS/JS + Chart.js 4.4.1
- **Deploy:** Docker + Render (free tier)

## Rodar Localmente

```bash
pip install -r requirements.txt
python main.py
# Abra http://localhost:8000
```

## Deploy no Render

1. Faça push deste repo para o GitHub
2. No [Render Dashboard](https://dashboard.render.com), clique **New > Web Service**
3. Conecte seu repositório GitHub
4. Render detecta o `Dockerfile` automaticamente
5. Clique **Deploy** — pronto!

## Variáveis de Ambiente (opcionais)

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `PORT` | `8000` | Porta do servidor (Render define automaticamente) |
| `CACHE_TTL` | `300` | Tempo de cache em segundos (5 min) |

## Estrutura

```
mag7-dashboard/
├── main.py            # Servidor FastAPI (API + serve frontend)
├── data_fetcher.py    # Lógica de dados (yfinance + cálculos + demo mode)
├── frontend/
│   └── index.html     # Dashboard completo (HTML + CSS + JS)
├── requirements.txt
├── Dockerfile
└── README.md
```
