# Dockerfile para deploy no Render (ou qualquer PaaS com Docker)
#
# Educacional: Docker é um sistema de "containers" — ele empacota todo o
# ambiente (Python, bibliotecas, código) numa "caixa" isolada e reproduzível.
# O Render detecta este arquivo automaticamente e faz o build.

# Imagem base: Python 3.11 slim (leve, ~120MB vs ~900MB da versão full)
FROM python:3.11-slim

# Diretório de trabalho dentro do container
WORKDIR /app

# Copia requirements primeiro (Docker cacheia esta camada se não mudar)
# Educacional: Docker funciona em "camadas". Se requirements.txt não mudou,
# ele pula o pip install e usa o cache — economiza minutos no build.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o resto do código
COPY . .

# Variável de ambiente padrão (Render sobrescreve PORT automaticamente)
ENV PORT=8000

# Expõe a porta (documentação, não obrigatório no Render)
EXPOSE ${PORT}

# Comando de inicialização
# Educacional: uvicorn é o servidor ASGI que roda o FastAPI.
# --host 0.0.0.0 = aceita conexões de qualquer IP (necessário em container)
# --port $PORT = usa a porta que o Render define
# --workers 1 = free tier tem pouca RAM, 1 worker é suficiente
CMD uvicorn main:app --host 0.0.0.0 --port $PORT --workers 1
