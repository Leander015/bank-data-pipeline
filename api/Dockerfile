# Imagem da API: Python enxuto + só as bibliotecas que a API usa
FROM python:3.12-slim

WORKDIR /app

# Primeiro só o requirements: assim o Docker reaproveita essa camada (cache)
# e não reinstala tudo quando apenas o código muda
COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY api/ ./api/

EXPOSE 8000
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
