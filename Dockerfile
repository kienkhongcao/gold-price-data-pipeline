FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chmod +x run_pipeline.sh

CMD ["bash", "run_pipeline.sh"]