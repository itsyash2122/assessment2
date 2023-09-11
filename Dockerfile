FROM python:3.10-slim
WORKDIR court_record_check

COPY . .

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc build-essential

RUN pip install -r requirements.txt

EXPOSE 8000
ENTRYPOINT ["sh", "-c"]

CMD ["exec uvicorn process_cases:app --host 0.0.0.0 --port 8000 --reload"]