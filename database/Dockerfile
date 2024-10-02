FROM python:3.11-slim

RUN apt-get update && apt-get install -y sqlite3

WORKDIR /database

COPY . .

CMD ["python", "data_handler.py"]

EXPOSE 8040