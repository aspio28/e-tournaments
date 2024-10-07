FROM python:3.11-slim

WORKDIR /minion

COPY . .

RUN pip install dill

CMD ["python", "minion.py"]

EXPOSE 8020