FROM python:3

COPY requirements.txt .
RUN pip install -r requirements.txt

ENV PYTHONUNBUFFERED=1

WORKDIR /usr/src/app

COPY producer.py .
COPY record.avsc .

CMD [ "python", "/usr/src/app/producer.py" ]