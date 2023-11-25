FROM apache/airflow:2.7.0
FROM python:3.8

COPY requirements/requirements.txt /requirements/requirements.txt

RUN pip install -r /requirements/requirements.txt

RUN pip3 install -U spacy

RUN python3 -m spacy download ru_core_news_sm && \
    python3 -m spacy download ru_core_news_md && \
    python3 -m spacy download ru_core_news_lg && \
    python3 -m spacy download en_core_web_sm && \
    python3 -m spacy download en_core_web_md && \
    python3 -m spacy download en_core_web_lg && \
    python3 -m spacy download en_core_web_trf

