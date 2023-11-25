FROM apache/airflow:2.7.0
FROM python:3.8

COPY requirements/requirements.txt .

RUN pip install -r requirements.txt

RUN python -m spacy download model --direct --sdist pip_args && \
    python -m spacy download ru_core_news_sm && \
    python -m spacy download ru_core_news_md && \
    python -m spacy download ru_core_news_lg && \
    python -m spacy download en_core_web_sm && \
    python -m spacy download en_core_web_md && \
    python -m spacy download en_core_web_lg && \
    python -m spacy download en_core_web_trf

