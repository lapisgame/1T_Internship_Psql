FROM apache/airflow:2.7.0

COPY requirements/requirements.txt /requirements/requirements.txt

RUN pip install -r /requirements/requirements.txt

RUN pip3 install -U spacy

RUN python3 -m spacy download ru_core_news_sm
RUN python3 -m spacy download ru_core_news_md
RUN python3 -m spacy download ru_core_news_lg
RUN python3 -m spacy download en_core_web_sm
RUN python3 -m spacy download en_core_web_md
RUN python3 -m spacy download en_core_web_lg
RUN python3 -m spacy download en_core_web_trf

