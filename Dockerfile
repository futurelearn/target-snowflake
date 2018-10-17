FROM python:3.6.6

WORKDIR /target-snowflake

COPY requirements.txt ./
RUN pip install -U pip
RUN pip install -r requirements.txt

COPY . ./

RUN pip install -e .
