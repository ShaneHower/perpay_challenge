# There is an issue with python 3.13 and psycopg2, see here - https://github.com/actions/setup-python/issues/864
FROM python:3.10

ENV APP_HOME="/usr/src/app"
WORKDIR $APP_HOME

# Set up app files
COPY requirements.txt ./
COPY data/. "$APP_HOME/data"
COPY scripts/. "$APP_HOME/scripts"
RUN mkdir ./logs

RUN pip install -r requirements.txt
