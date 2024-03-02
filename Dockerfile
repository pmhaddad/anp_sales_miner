FROM ubuntu:22.04

ENV LANG=C.UTF-8

ENV LC_ALL=C.UTF-8

ENV TZ=UTC

ARG PIP_NO_CACHE_DIR=1

RUN apt-get update -y

RUN apt-get install python3-distutils python3 curl -y

RUN curl https://raw.githubusercontent.com/pypa/pipenv/master/get-pipenv.py | python3

WORKDIR /var/anp_sales_miner

COPY . .

RUN pipenv verify && pipenv sync --dev

EXPOSE 8080

ENTRYPOINT ["/var/anp_sales_miner/entrypoint.sh"]

CMD ["pipenv", "run", "airflow", "webserver"]
