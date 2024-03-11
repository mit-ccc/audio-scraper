FROM postgres:16.1

ARG AWS_CONTAINER_CREDENTIALS_RELATIVE_URI
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y apt-utils
RUN apt-get install -y unixodbc unixodbc-dev odbc-postgresql ca-certificates \
                       python3 python3.10 python3-boto3 python3-psycopg2
RUN apt-get clean

# the trailing slash on ./scripts/ copies the contents but not scripts/ itself
COPY ./scripts/ /docker-entrypoint-initdb.d/

RUN mkdir -p /usr/src/app
COPY ./data/source-data.csv /usr/src/app

USER postgres
