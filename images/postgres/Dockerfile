FROM postgres:16.1

# the trailing slash on ./scripts/ copies the contents but not scripts/ itself
COPY ./scripts/ /docker-entrypoint-initdb.d/

RUN mkdir -p /usr/src/app
COPY ./data/station-data.csv /usr/src/app
