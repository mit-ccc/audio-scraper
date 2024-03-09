FROM python:3.10-slim

ARG AWS_CONTAINER_CREDENTIALS_RELATIVE_URI
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y apt-utils
RUN apt-get install -y build-essential unixodbc unixodbc-dev odbc-postgresql \
                       jq ca-certificates libxml2 libxml2-dev wait-for-it \
                       ffmpeg libavcodec-extra libasound2-dev
RUN apt-get clean

WORKDIR /usr/src/app
COPY requirements.txt /usr/src/app
RUN pip3 install -U pip && pip3 install -r requirements.txt
COPY . /usr/src/app

RUN chmod ug+x entrypoint.sh run.py

ENTRYPOINT ["./entrypoint.sh"]
CMD ["./run.py"]
