FROM nvidia/cuda:12.3.2-runtime-ubuntu22.04

ARG AWS_CONTAINER_CREDENTIALS_RELATIVE_URI
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y apt-utils
RUN apt-get install -y build-essential unixodbc unixodbc-dev odbc-postgresql \
                       jq ca-certificates libxml2 libxml2-dev \
                       libsndfile1 ffmpeg \
                       python3 python3-dev python3-pip python3.10
RUN apt-get clean

WORKDIR /usr/src/app
COPY requirements.txt /usr/src/app
RUN pip3 install -U pip && pip3 install -r requirements.txt
COPY . /usr/src/app

RUN chmod ug+x entrypoint.sh
RUN chmod ug+x run.py

ENTRYPOINT ["./entrypoint.sh"]
CMD ["./run.py"]
