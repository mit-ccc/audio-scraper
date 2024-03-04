FROM pytorch/pytorch:2.2.1-cuda12.1-cudnn8-runtime

ARG AWS_CONTAINER_CREDENTIALS_RELATIVE_URI
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y apt-utils
RUN apt-get install -y build-essential unixodbc unixodbc-dev odbc-postgresql \
                       jq ca-certificates libxml2 libxml2-dev \
                       libsndfile1 ffmpeg
RUN apt-get clean

WORKDIR /usr/src/app
COPY requirements.txt .
RUN python3.10 -m pip install -U pip && python3.10 -m pip install -r requirements.txt
COPY . .

RUN chmod ug+x entrypoint.sh run.py

ENTRYPOINT ["./entrypoint.sh"]
CMD ["./run.py"]
