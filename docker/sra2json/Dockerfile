# sra2json
FROM python:3.6

WORKDIR /tmp

RUN git clone https://github.com/seandavi/omicidx

WORKDIR /tmp/omicidx
RUN git pull origin master

RUN pip install .

RUN mkdir /data

WORKDIR /data
RUN rm -rf /tmp/omicidx

COPY sra_directory_to_s3.sh .
RUN chmod +x sra_directory_to_s3.sh

RUN pip install awscli
