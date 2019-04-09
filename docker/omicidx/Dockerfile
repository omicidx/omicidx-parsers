# sra2json
FROM python:3.7
RUN apt-get update
RUN apt-get install -y wget

WORKDIR /tmp

RUN git clone https://github.com/seandavi/omicidx.git

WORKDIR /tmp/omicidx
RUN git pull origin master

RUN pip install .

RUN mkdir /data
WORKDIR /data
RUN rm -rf /tmp/omicidx

RUN pip install virtualenv
RUN virtualenv -p python2 py2
RUN /bin/bash -c "source py2/bin/activate && pip install gsutil && deactivate"
RUN pip install awscli google

RUN echo "alias gsutil='/data/py2/bin/gsutil'" > /root/.bashrc

CMD ["/bin/bash"]
