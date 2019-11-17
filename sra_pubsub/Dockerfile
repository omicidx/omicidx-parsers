FROM python:3.7

RUN pip install poetry
RUN git clone https://github.com/seandavi/omicidx
RUN cd omicidx
RUN poetry install

ENTRYPOINT ['python', 'run', 'master.py']
