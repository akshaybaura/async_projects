FROM python:3.10-slim-bullseye

ENV TDIR /home/app

RUN mkdir -p $TDIR
COPY ./async_pull.py ./patient_producer.py ./schema ./requirements.txt $TDIR/
RUN cd $TDIR/ && pip install -r requirements.txt
RUN apt-get update && apt-get install -y vim

CMD /bin/bash