# syntax=docker/dockerfile:1

FROM python
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
RUN touch updateTime
COPY read-adsb.py .
CMD [ "python3", "./read-adsb.py"]
