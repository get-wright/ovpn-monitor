FROM python:latest

COPY ./ /opt

WORKDIR /opt

RUN pip3 install -r requirements.txt

ENTRYPOINT ["python", "/opt/app.py"]