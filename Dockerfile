FROM python:latest

COPY ./ /opt

WORKDIR /opt
RUN apt-get update && apt-get install -y tzdata
ENV TZ=Asia/Ho_Chi_Minh
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN pip3 install -r requirements.txt

ENTRYPOINT ["python", "/opt/main.py"]