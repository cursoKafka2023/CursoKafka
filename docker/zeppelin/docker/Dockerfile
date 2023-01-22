FROM ubuntu:20.04

RUN apt-get update && apt-get -y upgrade

RUN apt-get install -y openjdk-8-jdk

ENV JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64/jre/"

RUN apt install -y maven

RUN apt install -y python3

RUN apt install -y pip

RUN pip install jupyter
RUN pip install grpcio
RUN pip install protobuf
RUN pip install pandas
RUN pip install matplotlib
RUN pip install kafka-python 


#RUN apt install -y scala

RUN apt install -y wget

RUN apt-get install -y git

RUN wget https://archive.apache.org/dist/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz
RUN tar xvf spark-*
RUN mv spark-2.4.0-bin-hadoop2.7 /opt/spark

ENV SPARK_HOME="/opt/spark/"
ENV PYSPARK_PYTHON="/usr/bin/python3"

RUN wget https://dlcdn.apache.org/zeppelin/zeppelin-0.10.1/zeppelin-0.10.1-bin-all.tgz

RUN tar xvf zeppelin-0.10.1-bin-all.tgz
RUN mv zeppelin-0.10.1-bin-all zeppelin/


RUN wget https://raw.githubusercontent.com/LuisRomera/zeppe/main/conf/shiro.ini
RUN mv shiro.ini /zeppelin/conf/shiro.ini

RUN wget https://raw.githubusercontent.com/LuisRomera/zeppe/main/conf/zeppelin-site.xml
RUN mv zeppelin-site.xml /zeppelin/conf/zeppelin-site.xml

RUN wget https://raw.githubusercontent.com/LuisRomera/zeppe/main/interpreter/python/interpreter-setting.json
RUN mv interpreter-setting.json /zeppelin/interpreter/python/interpreter-setting.json


RUN wget https://raw.githubusercontent.com/LuisRomera/zeppe/main/notebook/java_2HNRCXEXV.zpln
RUN mv java_2HNRCXEXV.zpln /zeppelin/notebook/java_2HNRCXEXV.zpln

RUN wget https://raw.githubusercontent.com/LuisRomera/zeppe/main/conf/interpreter.json
RUN mv interpreter.json /zeppelin/conf/interpreter.json


ENV PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python


WORKDIR zeppelin/bin
CMD ["./zeppelin.sh"]

#RUN useradd -rm -d /home/ubuntu -s /bin/bash -g root -G sudo -u 1001 ubuntu
#USER ubuntu
#WORKDIR /home/ubuntu
#
#RUN git clone https://github.com/LuisRomera/zeppelin
#
#
#WORKDIR /home/ubuntu/zeppelin
#RUN mvn clean install -DskipTests
#RUN ./bin/zeppelin-daemon.sh start




EXPOSE 8080
