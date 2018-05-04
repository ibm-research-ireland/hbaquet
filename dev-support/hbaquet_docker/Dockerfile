# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM ubuntu:16.04

RUN apt-get update && apt-get install git -y
RUN apt-get install openjdk-8-jdk maven -y
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

RUN git clone https://github.com/ibm-research-ireland/hbaquet.git
RUN mvn clean install -DskipTests assembly:single -f ./hbaquet/pom.xml

RUN apt-get install wget -y
RUN wget http://www.mirrorservice.org/sites/ftp.apache.org/spark/spark-2.3.0/spark-2.3.0-bin-hadoop2.7.tgz
RUN mkdir -p /root/spark-bin
RUN tar xzf spark-2.3.0-bin-hadoop2.7.tgz --strip-components 1 -C /root/spark-bin

RUN cd hbaquet && git pull
RUN mvn clean install -DskipTests assembly:single -f ./hbaquet/pom.xml
RUN mkdir -p /root/hbaquet-bin
RUN tar xzf /hbaquet/hbase-assembly/target/*tar.gz --strip-components 1 -C /root/hbaquet-bin

# Set HBASE_HOME, add it to the path, and start HBase.
ENV HBASE_HOME /root/hbaquet-bin
ENV PATH /root/hbaquet-bin/bin:/root/spark-bin/bin:/root/spark-bin/sbin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ADD start-spark.sh /bin
ADD create_populate_table.sh /bin
RUN echo 'var data = spark.read.load("/tmp/hbase-root/hbase/data/default/test/*.parquet")' >> /root/.scala_history
RUN rm spark-2.3.0-bin-hadoop2.7.tgz
