FROM centos:7.9.2009
MAINTAINER chenguang<cheng@ciotc.org>
#Yum安装git和envsubst等命令
RUN yum -y install git gettext wget ntpdate net-tools unzip
#安装Docker
RUN yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
RUN yum -y install docker-ce-cli
#安装Kubectl
ADD kubernetes.repo /etc/yum.repos.d/kubernetes.repo
RUN yum -y install kubectl
#安装Java
WORKDIR /usr/app/java
ADD http://192.168.1.152:9999/repository/PACKAGE/java/jdk-8u341-linux-x64.tar.gz ./
RUN tar -zxvf jdk-8u341-linux-x64.tar.gz
RUN rm -rf jdk-8u341-linux-x64.tar.gz
ENV JAVA_HOME=/usr/app/java/jdk1.8.0_341
#安装Maven
WORKDIR /usr/app/maven
ADD http://192.168.1.152:9999/repository/PACKAGE/maven/apache-maven-3.6.3-bin.tar.gz ./
RUN tar -zxvf apache-maven-3.6.3-bin.tar.gz
RUN rm -rf apache-maven-3.6.3-bin.tar.gz
COPY ./settings.xml ./apache-maven-3.6.3/conf/
ENV MAVEN_HOME=/usr/app/maven/apache-maven-3.6.3
#安装NodeJS
WORKDIR /usr/app/nodejs
ADD http://192.168.1.152:9999/repository/PACKAGE/nodejs/node-v16.15.0-linux-x64.tar.gz ./
RUN tar -zxvf node-v16.15.0-linux-x64.tar.gz
RUN rm -rf node-v16.15.0-linux-x64.tar.gz
ADD ./npmrc ./node-v16.15.0-linux-x64/lib/node_modules/npm/npmrc
ENV NODEJS_HOME=/usr/app/nodejs/node-v16.15.0-linux-x64
#安装K8S部署模板
COPY ./template /usr/app/template
#定义工作目录
WORKDIR /root
RUN rm -f anaconda-ks.cfg
ENV PATH $PATH:$JAVA_HOME/bin:$MAVEN_HOME/bin:$NODEJS_HOME/bin
#启动执行的命令
ADD .toprc /root/.toprc
CMD top -b
