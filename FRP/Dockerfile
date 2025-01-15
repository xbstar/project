FROM centos:7.9.2009
MAINTAINER chenguang<cheng@ciotc.org>
#安装FRPS
WORKDIR /usr/app
ADD http://192.168.1.152:9999/repository/PACKAGE/frp/frp_0.51.2_linux_amd64.tar.gz ./
RUN tar -zxvf frp_0.51.2_linux_amd64.tar.gz
RUN rm -rf frp_0.51.2_linux_amd64.tar.gz
RUN mv frp_0.51.2_linux_amd64 frp
#定义工作目录
WORKDIR /usr/app/frp
#添加Path路径
ENV PATH $PATH:/usr/app/frp
#修改配置文件
ADD frps.ini ./frps.ini
ADD frpc.ini ./frpc.ini
ADD frp-server.sh ./frp-server.sh
ADD frp-client.sh ./frp-client.sh
#启动执行的命令
CMD frps -c frps.ini
EXPOSE 7000 7500 7600
