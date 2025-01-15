FROM java:8u111-alpine
MAINTAINER chenguang<cheng@ciotc.org>
#定义工作目录
ENV APP_PATH /usr/app
#部署应用
WORKDIR $APP_PATH
ADD ./target/proxy-1.1.0.jar $APP_PATH
ENTRYPOINT ["java","-jar","proxy-1.1.0.jar"]
#暴露应用端口
EXPOSE 443
#启动执行的命令
CMD /bin/bash
