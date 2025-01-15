#!/bin/sh
MySQLHost=$1;
MySQLPort=$2;
MySQLDB=$3;
MYSQLTables=$4;
MySQLUser=$5;
MySQLPasswd=$6;
HadoopHost=$7;
HadoopPort=$8;
HadoopPath=$9;
HadoopDB=$10;
NameSpace=${11};
TaskName=${12};
#首先启动Flink集群
echo "启动Flink集群：StandaloneSession";
bin/start-cluster.sh;
sleep 3;
echo -------------------------------------------------;
#提交Job作业到集群
echo "提交数据作业：(${MySQLUser}@${MySQLHost}/${MySQLDB}:${MySQLPort})->(hdfs://${HadoopHost}:${HadoopPort}/${HadoopPath}/${HadoopDB})";
b="%20";
param="entry-class=org.apache.paimon.flink.action.FlinkActions";
param=${param}"&program-args=mysql-sync-database";
param=${param}"${b}--warehouse${b}hdfs://${HadoopHost}:${HadoopPort}/${HadoopPath}";
param=${param}"${b}--database${b}${HadoopDB}";
#param=${param}"${b}--table--prefix${b}ods_";
if [ "${MYSQLTables}" != "ALL" ] ; then
  param=${param}"${b}--including-tables${b}'${MYSQLTables}'";
fi
param=${param}"${b}--mysql-conf${b}hostname=${MySQLHost}";
param=${param}"${b}--mysql-conf${b}port=${MySQLPort}";
param=${param}"${b}--mysql-conf${b}database-name=${MySQLDB}";
param=${param}"${b}--mysql-conf${b}username=${MySQLUser}";
param=${param}"${b}--mysql-conf${b}password=${MySQLPasswd}";
param=${param}"${b}--table-conf${b}sink.parallelism=1";
requrl="http://localhost:8081/jars/paimon.jar/run?${param}";
echo ${requrl};
code=$(curl -s -w %{http_code} -X POST -o /dev/null ${requrl});
echo "提交作业返回：${code}";
if [ "${code}" != "200" ] ; then
  echo "提交作业失败，任务将处于NoLastTask状态，直到第一个Fink作业提交！";
fi
echo -------------------------------------------------;
while true
do
  bin/get-status.sh ${NameSpace} ${TaskName};
  echo "";
  JobStatus=$(cat JobStatus.current);
  if [ "${JobStatus}" = "Running" ] ; then
    id=$(cat JobID.current);
    code=$(curl -s -w %{http_code} -o chk.json http://localhost:8081/jobs/${id}/checkpoints);
    counts=$(cat chk.json | tr '\n' ' ' |  grep -Po '"counts":{(.+?)}');
    completed=$(echo $counts |  grep -Po '"completed":(.*?),');
    completed=${completed#*\"completed\":};
    completed=${completed%%,};
    if [ ${completed} = 0 ] ; then
      echo -------------------------------------------------;
      sleep 3;
      bin/flink savepoint ${id}
      echo -------------------------------------------------;
    fi
  fi
  if [ "${JobStatus}" = "Terminating" ] ; then
    echo "终止作业集群：";
    bin/stop-cluster.sh;
    break;
  fi
  sleep 6;
done
