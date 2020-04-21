#!/bin/bash
# user.txt的格式为：IP:user:password

FILENAME=user.txt
LINUX_EXPORTER=node_exporter-0.18.1.linux-amd64.tar.gz
WINDOW_EXPORTER=wmi_exporter-0.7.999-preview.2-amd64.exe
MYSQL_EXPORTER=mysqld_exporter-0.12.1.linux-amd64.tar.gz
ORACLE_EXPORTER=oracledb_exporter.0.2.2.linux-amd64.tar.gz
SNMP_EXPORTER=snmp_exporter-0.15.0.linux-amd64.tar.gz
KAFKA_ADAPTER=prometheus-kafka-adapter

function usage {
    echo "Usage: `basename $0` the number of paramets more than one"
    exit 1
}

function whether_port_use(){
  port=$1
  info=`lsof -i:$port`
  if [ ! -z "$info" ];then
     echo "The port: $port is using"
     exit 1
  else
     echo "The port: $port is not used,and you can use it!"
  fi
}

function send_tar_target(){
  cat $FILENAME | while read LINE
  do
  str=$LINE
  OLD_IFS="$IFS"
  IFS=":"
  arr=($str)
  IFS="$OLD_IFS"
  IP=${arr[0]}
  USER_ADD=${arr[1]}
  PASSWORD=${[arr[2]]}
/usr/bin/expect <<-EOF
  spawn scp ./$service  $USER_ADD@$IP:/home/$USER_ADD
  expect {
    "*yes/no)?*" { send "yes\r";exp_continue}
    "*ssword:*" { send "$PASSWORD\r" }
    }
  expect eof
EOF
  done
}
function linux_install(){
  local service=$1
  send_tar_target $service
  cat $FILENAME | while read LINE
  do
  str=$LINE
  OLD_IFS="$IFS"
  IFS=":"
  arr=($str)
  IFS="$OLD_IFS"
  IP=${arr[0]}
  USER_ADD=${arr[1]}
  PASSWORD=${[arr[2]]}
/usr/bin/expect <<-EOF
  spawn ssh $USER_ADD@$IP
  expect {
    "*yes/no)?*" { send "yes\r";exp_continue}
    "*ssword:*" { send "$PASSWORD\r" }
    }
  expect "*$\*" { send "cd /home/$USER_ADD\r" }
  expect "*$\*" { send "tar -zxvf /home/$USER_ADD/$service\r" }
  expect "*$\*" { send "nohup ./node_exporter-0.18.1.linux-amd64/node_exporter &\r"}
  expect eof
EOF
  done
}

#mysq安装需要进入mysql中对用户赋权
function mysql_install(){
  local service=$1
  echo $service
  send_tar_target $service
  cat $FILENAME | while read LINE
  do
  str=$LINE
  OLD_IFS="$IFS"
  IFS=":"
  arr=($str)
  IFS="$OLD_IFS"
  IP=${arr[0]}
   USER_ADD=${arr[1]}
  PASSWORD=${[arr[2]]}
/usr/bin/expect <<-EOF
  spawn ssh $USER_ADD@$IP
  expect {
    "*yes/no)?*" { send "yes\r";exp_continue}
    "*ssword:*" { send "$PASSWORD\r" }
    }
  expect "*$\*" { send "cd /home/$USER_ADD\r" }
  expect "*$\*" { send "tar -zxvf /home/$USER_ADD/$service\r" }
  expect "*$\*" { send "cd /home/$USER_ADD/mysqld_exporter-0.12.1.linux-amd64\r" }
  expect "*$\*" { send "echo \[client\] >> my.cnf\r" }
  expect "*$\*" { send "echo host=$IP >> my.cnf\r" }
  expect "*$\*" { send "echo user=$USER_ADD >> my.cnf\r" }
  expect "*$\*" { send "echo password=$PASSWORD >> my.cnf\r" }
  expect "*$\*" { send "nohup ./mysqld_exporter --config.my-cnf=./my.cnf &\r" }
  expect eof
EOF
  done

}

function kafka_install(){
  local service=$1
  send_tar_target $service
  cat $FILENAME | while read LINE
  do
  str=$LINE
  OLD_IFS="$IFS"
  IFS=":"
  arr=($str)
  IFS="$OLD_IFS"
  IP=${arr[0]}
/usr/bin/expect <<-EOF
  spawn ssh $USER_ADD@$IP
  expect {
    "*yes/no)?*" { send "yes\r";exp_continue}
    "*ssword:*" { send "$PASSWORD\r" }
    }
  expect "*$\*" { send "cd /home/$USER_ADD\r" }
  expect "*$\*" { send "nohup sh ./KAFKA_ADAPTER &\r" }
  expect eof
EOF
  done
}
function install_consul(){
  unzip consul_1.0.0_linux_amd64.zip
  ./consul agent -server -ui -bootstrap-expect 1 -data-dir /tmp/consul &

}
function install_prometheus(){
  tar -zxvf prometheus-2.11.1.darwin-amd64.tar.gz
  ln -s prometheus-2.11.1.darwin-amd64 prometheus
  cd prometheus
  nohup ./prometheus &

}

function main(){
  # init
    if [ $# -lt 1 ];then
      usage
    paramets=$1
    if [ "$paramets"="linux" ];then
      echo "start linux ... "
      service=$LINUX_EXPORTER
      whether_port_use 9100
      linux_install $service
    #windows无法使用脚本安装。
    elif [ "$paramets"="windows" ]; then
      echo "start windows ... "
      service=$WINDOW_EXPORTER
    elif [ "$paramets"="mysql" ]; then
      echo "start mysql ... "
      service=$MYSQL_EXPORTER
      whether_port_use 9104
      mysql_install $service
    elif [ "$paramets"="oracle" ]; then
      echo "start oracle ... "
      service=$ORACLE_EXPORTER
    elif [ "$paramets"="kafka" ]; then
      echo "start kafka ... "
      service=$KAFKA_ADAPTER
      kafka_install $service
    elif [ "$paramets"="consul" ]; then
        echo "start consul ..."
      install_consul
      #statements
    elif [ "$paramets"="prometheus" ];then
        echo "start prometheus ..."
        install_prometheus
    else
      echo "$paramets is not match any Options"
    fi
}
main $@
