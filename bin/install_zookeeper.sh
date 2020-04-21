#!/bin/bash

FILENAME=user.txt
USER_ADD=$1
PASSWORD=$USER_ADD
USER_PATH=/home/$USER_ADD
USER_INSTALL_PATH=$USER_PATH/install
USER_SOFT_PATH=$USER_PATH/software
PROFILE="$USER_PATH/.bash_profile"
ZOOKEEPER_NAME="zookeeper-3.4.7.tar.gz"
ZOOKEEPER=zookeeper-3.4.7
dtc_ln=`which ln`
dtc_sed=`which sed`
function Distribution(){
  if [ ！-f $FILENAME ] || [ -z $USER_ADD ];then
    echo 'filename is not exist or user is null'
    exit 1
  fi
  if [ -z $PASSWORD ];then
    PASSWORD=$USER_ADD
  fi
  cp ./$ZOOKEEPER_NAME $USER_INSTALL_PATH
  tar -zxvf $USER_INSTALL_PATH/$ZOOKEEPER_NAME -C $USER_SOFT_PATH
  $dtc_ln -s $USER_SOFT_PATH/$ZOOKEEPER $USER_SOFT_PATH/zookeeper
  mkdir -p $USER_SOFT_PATH/zookeeper/data
  mkdir -p $USER_SOFT_PATH/zookeeper/log
  chmod -R 755 $USER_SOFT_PATH/zookeeper/data
  chmod -R 755 $USER_SOFT_PATH/zookeeper/log
  cp $USER_SOFT_PATH/zookeeper/conf/zoo_sample.cfg $USER_SOFT_PATH/zookeeper/conf/zoo.cfg
  $dtc_sed -i "s#dataDir=\/tmp\/zookeeper#dataDir=$USER_SOFT_PATH\/zookeeper\/data#g" $USER_SOFT_PATH/zookeeper/conf/zoo.cfg
  echo "dataLogDir=$USER_SOFT_PATH/zookeeper/log" >>$USER_SOFT_PATH/zookeeper/conf/zoo.cfg
  sum=0
  cat $FILENAME | while read LINE
  do
  str=$LINE
  OLD_IFS="$IFS"
  IFS=":"
  arr=($str)
  IFS="$OLD_IFS"
  #为自定义变量赋值
  IP=${arr[0]}
  echo "server.$sum=$IP:2888:3888">> $USER_SOFT_PATH/zookeeper/conf/zoo.cfg
  let sum=$sum+1
  done
  #读取行内容
  cat $FILENAME | while read LINE
  do
  str=$LINE
  OLD_IFS="$IFS"
  IFS=":"
  arr=($str)
  IFS="$OLD_IFS"
  #为自定义变量赋值
  IP=${arr[0]}
/usr/bin/expect <<-EOF
  spawn scp -r $USER_SOFT_PATH/zookeeper $USER_ADD@$IP:$USER_SOFT_PATH
  expect {
    "*yes/no)?" {send "yes\r"
                   expect "*ssword:" {send "$PASSWORD\r"}
          }
      "*ssword:" {send "$PASSWORD\r"}
      "*:" {send "PASSWORD\r"}
    }
expect eof
EOF
  done
}
function Install_ZK(){
  cat $FILENAME | while read LINE
  do
  str=$LINE
  OLD_IFS="$IFS"
  IFS=":"
  arr=($str)
  IFS="$OLD_IFS"
  IP=${arr[0]}
/usr/bin/expect <<-EOF
  spawn ssh $IP
  expect "*$"
  send "echo export ZOOKEEPER_HOME=$USER_SOFT_PATH/zookeeper >> $PROFILE\r"
  expect "*$"
  send "echo export PATH='$''ZOOKEEPER_HOME'/bin:'$''PATH' >> $PROFILE\r"
  expect "*$"
  send "source $PROFILE\r"
  expect "*$"
  send "touch $USER_SOFT_PATH/zookeeper/data/myid\r"
  expect "*$"
  send "echo $sum > $USER_SOFT_PATH/zookeeper/data/myid\r"
expect eof
EOF
  done
}
function Start_ZK(){
  cat $FILENAME | while read LINE
  do
  str=$LINE
  OLD_IFS="$IFS"
  IFS=":"
  arr=($str)
  IFS="$OLD_IFS"
  IP=${arr[0]}
/usr/bin/expect <<-EOF
  spawn ssh $IP
  expect "*$"
  send "zkServer.sh start\r"
expect eof
EOF
  done
}
Distribution
Install_ZK
Start_ZK
