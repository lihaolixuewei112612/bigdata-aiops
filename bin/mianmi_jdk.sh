#!/bin/bash
# The formate of user.txt is
## 10.3.7.231:root:123456
# The useful of Script: mianmi_jdk.sh <user> <ip>
# param 1: name of user
# param 2: ip of convergencing

FILENAME=user.txt
USER_ADD=$1
LOCAL_HOST=$2
PASSWORD=$USER_ADD
ssh_dir="/home/$USER_ADD/.ssh/"
author="authorized_keys"
USER_PATH=/home/$USER_ADD
JDK_NAME="jdk-8u161-linux-x64.tar.gz"
JDK_INSTALL_DIR="$USER_PATH/software/java"
PROFILE="$USER_PATH/.bash_profile"
dtc_ln=`which ln`
dtc_ssh_keygen=`which ssh-keygen`
function Distribution(){
  if [ ！-f $FILENAME ] || [ -z $USER_ADD ];then
    echo 'filename is not exist or user is null'
    exit 1
  fi
  if [ -z $PASSWORD ];then
    PASSWORD=$USER_ADD
  fi
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
  spawn scp ./$JDK_NAME $USER_ADD@$IP:/home/$USER_ADD
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

function key_gen(){
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
  expect {
    "*yes/no)?" {send "yes\n\r"
                   expect "*ssword:" {send "$PASSWORD\r"}
          }
          "*ssword:" {send "$PASSWORD\r"}
    }
  expect "*#"
  expect "rm -rf /home/$USER_ADD/.ssh"
  expect "*#"
  send "${dtc_ssh_keygen} -t rsa -P ''\r"
  expect "*rsa):" { send "\r"
          expect "*y/n)?" {send "y\r"}
  }
  expect "*$"
  send "cat /home/$USER_ADD/.ssh/id_rsa.pub >> /home/$USER_ADD/.ssh/authorized_keys\r"
  expect "*$"
  send "chmod 700 /home/$USER_ADD/.ssh/authorized_keys\r"
expect eof
EOF
  done
}

function Convergence(){
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
  expect {
    "*yes/no)?*" { send "yes\r";exp_continue}
    "*ssword:*" { send "$PASSWORD\r" }
    }
  expect "*$ " { send "scp $ssh_dir$author $USER_ADD@$LOCAL_HOST:$ssh_dir$author-$IP\r"
         expect {
             "*yes/no)?*" { send "yes\r";exp_continue}
             "*ssword:*" { send "$PASSWORD\r" }
                }
        }
expect eof
EOF
  done
}

function Distribute_author(){
/usr/bin/expect <<-EOF
  spawn ssh $LOCAL_HOST
  expect {
           "*no)?" {send "yes\r"
                    expect "*word:" {send "$PASSWORD\r"}
                    }
           "*word:" {send "$PASSWORD\r"}
        }
  expect "*#"
  send "cat $ssh_dir$author-* >> $ssh_dir$author\r"
  interact
expect eof
EOF
}
function Test_MianMi(){
  cat $FILENAME | while read LINE
  do
  str=$LINE
  OLD_IFS="$IFS"
  IFS=":"
  arr=($str)
  IFS="$OLD_IFS"
  IP=${arr[0]}
/usr/bin/expect <<-EOF
  spawn scp $ssh_dir$author $USER_ADD@$IP:$ssh_dir$auhor
  expect {
        "*yes/no)?*" { send "yes\r" }
        "*ssword:*" { send "$PASSWORD\r" }
      }
expect eof
EOF
  done
}
function Mkdir_User(){
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
  #expect "*#"
  #send "su - $USER_ADD\r"
  expect "*$"
  send "mkdir install && mkdir software && mkdir logs\r"
expect eof
EOF
  done
}
function Install_jdk(){
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
  send "tar -zxvf $USER_PATH/$JDK_NAME -C $USER_PATH/software\r"
  expect "*$"
  send "$dtc_ln -s $USER_PATH/software/jdk1.8.0_161 $JDK_INSTALL_DIR\r"
  expect "*$"
  send "echo export JAVA_HOME=$JDK_INSTALL_DIR >> $PROFILE\r"
  expect "*$"
  send "echo export PATH='$''JAVA_HOME'/bin:'$''PATH'>> $PROFILE\r"
  expect "*$"
  send "echo export CLASSPATH=.:'$''JAVA_HOME'/lib/dt.jar:'$''JAVA_HOME'/lib/tools.jar >> $PROFILE\r"
  expect "*$"
  send "source $PROFILE\n\r"
expect eof
EOF
  done
}
Distribution
key_gen
Convergence
Distribute_author
Test_MianMi
Mkdir_User
Install_jdk
