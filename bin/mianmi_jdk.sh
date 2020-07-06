#!/bin/bash
# The formate of user.txt is
## 10.3.7.231:root:123456
# The useful of Script: mianmi_jdk.sh <user> <ip>
# param 1: name of user
# param 2: ip of convergencing

FILENAME=/home/user.txt
USER_ADD=$1
LOCAL_HOST=$2
PASSWORD="123456"
ssh_dir="/root/.ssh/"
author="authorized_keys"
USER_PATH=/home/$USER_ADD
JDK_NAME="jdk-8u161-linux-x64.tar.gz"
JDK_INSTALL_DIR="$USER_PATH/software/java"
PROFILE="/etc/profile"
dtc_ln=`which ln`
dtc_ssh_keygen=`which ssh-keygen`

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
  expect "rm -rf ~/.ssh"
  expect "*#"
  send "${dtc_ssh_keygen} -t rsa -P ''\r"
  expect "*rsa):" { send "\r"
          expect "*y/n)?" {send "y\r"}
  }
  expect "*$"
  send "cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys\r"
  expect "*$"
  send "chmod 700 ~t/.ssh/authorized_keys\r"
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
key_gen
Convergence
Distribute_author
Test_MianMi
