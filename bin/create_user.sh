#!/bin/bash
# using by root and the formate of user.txt is
# 10.3.7.231:root:123456
# 10.3.7.232:root:123456
#分发服务器需要先安装expect：yum install expect -y

FILENAME=user.txt
USER_ADD=$1
dtc_chmod=`which chmod`
dtc_useradd=`which useradd`
dtc_passwd=`which passwd`
function create_user(){
        cat $FILENAME | while read LINE
        do
        str=$LINE
        OLD_IFS="$IFS"
        IFS=":"
        arr=($str)
        IFS="$OLD_IFS"

        IP=${arr[0]}
        USER=${arr[1]}
        PASSWORD=${arr[2]}
/usr/bin/expect <<-EOF
        spawn ssh $IP
        expect {
                "*yes/no)?" { send "yes\n\r"
                         expect "*ssword:" { send "$PASSWORD\r" }
                }
                "*ssword:" { send "$PASSWORD\r" }
                "id_rsa':*" { send "\r"
                                expect "*ssword:" { send "$PASSWORD\r" }
                        }
                }
        expect "*#"
        send "yum install expect -y\r"
        expect "*#"
        send "cd /root/\r"
        expect "*#"
        send "$dtc_useradd $USER_ADD\r"
        expect "*#"
        send "$dtc_passwd $USER_ADD\r"
        expect "*:"
        send "$USER_ADD\r"
        expect "*:"
        send "$USER_ADD\r"
        expect "*#"
        send "$dtc_chmod u+w /etc/sudoers\r"
        expect "*#"
        send "echo '$USER_ADD    ALL=(ALL)    ALL' >>  /etc/sudoers\r"
        expect "*#"
        send "$dtc_chmod u-w /etc/sudoers\r"
expect eof
EOF
        done
}
create_user
