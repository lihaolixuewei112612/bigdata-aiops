#!/bin/bash
export http_proxy=http://10.254.255.55:3128
export https_proxy=http://10.254.255.55:3128
HOSTNAME="173.2.50.151"    #数据库信息
PORT="3306"
USERNAME="dingdian"
PASSWORD="123456"

DBNAME="pub_sys"         #数据库名称
TABLENAME="t_etl_job_logs"
home="ids测试环境ids任务发生异常,请注意"
home_1="ids测试环境ids任务无异常."
webhook='https://oapi.dingtalk.com/robot/send?access_token=1390ceb380a86ddec12c1daf27be8fe9781be00cc4f21fe9559c7f5c12949a37'
sql="select count(*) from springboot.t_etl_job_logs t WHERE t.STAT_DATE=DATE_SUB(CURDATE(),INTERVAL 1 DAY) and t.status!=2"

function SendMsgToDingding() {
    curl $webhook -H 'Content-Type: application/json' -d "
    {
        'msgtype': 'text',
        'text': {
            'content': '$1'
        },
        'at': {
            'isAtAll': true
        }
    }"
}

COMMAND1="mysql -h${HOSTNAME} -P${PORT} -u${USERNAME} -p${PASSWORD} ${DBNAME} -e ${sql}"

if [ $? = 0 ];then
	SendMsgToDingding $home
else
	SendMsgToDingding_1 $home_1
fi



