#!/usr/bin/env bash
#useage:flink.sh flink-log start
export PATH=/etc:/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin:/usr/java/jdk1.8.0_162/bin
if [ -z "${LOG_DIR}" ];then
    LOG_DIR=/Users/lixuewei/logs
fi
export LOG_DIR

if [ -z "${FM_HOME}" ];then
    FM_HOME=$(cd `dirname $0`;cd ..; pwd)
fi
export FM_HOME

if [ -z "${FM_CONF_DIR}" ] || [ ! -d "${FM_CONF_DIR}" ];then
    FM_CONF_DIR=${FM_HOME}/conf
fi
export FM_CONF_DIRF

if [ -f ${FM_CONF_DIR}/fm-env.sh ];then
    source ${FM_CONF_DIR}/fm-env.sh
fi

function usage {
    echo "Usage: `basename $0` <service> (status|start|stop|restart) [-f] [-w 10]"
    echo
    echo "    Services:"
    echo "      flink-zabbix flink zabbix process."
    echo "      flink-snmp   flink snmp process."
    echo "    Commands:"
    exit 1
}

# $0 classname 进程存在，返回进程号；否则返回""
function get_pid_by_classname {
    local class_name=$1
    if [ -z ${class_name} ];then
        return 1
    fi
#    local jpid=`jps -l | grep ${class_name} | awk '{print $1}' | sed 's/[[:space:]]//g'`
    local jpid=$(jps -ml | grep ${class_name} | awk '{print $1}' | sed 's/[[:space:]]//g')
    if [ -z "${jpid}" ];then
        local pid=`ps -ef | grep ${class_name} | grep -v grep| awk '{print $2}' | sed 's/[[:space:]]//g'`
        if [ -z "${pid}" ];then
            return 2
        fi
    else
        echo ${jpid}
    fi
    return 0
}

# $0 classname 进程号存在：0，不存在：1
function is_running_by_classname {
    local pid=$(get_pid_by_classname $1)
    echo "pid is:" ${pid}
    if [ -z "${pid}" ];then
        return 1
    else
        return 0
    fi
}

# $0 .+
function wait_for {
    if [ $# -ne 2 ];then
        return 1
    fi
    local condition=$1
    local seconds=$2
    while ! ${condition}; do
        echo ${condition}
        ((seconds--)) # always wait when seconds set to 0
        if [ ${seconds} -eq 0 ];then # timed out
            return 2
        fi
        sleep 1
    done
    return 0
}

# $0 .+
function not {
    if [ $# -lt 1 ];then
        echo "Function not(): Illegel parameter." >&2
        return 1
    fi
    if $@;then
        return 2
    else
        return 0
    fi
}

# $0 service(master|worker)
function get_classname_by_service {
    if [ $# -ne 1 ];then
        return 1
    fi
    local service=$1
    if [ -z ${service} ];then
        return 1
    fi
    case ${service} in
      flink-log)
        echo "com.dtc.java.analytic.prometheus.Worker"
        return 0
        ;;
      flink-zabbix)
        echo "com.dtc.java.analytic.zabbix.PrometheusToFlink"
        return 0
        ;;
      flink-snmp)
        echo "com.dtc.analytics.works.Daily"
        return 0
        ;;
      *)
        return 2
        ;;
esac
}

# $0 flink-zabbix|flink-snmp
function start_service {
    local service=$1
    local classname=`get_classname_by_service ${service}`
    local ret=$?
    if [ ${ret} -ne 0 ];then
        return 1
    fi
    if is_running_by_classname ${classname};then
        return 2
    else
        local out_file=${LOG_DIR}/dtc-${service}.out
        echo -n "[`date +'%F %T'`] " >> ${out_file} 2>&1
        start_${service}
#        start_${service} >> ${out_file} 2>&1 &
        echo "Launching service ${service}. see ${out_file}"
        return 0
    fi
}



# $0 master|worker
function stop_service {
    local service=$1
    local classname=`get_classname_by_service ${service}`
    local ret=$?
    if [ ${ret} -ne 0 ];then
        return 1
    fi
    if is_running_by_classname ${classname};then
        local pid=$(get_pid_by_classname ${classname})
        kill ${pid}
        return 0
    else
        return 2
    fi
}

# $0 master|worker
function force_stop_service {
    local service=$1
    local classname=`get_classname_by_service ${service}`
    local ret=$?
    if [ ${ret} -ne 0 ];then
        return 1
    fi
    if is_running_by_classname ${classname};then
        local pid=$(get_pid_by_classname  ${classname})
        read -p "Sure to kill ${service}(pid=${pid}) with -9?(n/Y)" ack
        if [ "x${ack}" != "xY" ];then
            echo "Canceled." >&2
            return 2
        fi
        kill -9 ${pid}
        echo "Done"
        return 0
    else
        echo "No ${service} running." >&2
        return 2
    fi
}

# $0 master|worker
function service_status {
    local service=$1
    local classname=`get_classname_by_service ${service}`
    local ret=$?
    if [ ${ret} -ne 0 ];then
        return 1
    fi

    if is_running_by_classname ${classname};then
        local pid=$(get_pid_by_classname ${classname})
        echo "Service ${service} is running, pid=${pid}"
        return 0
    else
        echo "Service ${service} is not running." >&2
        return 2
    fi
}

# $0 master|worker [tries]
function start_service_and_wait {
    local service=$1
    local classname=`get_classname_by_service ${service}`
    local ret=$?
    if [ ${ret} -ne 0 ];then
        return 1
    fi

    local tries=0
    if [ $# -eq 2 ];then
        local tries=$2
    fi
    if start_service ${service};then
        echo "Starting ${service}..."
#        wait_for "is_running_by_classname ${classname}" ${tries}
#        local ret=$?
#        if [ ${ret} -eq 0 ];then
            echo "Start ${service} successfully."
            return 0
#        else
#            echo "Start ${service} failed, timed out." >&2
#            return 3
#        fi
    else
        echo "Service ${service} is already running." >&2
        return 2
    fi
}

# $0 master|worker [tries]
function stop_service_and_wait {
    local service=$1
    local classname=`get_classname_by_service ${service}`
    local ret=$?
    if [ ${ret} -ne 0 ];then
        return 1
    fi

    local tries=0
    if [ $# -eq 2 ];then
        local tries=$2
    fi
    if stop_service ${service};then
        echo "Shutting down ${service}..."
        wait_for "not is_running_by_classname ${classname}" ${tries}
        local ret=$?
        if [ ${ret} -eq 0 ];then
            echo "Stop ${service} successfully."
            return 0
        else
            echo "Stop ${service} failed, timed out." >&2
            return 3
        fi
    else
        echo "No ${service} running." >&2
        return 2
    fi
}

# $0 master|worker
function restart_service_and_wait {
    local service=$1
    if [ -z "${service}" ];then
        return 2
    fi
    stop_service_and_wait ${service} 0
    start_service_and_wait ${service} 0
    return $?
}

function main {
    service=
    cmd=
    force=
    waits=0

    [ $# -lt 2 ] && usage
    service=$1
    shift
    cmd=$1
    shift
    while getopts :w:f OPTION
    do
        case $OPTION in
            w)
                waits=$OPTARG
                ;;
            f)
                force="true"
                ;;
            \?)
                usage
                ;;
        esac
    done
    shift $(($OPTIND - 1))
    case ${cmd} in
        status)
            service_status ${service}
            ;;
        start)
            start_service_and_wait ${service} ${waits}
            ;;
        stop)
            if [ -z ${force} ];then
                stop_service_and_wait ${service} ${waits}
            else
                force_stop_service ${service}
            fi
            ;;
        restart)
            restart_service_and_wait ${service}
            ;;
        *)
            usage;
            ;;
    esac
}

# main $@ | tee -a ${LOG_DIR}/${service}.out 3>&1 1>&2 2>&3 | tee -a ${LOG_DIR}/${service}.err
main $@

