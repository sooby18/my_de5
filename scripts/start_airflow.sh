#!/bin/bash
export AIRFLOW_BASE=/home/ubuntu/airflow
export VIRTUAL_ENV_DIR=$AIRFLOW_BASE/afenv
export AIRFLOW_HOME=$AIRFLOW_BASE/airflow_home
source $VIRTUAL_ENV_DIR/bin/activate

#Start DB
airflow initdb

#Start Webserver
airflow webserver -D --pid $AIRFLOW_BASE/run/werserver.pid --stdout $AIRFLOW_BASE/logs/webserver/webserver.out --stderr $AIRFLOW_BASE/logs/webserver/webserver.err --log-file $AIRFLOW_BASE/logs/webserver/webserver.log --access_logfile $AIRFLOW_BASE/logs/webserver/access

#Start Scheduler
airflow scheduler -D  --pid $AIRFLOW_BASE/run/scheduler.pid --stdout $AIRFLOW_BASE/logs/scheduler/scheduler.out --stderr $AIRFLOW_BASE/logs/webserver/scheduler.err --log-file $AIRFLOW_BASE/logs/webserver/scheduler.log
