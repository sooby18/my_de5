#!/bin/bash
AIRFLOW_BASE=/home/ubuntu/airflow
VIRTUAL_ENV_DIR=$AIRFLOW_BASE/afenv
AIRFLOW_HOME=$AIRFLOW_BASE/airflow_home
source $VIRTUAL_ENV_DIR/bin/activate
kill -s TERM `cat $AIRFLOW_BASE/run/werserver.pid`
kill -s TERM `cat $AIRFLOW_BASE/run/scheduler.pid`
