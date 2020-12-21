import os
import os.path
import json
from subprocess import Popen, STDOUT, PIPE
import logging
import urllib2
from datetime import datetime, timedelta
from airflow import AirflowException


libjar_script = "/airflow/scripts/fetch-libjars.sh"
hadoop_cp_script = "/airflow/scripts/fetch-hadoop-classpath.sh"

log = logging.getLogger(__name__)

def get_config(job_bucket_name):
    bucket_url_prefix = "http://0.0.0.0/buckets/" #add config bucket to be used at runtime

    # GET JOB SPECIFIC BUCKET CONFIGS
    job_config_bucket = urllib2.urlopen(bucket_url_prefix + job_bucket_name).read()
    if job_config_bucket is not None:
        job_config = json.loads(job_config_bucket)
        if job_config is not None and job_config.get('keys') is not None:
            return job_config.get('keys')
    return {}

def fetch_hadoop_classpath(path):
    '''Fetch jars from the input path and returns HADOOP_CLASSPATH'''
    cmd = ''
    if path:
        cmd = 'path=' + path + ";"
        cmd += 'hadoop_cp_script=' + hadoop_cp_script + ";"
        cmd += 'export HADOOP_CLIENT_OPTS=-Djava.io.tmpdir=/airflow/hadoop_tmp;'
        cmd += 'export HADOOP_USER_CLASSPATH_FIRST=true;'
        cmd += 'export HADOOP_CLASSPATH=$(sh $hadoop_cp_script $path):$HADOOP_CLASSPATH; echo "HADOOP_CLASSPATH: $HADOOP_CLASSPATH";'

    return cmd

def get_default_params(file):
    mydict = {}
    with open(file) as myfile:
        for line in myfile:
            key,val = line.partition("=")[::2]
            mydict[key] = val.strip()
    return mydict

def get_compute_date(run_interval, config_bucket_json, source, **kwargs):
    """
    Retrieves date from config and pushes via xcom
    Compute date is one day before the Current date
    """
    context = kwargs
    # airflow triggers in utc, we need to add 5.30hrs
    execution_date = (context["execution_date"] + timedelta(hours=5.50)).strftime("%Y-%m-%d")

    dagRunId = context['dag_run'].run_id
    if 'manual' in dagRunId:
        compute_date = (datetime.strptime(execution_date, "%Y-%m-%d")-timedelta(days=1)).strftime("%Y-%m-%d")
    elif config_bucket_json.get(source+'.computeDate'):
        compute_date = datetime.strptime(config_bucket_json[source+'.computeDate'], "%Y-%m-%d").strftime("%Y-%m-%d")
    else:
        compute_date = (datetime.strptime(execution_date,"%Y-%m-%d")+timedelta(days=run_interval)).strftime("%Y-%m-%d")

    ti = context['task_instance']
    ti.xcom_push("computeDate", compute_date)
    logging.info("Compute Date: " + compute_date)
    input_date = (datetime.strptime(compute_date,"%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    ti.xcom_push("inputDate", input_date)
    logging.info("inputDate: " + input_date)

def get_application_jar(path):
    if os.popen("ls -t %s | head -n1" % path).read().replace("\n", ""):
        application_jar = os.popen("ls -t %s | head -n1" % path).read().replace("\n", "")
    else:
        application_jar = "No jar present in this box"

    print application_jar
    return application_jar
