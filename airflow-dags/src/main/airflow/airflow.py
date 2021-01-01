#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import sys
import copy
import pendulum
import logging
from airflow import DAG
from airflow import utils
from airflow import AirflowException
from airflow.operators import HadoopOperator
from airflow.operators import JavaOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators import ShellOperator
from airflow.operators.python_operator import PythonOperator
from datetime import date, datetime, timedelta
from airflow.operators import SparkSubmitOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.executors.sequential_executor import SequentialExecutor

sys.path.append("/airflow/dags/subdag/")
from subdag import sub_dag
dag_utils = '/airflow/dags/utils/dag_utils.py'
sys.path.append(os.path.dirname(os.path.expanduser(dag_utils)))
import dag_utils

SOURCE_NAME = "airflow_dag"
default_params_file = '/airflow/dags/utils/default_config'
user_name = ''
config_bucket_json = dag_utils.get_config('airflow.dag.prod')
failure_email_address = str(config_bucket_json['failure_email_address'])
success_email_address = str(config_bucket_json['success_email_address'])
app_args = {
   'lock_dir' : str(config_bucket_json['lock_dir']),  # lock directory used to run one instance of spark at a time and prevent 2 instances from running simultaneously
   'application_jar' : str(config_bucket_json['application_jar']),
   'libjar_path' : str(config_bucket_json['jar_location'])
}
main_class_config = dag_utils.get_default_params(default_params_file)
main_class_config['mapred.job.queue.name'] = str(config_bucket_json['queue'])
main_class_config['computeDate'] = '${computeDate}'
main_class_config['configBucket'] = 'airflow.dag.pipelines.prod'
main_class_config['configIdentifier'] = str(config_bucket_json['configIdentifier'])
main_class_config['jobName'] = 'AirflowDag'
main_class_config['jobRunIdentifier'] = str(config_bucket_json['jobRunIdentifier'])
main_class_config['jobIdentifier'] = 'AirflowIdentifier'
main_class_config['counterGroup'] = 'AirflowCounters'
app_args['main_class_config'] = main_class_config
app_args['sla'] = int(str(config_bucket_json['sla']))
app_args['dirPath1'] = str(config_bucket_json['dirPath1'])
app_args['dirPath2'] = str(config_bucket_json['dirPath2'])
application_jar = dag_utils.get_application_jar(app_args.get('application_jar'))
hadoop_classpath = dag_utils.fetch_hadoop_classpath(app_args.get('libjar_path'))

directory_present = os.popen('if hadoop fs -test -d '
                             + app_args['directoryPath'] + ';'
                             + "then echo 'true';else echo 'false'; fi"
                             ).read().replace('\n', '')

"""
Arguments for airflow dag
"""
args = {
    'owner': 'rahul.tandon',
    'start_date': utils.dates.days_ago(1),
    'email': [failure_email_address],
    'email_on_failure': True,
    'provide_context': True,
    'email_on_retry': False,
    'sla': timedelta(minutes=app_args['sla']),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

"""
Airflow Dag parameters
"""
dag = DAG(
    'airflow_dag',
    description='airflow pipeline',
    schedule_interval='0 * * * *',
    start_date=datetime(2020, 05, 10, tzinfo=pendulum.timezone('Asia/Kolkata')),
    catchup=False,
    default_args=args,
    )

"""
Gets compute date of the airflow task
"""
compute_date_task = PythonOperator(
    task_id = 'get_compute_date',
    python_callable = dag_utils.get_compute_date,
    op_kwargs={'run_interval': 0, 'config_bucket_json': config_bucket_json, 'source': SOURCE_NAME},
    provide_context = True,
    dag = dag
)

"""
Gets dag run id
"""
def get_dag_run_id(**kwargs):
    context = kwargs
    dagRunId = context['dag_run'].run_id
    kwargs['ti'].xcom_push(key='dagRunId', value=dagRunId)

dag_run_id = PythonOperator(task_id='dag_run_id',
                            python_callable=get_dag_run_id,
                            provide_context=True,
                            dag=dag,
                            trigger_rule='one_success'
                            )

'''Python Callable to determine status of check_lock_task and direct branch operator.'''
def lock_branching(success, failure, **kwargs):
    ti = kwargs['ti']
    status = ti.xcom_pull(key='status', task_ids='check_lock')
    logging.info("Status found: " + status)
    # Lock dir not found
    if (status == "1"):
        return success
    else:
        return failure

"""
Check-lock-task
"""
check_lock_task = ShellOperator(dag=dag, task_id="check_lock",
                                command="hadoop fs -test -d " + app_args["lock_dir"],
                                extract_output=1, extract_status=1, supress_exception=1)

"""
Lock-branching-task
"""
lock_branching_task = BranchPythonOperator(dag=dag, task_id="lock_status",
                                           python_callable=lock_branching,
                                           op_kwargs={'success': 'take_lock_task',
                                                      'failure': 'failed_lock_email'},
                                           trigger_rule="one_success")

def is_dir_available(**context):
    directory_present = context['params']['directory_present']
    if directory_present == 'true':
        return 'sub_task'
    else:
        return 'email_task_success'

"""
return whether to exit the dag or run the next steps of dag
"""
def is_updated(**context):
    ti = context['ti']
    is_updated = ti.xcom_pull(key='dagUpdated', task_ids='check_update')
    logging.info("dagUpdated "+ is_updated)
    if is_updated == 'false':
        return 'exit_dag'
    else:
        return 'dag_run_id'

"""
Prepare-inputs
"""
prepare_inputs_task = ShellOperator(
        task_id='prepare_inputs', command="bash /airflow/dags/latestAvailableDir.sh", cargs=[
            app_args['dirPath1'],
            app_args['dirPath2']
            ],
        extract_fields={"latestAvailableDir1": "=","latestAvailableDir2": "="},
        dag=dag
        )

"""
delete output directory if present
"""
delete_output_dir = ShellOperator(
    dag=dag,
    task_id="delete_output_dir",
    supress_exception=1,
    command="sudo -u hadoop fs -rm -r " + app_args['directoryPath'],
    trigger_rule='one_success'
    )

"""
check whether output directory is created
"""
check_output_dir_status = BranchPythonOperator(
    task_id='check_output_dir_status',
    provide_context=True,
    python_callable=is_dir_available,
    params={'directory_present': directory_present},
    dag=dag,
    trigger_rule='one_success'
    )

"""
take lock to avoid running two instances together
"""
take_lock_task = ShellOperator(
    dag=dag, task_id='take_lock_task',
    supress_exception=1,
    command='hadoop fs -mkdir '+ app_args['lock_dir'],
    trigger_rule='one_success'
    )

"""
check whether we need to run the airflow dag or not
"""
check_update = JavaOperator(
    task_id='check_update',
    main_class='com.airflow.CheckUpdate',
    application_jar=application_jar,
    classpath=hadoop_classpath,
    extract_fields={'dagUpdated': ':'},
    extract_output=1,
    extract_status=1,
    main_class_args='airflow.dag.prod',
    dag=dag,
    )

"""
branch the dag
"""
branch_dag = BranchPythonOperator(
    task_id='branch_dag',
    provide_context=True,
    python_callable=is_updated,
    dag=dag,
    trigger_rule='one_success'
    )

"""
Initializes the task for the pipeline
"""
initialize_args = copy.deepcopy(app_args)
initialize_args.update({'application_args': 'initialize'})
initialize_args['main_class_config']['airflowRunId'] = '${dagRunId}'

initialize_task = JavaOperator(
    task_id='initialize_pipeline',
    main_class='com.main.java.PipelineHandler',
    application_jar=application_jar,
    classpath=hadoop_classpath,
    extract_fields={'runId': ':'},
    template_fields=['dagRunId'],
    xcom_task_ids=['dag_run_id'],
    extract_output=1,
    extract_status=1,
    config_params=initialize_args.get("main_class_config", {}),
    main_class_args='initialize',
    dag=dag,
    )

"""
Spark job to do the computation
"""
spark_args = {
    'conf': str(config_bucket_json['spark_config']),
    'num_executors': str(config_bucket_json['num_executors']),
    'executor_memory': str(config_bucket_json['executor_memory']),
    'executor_cores': str(config_bucket_json['executor_cores']),
    'driver_memory': str(config_bucket_json['driver_memory']),
    'master': str(config_bucket_json['master']),
    'deploy_mode': str(config_bucket_json['deploy_mode']),
    'job_name': 'SparkJob',
    'application_args': 'airflow.dag.prod ${runId} ${dagRunId} ${latestAvailableDir1} ${latestAvailableDir2}' ,
    'queue': str(config_bucket_json['queue']),
    'user_name': '',
    }

spark_pipeline_task = SparkSubmitOperator(
    task_id='spark_pipeline',
    extract_fields={'action-counters': ':'},
    name='spark_pipeline',
    spark_home='/home/sparkhome',
    user_name='',
    template_fields=['dagRunId', 'runId', 'dagRunId', 'latestAvailableDir1', 'latestAvailableDir2'],
    xcom_task_ids=['dag_run_id', "prepare_inputs", 'initialize_pipeline'],
    application_class='com.main.java.SparkJob',
    application_jar=application_jar,
    file_config=spark_args,
    dag=dag,
    trigger_rule='one_success',
    )

"""
Job to publish counters
"""
publish_job_counters_args = copy.deepcopy(app_args)
publish_job_counters_args.update({'application_args': '${runId} AirflowCounters: SparkJob:'})
publish_job_counters_args['main_class_config']['runId'] = '${runId}'

publish_job_counters_task = JavaOperator(
    task_id='publish_job_counters_pipeline',
    extract_fields={'AirflowCounters': ':'},
    main_class='com.airflow.SparkCounterFetcher',
    application_jar=application_jar,
    template_fields=['runId'],
    xcom_task_ids=['initialize_pipeline'],
    classpath=hadoop_classpath,
    extract_output=1,
    extract_status=1,
    config_params=publish_job_counters_args.get('main_class_config', {}),
    main_class_args=publish_job_counters_args['application_args'],
    dag=dag,
    trigger_rule='one_success',
    )

"""
Release lock on failure
"""
failed_release_lock_task = ShellOperator(
                                    task_id="failed-release-lock",
                                    supress_exception=1,
                                    command="hadoop dfs -rm -r " + app_args["lock_dir"],
                                    trigger_rule="one_failed",
                                    dag=dag
                                    )

"""
Release lock on success
"""
release_lock_task = ShellOperator(dag=dag, task_id='release_lock',
                                  supress_exception=1,
                                  command='sudo -u hadoop dfs -rm -r '
                                   + app_args['lock_dir'],
                                  trigger_rule='one_success'
                                  )

"""
Email on success
"""
success_email_task = EmailOperator(
        to=success_email_address,
        task_id='email_task_success',
        params={'jobName': app_args['main_class_config']['jobName']},
        provide_context=True,
        subject='P2.Reco.Phoenix.Airflow.{{ params.jobName }}.Succeeded',
        html_content='Hello,<br>Spark Job is successful.'
            + '<br>Compute Date: {{ ti.xcom_pull(task_ids="get_compute_date", key="computeDate") }}'
            + '<br>SparkJob step counters:<br>{{ti.xcom_pull(task_ids="publish_job_counters_pipeline", key="AirflowCounters" )}}'
            + '<br>Thanks',
        dag=dag,
        )

"""
Sub dag runs only if output directory present, used to attach the output directory in email
"""
sub_task = SubDagOperator(
    task_id = "sub_task",
    subdag=sub_dag(args, "airflow_dag", "sub_task", schedule_interval="0 * * * *",startDate=datetime(2020, 05, 10, tzinfo=pendulum.timezone('Asia/Kolkata'))),
    executor=SequentialExecutor(),
    trigger_rule='one_success',
    dag=dag
    )

"""
Email on Failure
"""
failure_email_task = EmailOperator(
    to=failure_email_address,
    task_id='email_task_failure',
    provide_context=True,
    params={'jobName': app_args['main_class_config']['jobName'],
            'lockDir': app_args['lock_dir']},
    subject='Airflow.{{ params.jobName }}.Failed',
    html_content='Hello,<br>There was an error in spark pipeline.<br>Lock :{{ params.lockDir }}<br>Thanks',
    trigger_rule='one_success',
    dag=dag,
    )

"""
Failure Email on lock failure for example if any other instance is running the second instance will fail with the lock failure message
"""
failed_lock_email = EmailOperator(
    to=failure_email_address,
    task_id='failed_lock_email',
    provide_context=True,
    params={'jobName': app_args['main_class_config']['jobName'],
            'lockDir': app_args['lock_dir']},
    subject='P2.Reco.Phoenix.Airflow.{{ params.jobName }}.Failed',
    html_content='Hello,<br>Lock Already Exists.<br>Lock: {{ params.lockDir }}<br>Thanks,<br>',
    dag=dag,
    )

"""
Close pipeline task used to close the airflow task. Used to update MYSQL flags for counter setting during run of the airflow
"""
close_args = copy.deepcopy(app_args)
close_args.update({'application_args': 'close'})
close_args['main_class_config']['runId'] = '${runId}'

close_task = JavaOperator(
    task_id='close_pipeline',
    extract_fields={'jobStatus': ':'},
    main_class='com.main.java.PipelineHandler',
    application_jar=application_jar,
    template_fields=['runId'],
    xcom_task_ids=['initialize_pipeline'],
    classpath=hadoop_classpath,
    extract_output=1,
    extract_status=1,
    suppress_exception=1,
    config_params=close_args.get('main_class_config', {}),
    main_class_args=close_args['application_args'],
    trigger_rule='one_success',
    dag=dag,
    )

"""
Set Failure Flag on MYSQL
"""
failed_close_task = JavaOperator(
    task_id='failed_close_pipeline',
    main_class='com.main.java.PipelineHandler',
    application_jar=application_jar,
    template_fields=['runId'],
    xcom_task_ids=['initialize_pipeline'],
    classpath=hadoop_classpath,
    extract_output=1,
    extract_status=1,
    suppress_exception=1,
    config_params=close_args.get('main_class_config', {}),
    main_class_args=close_args['application_args'],
    dag=dag,
    trigger_rule='one_failed',
    )

"""
Release lock on success of previous task which denotes failure
"""
release_lock_on_failure = ShellOperator(
    dag=dag,
    task_id="release_lock_on_failure",
    command="hadoop fs -rm -r " + app_args['lock_dir'],
    supress_exception=1,
    trigger_rule="one_success"
)

"""
Raise Airflow exception to avoid success being shown in Airflow dag
"""
def is_dag_failure(**context):
  raise AirflowException(
      "Airflow dag has failed"
  )

"""
Dag failure task which is used to throw exception so that Airflow does not show success
"""
dag_failure_task = PythonOperator(
    task_id='dag_failure_task',
    provide_context=True,
    retries = 0,
    python_callable=is_dag_failure,
    trigger_rule="one_success",
    dag=dag
    )

"""
Exit the dag if no update is done using dummy operator
"""
exit_dag = DummyOperator(
    task_id='exit_dag',
    dag=dag
    )

check_update >> branch_dag >> dag_run_id >> compute_date_task >> check_lock_task >> lock_branching_task
lock_branching_task >> take_lock_task >> prepare_inputs_task >> initialize_task >> delete_output_dir >> spark_pipeline_task >> close_task >> publish_job_counters_task >> check_output_dir_status >> sub_task >> release_lock_task
branch_dag >> exit_dag
check_output_dir_status >> success_email_task >> release_lock_task
lock_branching_task >> failed_lock_email
[take_lock_task, prepare_inputs_task] >> failed_release_lock_task
[sub_task, initialize_task, spark_pipeline_task, publish_job_counters_task, release_lock_task] >> failed_close_task
failed_release_lock_task >> failure_email_task
failed_close_task >> release_lock_on_failure >> failure_email_task
[failed_lock_email, failure_email_task] >> dag_failure_task