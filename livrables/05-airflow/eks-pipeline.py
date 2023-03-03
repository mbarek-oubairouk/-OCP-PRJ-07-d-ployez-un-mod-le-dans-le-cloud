# airflow standalone
from datetime import datetime, timedelta
from textwrap import dedent
# module to handle variables
from airflow.models import Variable
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

#from airflow.operators.dummy import DummyOperator, EmptyOperator
from airflow.operators.python import BranchPythonOperator
# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from configparser import ConfigParser, ExtendedInterpolation

# variables
conf = ConfigParser(interpolation=ExtendedInterpolation())
conf.optionxform=str
in_file=Variable.get('dirconf')+"/conf.ini"
conf.read(in_file, encoding='utf-8')


dirliv = conf['deploy']['livrables']
aws_id=conf['aws']['aws_id']
cluster_name=conf['deploy']['cluster_name']
branch=conf["run"]["branch"]

def check_cond():
    if branch == "create_and_execute":
        return 'Generation_templates_yaml'
    else:
        return 'Lancer_Job_Spark'                       


with DAG(
    "eks-pipeline",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    description="pipeline spark on eks",
    schedule=timedelta(days=1),
    start_date=datetime(2023, 2, 1),
    catchup=False,
    tags=["déploiement eks"],
) as dag:


    make_request = BranchPythonOperator(
        task_id='verification',
        python_callable= check_cond,
        dag=dag
    )


    t1 = BashOperator(
        task_id="Generation_templates_yaml",
        bash_command=f"$SH/prepare_template.sh {in_file}",
        env={
            "SH": f"{dirliv}/05-airflow/scripts/",
        },
        append_env=False
    )
    t1.doc_md = dedent(
        """
    #### Génération des fichiers manifests à partir des templates 
     - Valorisation des variables
     - Les fichies générés seront disponibles dans `05-airflow/templates`
    """
    )

    t2 = BashOperator(
        task_id="Creation_Cluster_nodeGroups",
        depends_on_past=False,
        bash_command="eksctl create cluster -f $CRD/01-eksctl.yaml  ",
        env={
            "CRD": f"{dirliv}/05-airflow/templates",
        },
        append_env=True,
    )

    t2.doc_md = dedent(
        """
    #### Création du cluster eks et groupes de noeuds
     - command : `eksctl create cluster -f $CRD/01-eksctl.yaml`
    """
    )

    t3 = BashOperator(
        task_id="Creation_NameSpace_spark-fargate",
        depends_on_past=False,
        bash_command="kubectl create namespace spark-fargate",
        retries=0,
    )

    t4 = BashOperator(
        task_id="Mise_A_L_Echelle",
        depends_on_past=False,
        bash_command="kubectl apply -f $CRD/02-cluster_autoscaler.yaml",
        env={
            "CRD": f"{dirliv}/05-airflow/templates",
        },
        append_env=True,
        retries=0,
    )
    
    t5 = BashOperator(
        task_id="Creation_CompteDeService",
        depends_on_past=False,
        bash_command=" eksctl create iamserviceaccount \
                          --name spark-fargate \
                          --namespace spark-fargate \
                          --cluster $CLUSTER_NAME \
                          --attach-policy-arn arn:aws:iam::$AWS_ID:policy/access2s3 \
                          --approve --override-existing-serviceaccounts",
        retries=0,
        env={
            "CLUSTER_NAME": f"{cluster_name}",
            "AWS_ID":f"{aws_id}"
        },
        append_env=True,
    )
    t1.doc_md = dedent(
        """
    """
    )

    dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG; OR
    dag.doc_md = """
    Pipeline pour déploiement ML Spark dans cloud aws
    """  # otherwise, type it like this
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
    {% endfor %}
    """
    )

    # t5 = BashOperator(
    #     task_id="templated",
    #     depends_on_past=False,
    #     bash_command=templated_command,
    # )

    t6 = BashOperator(
        task_id="kubernetes_dashboard",
        depends_on_past=False,
        bash_command="kubectl apply -f https://raw.githubusercontent.com/kubernetes/dashboard/v2.6.1/aio/deploy/recommended.yaml",
        retries=0,
    )
    

    t7 = BashOperator(
        task_id="Lancer_Job_Spark",
        depends_on_past=False,
        bash_command="kubectl apply -f $CRD/03-spark-job-fargate.yaml",
        env={
            "CRD": f"{dirliv}/05-airflow/templates",
        },
        append_env=True,
        retries=0,
        trigger_rule='none_failed_min_one_success'
    )
 
    
    t1 >> t2 >> [t3, t4] >> t5 >> t6 >> t7 

    make_request  >> [ t1 , t7 ]
    
 