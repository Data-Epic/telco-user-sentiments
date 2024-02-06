from airflow import DAG
import pendulum
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

with DAG(
        dag_id= "master_dag_sport",
        schedule_interval="@daily",
        start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        catchup=False
) as dag:
    start_task = BashOperator(
        task_id="start_task",
        bash_command="echo 'Start task'",
    )


    with TaskGroup('el_sport_pipeline') as el_sport_pipeline:
        badminton = TriggerDagRunOperator(
            task_id="el_sport_badminton_data_into_mongodb_t",
            trigger_dag_id="el_sport_badminton_data_into_mongodb"
        )
        baseball = TriggerDagRunOperator(
            task_id="el_sport_baseball_data_into_mongodb_t",
            trigger_dag_id="el_sport_baseball_data_into_mongodb",
        )
        boxing = TriggerDagRunOperator(
            task_id="el_sport_boxing_data_into_mongodb_t",
            trigger_dag_id="el_sport_boxing_data_into_mongodb",
        )
        cricket = TriggerDagRunOperator(
            task_id="el_sport_cricket_data_into_mongodb_t",
            trigger_dag_id="el_sport_cricket_data_into_mongodb",
        )
        football = TriggerDagRunOperator(
            task_id="el_sport_football_data_into_mongodb_t",
            trigger_dag_id="el_sport_football_data_into_mongodb",
        )
        formula1 = TriggerDagRunOperator(
            task_id="el_sport_formula1_data_into_mongodb",
            trigger_dag_id="el_sport_formula1_data_into_mongodb",
        )
        rugby = TriggerDagRunOperator(
            task_id="el_sport_rugby_data_into_mongodb_t",
            trigger_dag_id="el_sport_rugby_data_into_mongodb",
        )
        tennis = TriggerDagRunOperator(
            task_id="el_sport_tennis_data_into_mongodb_t",
            trigger_dag_id="el_sport_tennis_data_into_mongodb",
        )
        volleyball = TriggerDagRunOperator(
            task_id="el_sport_volleyball_data_into_mongodb_t",
            trigger_dag_id="el_sport_volleyball_data_into_mongodb",
        )
        sport_key = TriggerDagRunOperator(
            task_id="el_sport_data_into_mongodb_t",
            trigger_dag_id="el_sport_data_into_mongodb"
        )
    wait_1_min = BashOperator(
        task_id='wait_1_min',
        bash_command='sleep 60',
    )

    trigger10 = TriggerDagRunOperator(
        task_id="etl_sport_data_into_new_mongodb",
        trigger_dag_id="etl_sport_data_into_new_mongodb"
    )
    start_task >> el_sport_pipeline >> wait_1_min >> trigger10
