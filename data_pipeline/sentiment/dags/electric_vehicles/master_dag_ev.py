from airflow import DAG
import pendulum
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

with DAG(
        dag_id= "master_dag_ev",
        schedule_interval="@daily",
        start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        catchup=False
) as dag:
    start_task = BashOperator(
        task_id="start_task",
        bash_command="echo 'Start task'",
    )


    with TaskGroup('el_ev_pipeline') as el_ev_pipeline:
        audi_etron = TriggerDagRunOperator(
            task_id="el_electric_audi_etron_data_into_mongodb_t",
            trigger_dag_id="el_electric_audi_etron_data_into_mongodb"
        )
        bmwi3 = TriggerDagRunOperator(
            task_id="el_electric_bmwi3_data_into_mongodb_t",
            trigger_dag_id="el_electric_bmwi3_data_into_mongodb",
        )
        chevrolet_bolt = TriggerDagRunOperator(
            task_id="el_electric_chevrolet_bolt_data_into_mongodb_t",
            trigger_dag_id="el_electric_chevrolet_bolt_data_into_mongodb",
        )
        cardano = TriggerDagRunOperator(
            task_id="el_electric_ford_data_into_mongodb_t",
            trigger_dag_id="el_electric_ford_data_into_mongodb",
        )
        hyundai_kona = TriggerDagRunOperator(
            task_id="el_electric_hyundai_kona_data_into_mongodb_t",
            trigger_dag_id="el_electric_hyundai_kona_data_into_mongodb",
        )
        kia_niro = TriggerDagRunOperator(
            task_id="el_electric_kia_niro_data_into_mongodb_t",
            trigger_dag_id="el_electric_kia_niro_data_into_mongodb",
        )
        lucid_air = TriggerDagRunOperator(
            task_id="el_electric_lucid_air_data_into_mongodb_t",
            trigger_dag_id="el_electric_lucid_air_data_into_mongodb",
        )
        nissan_leaf = TriggerDagRunOperator(
            task_id="el_electric_nissan_leaf_data_into_mongodb_t",
            trigger_dag_id="el_electric_nissan_leaf_data_into_mongodb",
        )
        porsche_taycan = TriggerDagRunOperator(
            task_id="el_electric_porsche_taycan_data_into_mongodb_t",
            trigger_dag_id="el_electric_porsche_taycan_data_into_mongodb",
        )
        tesla = TriggerDagRunOperator(
            task_id="el_electric_tesla_data_into_mongodb",
            trigger_dag_id="el_electric_tesla_data_into_mongodb"
        )
    wait_1_min = BashOperator(
        task_id='wait_1_min',
        bash_command='sleep 60',
    )

    trigger10 = TriggerDagRunOperator(
        task_id="etl_ev_data_into_new_mongodb_t",
        trigger_dag_id="etl_ev_data_into_new_mongodb"
    )
    start_task >> el_ev_pipeline >> wait_1_min >> trigger10
