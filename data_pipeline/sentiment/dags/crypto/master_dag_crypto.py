from airflow import DAG
import pendulum
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

with DAG(
        dag_id= "master_dag_crypto",
        schedule_interval="@daily",
        start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        catchup=False
) as dag:
    start_task = BashOperator(
        task_id="start_task",
        bash_command="echo 'Start task'",
    )


    with TaskGroup('el_crypto_pipeline') as el_crypto_pipeline:
        avalanche = TriggerDagRunOperator(
            task_id="el_crypto_avalanche_data_into_mongodb_t",
            trigger_dag_id="el_crypto_avalanche_data_into_mongodb"
        )
        binance_coin = TriggerDagRunOperator(
            task_id="el_crypto_binance_coin_data_into_mongodb_t",
            trigger_dag_id="el_crypto_binance_coin_data_into_mongodb",
        )
        bitcoin = TriggerDagRunOperator(
            task_id="el_crypto_bitcoin_data_into_mongodb",
            trigger_dag_id="el_crypto_bitcoin_data_into_mongodb",
        )
        cardano = TriggerDagRunOperator(
            task_id="el_crypto_cardano_data_into_mongodb_t",
            trigger_dag_id="el_crypto_cardano_data_into_mongodb",
        )
        dogecoin = TriggerDagRunOperator(
            task_id="el_crypto_dogecoin_data_into_mongodb_t",
            trigger_dag_id="el_crypto_dogecoin_data_into_mongodb",
        )
        polkadot = TriggerDagRunOperator(
            task_id="el_crypto_polkadot_data_into_mongodb_t",
            trigger_dag_id="el_crypto_polkadot_data_into_mongodb",
        )
        ripple = TriggerDagRunOperator(
            task_id="el_crypto_ripple_data_into_mongodb_t",
            trigger_dag_id="el_crypto_ripple_data_into_mongodb",
        )
        tennis = TriggerDagRunOperator(
            task_id="el_crypto_shiba_inu_data_into_mongodb_t",
            trigger_dag_id="el_crypto_shiba_inu_data_into_mongodb",
        )
        volleyball = TriggerDagRunOperator(
            task_id="el_crypto_solana_data_into_mongodb_t",
            trigger_dag_id="el_crypto_solana_data_into_mongodb",
        )
        stellar = TriggerDagRunOperator(
            task_id="el_crypto_data_into_mongodb_t",
            trigger_dag_id="el_crypto_data_into_mongodb"
        )
    wait_1_min = BashOperator(
        task_id='wait_1_min',
        bash_command='sleep 60',
    )

    trigger10 = TriggerDagRunOperator(
        task_id="etl_crypto_data_into_new_mongodb_t",
        trigger_dag_id="etl_crypto_data_into_new_mongodb"
    )
    start_task >> el_crypto_pipeline >> wait_1_min >> trigger10
