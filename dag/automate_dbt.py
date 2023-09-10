from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime, timedelta

# Define default_args to configure your DAG
default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
with DAG(
    dag_id='dbt_automation',
    default_args=default_args,
    schedule_interval='0 0/8 * * *') as dag: 

    # Cette task est pour tester la connection au server

    dbt_test_task = BashOperator(
        task_id='dbt_test',
        bash_command='cd C:/Users/papa.seye/Documents/dbt_project/dbt_kpi_project && dbt debug',
        
    )

    dbt_seed_task = BashOperator(
    task_id='run_dbt_seed',
    bash_command='cd C:/Users/papa.seye/Documents/dbt_project/dbt_kpi_project && dbt seed'
  # Remplacer le chemin du projet par votre chemin
    )

    dbt_run_task = BashOperator(
        task_id='dbt_run',
        bash_command='cd C:/Users/papa.seye/Documents/dbt_project/dbt_kpi_project && dbt run',
        )

    dbt_test_task = BashOperator(
        task_id='dbt_test',
        bash_command='cd C:/Users/papa.seye/Documents/dbt_project/dbt_kpi_project && dbt test',
    )

    # Task pour envoyer une notification au group aprés exécution

    send_email_task = EmailOperator(
    task_id='send_email',
    to=['dia1997ousmane@gmail.com', 'papaseye296@gmail.com', 'vonewman7@gmail.com'],  # Email recipient(s)
    subject='Test Email from Airflow',  # Email subject
    html_content='C"est mail de notification pour dbt pour vérifier la connectivité',  
    dag=dag,
)

    # Pour tester la connection et envoyer un mail de notification
    dbt_test_task >> send_email_task 

    # Pour charger les fichiers avec dbt seed et exécuter les modeles avec dbt run
    dbt_seed_task>> dbt_run_task   

    

    
