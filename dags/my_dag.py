#
# from airflow import DA Step 1: Make the Imports
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from random import randint

#
def _training_model():
    return randint(1,10)
def _choosing_best_model(ti):
    accuracies= ti.xcom_pull(task_ids=[
        'training_model_A',
        'training_model_B',
        'training_model_C'
    ])
    if max(accuracies) > 8:
        return 'accurate'
    return 'inaccurate'

# Step 2: Create the Airflow DAG object
with DAG(
    'my_dag',
    description='A simple tutorial DAG',
    schedule_interval='@daily',
    start_date=datetime(2021, 1 ,1),
    catchup=False
) as dag:
    # Step 3: Add your tasks!

    # Tasks are implemented under the dag object
#    training_model_A = PythonOperator(
#        task_id="training_model_A",
#        python_callable=_training_model,

#    )

#     training_model_B = PythonOperator(
#        task_id="training_model_B",
#        python_callable=_training_model,

#    )
#     training_model_C = PythonOperator(
#        task_id="training_model_C",
#        python_callable=_training_model,

#    )

    training_model_tasks= [
        PythonOperator(
            task_id=f"training_model_{model_id}",
            python_callable=_training_model,
            op_kwargs={
                "model": model_id
            }

        ) for model_id in ['A', 'B', 'C']
    ]
    choosing_best_model = BranchPythonOperator(
        task_id="choosing_best_model",
        python_callable= _choosing_best_model
    )
    accurate = bash_task = BashOperator(
        task_id="accurate",
        bash_command='echo "accurate"',
    )
    inaccurate = BashOperator(
        task_id="inaccurate",
        bash_command='echo "inaccurate"',
    )
#
# Step 4: Defining dependencies
training_model_tasks >> choosing_best_model >> [accurate, inaccurate]
