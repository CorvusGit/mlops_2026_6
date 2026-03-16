from airflow.decorators import dag, task
from airflow.models import Variable


@dag(schedule=None)
def load_variable_from_lockbox():
    @task
    def print_var_query():
        query = Variable.get("S3_BUCKET_NAME")
        print("query: ", query)
    print_var_query()



load_variable_from_lockbox()
