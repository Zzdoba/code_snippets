import os
import json
import pandas as pd
from google.oauth2 import service_account
import datetime
import telebot

# set vars
token = os.environ['TELEGRAM_TOKEN']
chat = int(os.environ['CHAT'])
bot = telebot.TeleBot(token)

creds = json.load(open("/secrets/dbt-service-keyfile"))
credentials = service_account.Credentials.from_service_account_info(creds)
target_path = "/my-project-dbt/target"

project_name_bq = 'my-project'
dataset_name = 'dbt_run_logs'

# notification func with a link to run results table
def notify(alert):
    message = f'''
&#9888; my-project // dbt run failed:

<i>{alert}</i>

<a href="https://console.cloud.google.com/bigquery?authuser=0&project={project_name_bq}&supportedpurview=project&ws=!1m5!1m4!4m3!1s{project_name_bq}!2sdbt_run_logs!3srun_logs">All run results</a>
    '''
    bot.send_message(chat, message, parse_mode='HTML')

# main func with run_results parsing after `dbt run` and loading data to BQ
def process():

    try:
        with open(os.path.join(target_path, "run_results.json")) as json_file:
            data = json.load(json_file)
        
        date = datetime.date.today()
        invocation_id = data['metadata']['invocation_id']
        dbt_version = data['metadata']['dbt_version']
        total_elapsed_time = data['elapsed_time']
        m_type = []
        name = []
        execution_time = []
        status = []
        code = []
        message = []
        rows_affected = []
        bytes_processed = []

        for res in data['results']:
            m_type.append(res['unique_id'].split('.')[0])
            name.append(res['unique_id'].split('.')[2])
            execution_time.append(res['execution_time'])
            status.append(res['status'])
            code.append(res.get('adapter_response').get('code'))
            message.append(res['message'])
            rows_affected.append(res.get('adapter_response').get('rows_affected')),
            bytes_processed.append(res.get('adapter_response').get('bytes_processed'))
            if res['status'] == 'error':
                notify(res['message'])

        log_df = pd.DataFrame(
            {
                'date':date, 
                'invocation_id':invocation_id,
                'dbt_version':dbt_version,
                'total_elapsed_time':total_elapsed_time,
                'type':m_type, 
                'name':name, 
                'execution_time':execution_time, 
                'status':status,
                'code':code,
                'message':message,
                'rows_affected':rows_affected,
                'bytes_processed':bytes_processed
            }
        )

        log_df.to_gbq(
            f'dbt_run_logs.run_logs', 
            f'{project_name_bq}', 
            if_exists = 'append', 
            table_schema = [
                {'name': 'date', 'type': 'DATE'},
                {'name': 'invocation_id', 'type': 'STRING'},
                {'name': 'dbt_version', 'type': 'STRING'},
                {'name': 'total_elapsed_time', 'type': 'FLOAT64'},
                {'name': 'type', 'type': 'STRING'},
                {'name': 'name', 'type': 'STRING'},
                {'name': 'execution_time', 'type': 'FLOAT64'},
                {'name': 'status', 'type': 'STRING'},
                {'name': 'code', 'type': 'STRING'},
                {'name': 'message', 'type': 'STRING'},
                {'name': 'rows_affected', 'type': 'INT64'},
                {'name': 'bytes_processed', 'type': 'INT64'}
            ],
            progress_bar=False, 
            credentials=credentials
        )

        print(f'run results loaded to {project_name_bq}.dbt_run_logs.run_logs')
        del log_df

    except FileNotFoundError as e:
        print('there are no run_results')
        notify(e)

if __name__ == '__main__':
    process()