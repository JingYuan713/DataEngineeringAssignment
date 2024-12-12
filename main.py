import requests
from datetime import date
from dateutil.relativedelta import relativedelta
import pandas as pd
from sqlalchemy import create_engine

## extract
def extract_commits(since):
    url = f"https://api.github.com/repos/apache/airflow/commits"
    params = {'since': since, 'per_page': 100}
    all_commits = []
    page = 1
    while True:
        params['page'] = page
        response = requests.get(url, params=params)
        response.raise_for_status()
        # print(response)
        # print()
        commits = response.json()
        if not commits:       # checks whether the list commits is empty
            break
        all_commits.extend(commits)
        page += 1
    return all_commits
  
## transform
def transform_commits(commits):
    transformed = []
    for commit in commits:
        if commit['author']:
            transformed.append({
                'committer_name': commit['author']['login'],
                'commit_date': commit['commit']['committer']['date'],
                'message': commit['commit']['message']
            })
    # print(transformed)
    # print()
    return pd.DataFrame(transformed)

## load
def load_to_db(df, db_url):
    engine = create_engine(db_url)
    df.to_sql('commits', engine, if_exists='replace', index=False)

## Run ETL
if __name__ == "__main__":   # the ETL pipeline runs only when the script is executed directly
    since = (date.today() - relativedelta(months=6)).isoformat()

    commits = extract_commits(since)
    # print(commits)
    # print()
    df = transform_commits(commits)
    # print(df)
    # print()
    load_to_db(df, "mssql+pyodbc://LAPTOP-D9ONC2UF\MSSQLSERVER03/AirflowDB?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes")
