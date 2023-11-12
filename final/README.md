# ECE5984 Final Project
by Chang Kyu Kim and Eugene Jung Jo

## Setup

* Clone and symlink the project final folder in the airflow directory and launch the Airflow server
```bash
cd /tmp

git clone https://github.com/iameugenejo/ece5984project.git

cd ~/airflow

# backup dags
mv dags dags_backup

# symlink the project folder
ln -sf /tmp/ece5894project dags

# install requirements
pip install -r dags/requirements.txt

airflow standalone
```

* Setup Airflow Variables
  * Required:
    * `DB_PASSWORD`: database password
  * Optional
    * `DB_URL`: database url
    * `DB_USER`: database user
    * `DB_DB`: database name 
    * `SOURCE_PATH`: raw data source path (must be a public HTTP link to a zip file)
    * `DL_PATH`: datalake path (must be an accessible s3 path)
    * `DW_PATH`: data warehouse path (must be an accessible s3 path)
    * `TITLE_COLUMN`: title column name
    * `DESCRIPTION_COLUMN`: description column name
