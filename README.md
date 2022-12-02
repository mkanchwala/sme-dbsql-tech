# sme-dbsql-tech
DBSQL Workshop - Sample Notebooks

## Create your config file
Before you start using this repo, you should create your own configuration file. In the config folder, you just need to copy the config_template.cfg, name it my_config.cfg and update it with your credentials. This file will be ignored by git.

## Create a python environment
### Create a conda environment (if does not exist yet)
> conda create -n env-python python=3.9 
### Activate the environment
> conda activate env-python
### Install librairies (if not installed yet)
> pip install -r requirements.txt

https://databricks.atlassian.net/wiki/spaces/UN/pages/2659943487/SQL+Execution+API+-+Private+preview+sql+exec+api+private+preview

## Quickstart
1. pre-requisites: 
   - databricks cli installed and configured to point to your workspace
   - odbc driver installed 
1. create you `config/my_config.cfg` as described above
1. install requirements `pip install -r requirements.txt`
1. run `python benchmark.py`