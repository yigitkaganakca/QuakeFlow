"""QuakeFlow shared library.

Imported by the backfill container and by Airflow DAGs (mounted at
/opt/airflow/src and added to PYTHONPATH). Keep this side-effect free so it
loads cheaply inside DAG parsing.
"""
from .config import settings 
