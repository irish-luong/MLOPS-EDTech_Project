# Get base Python image
FROM apache/airflow:2.7.1-python3.9

USER root
RUN apt-get update \
    && apt-get install --no-install-recommends -y nano libmysqlclient21 telnet
USER airflow
ENV HOME=/home/airflow

RUN python -m pip install --upgrade pip

COPY --chown=airflow:root dags $HOME/dags
COPY --chown=airflow:root requirements.txt $HOME/requirements.txt


RUN pip install --no-cache  -r $HOME/requirements.txt

# Set the LD_PRELOAD environment variable
ENV LD_PRELOAD /usr/lib/x86_64-linux-gnu/libstdc++.so.6:$LD_PRELOAD
