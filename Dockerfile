#base image
FROM python:3.8-slim

LABEL maintainer="Sourabh"

#arguments
ARG AIRFLOW_VERSION=2.5.1
ARG PYTHON_VERSION=3.8
ARG AIRFLOW_HOME=/opt/airflow 

ENV AIRFLOW_HOME=${AIRFLOW_HOME}

#install libraries

# Install dependencies and tools
RUN apt-get update -yqq && \
    apt-get upgrade -yqq && \
    apt-get install -yqq --no-install-recommends \ 
    wget \
    libczmq-dev \
    curl \
    libssl-dev \
    git \
    inetutils-telnet \
    bind9utils freetds-dev \
    libkrb5-dev \
    libsasl2-dev \
    libffi-dev libpq-dev \
    freetds-bin build-essential \
    default-libmysqlclient-dev \
    apt-utils \
    rsync \
    zip \
    unzip \
    gcc \
    vim \
    locales \
    && apt-get clean

RUN wget https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.8.txt


RUN pip install --upgrade pip && \
    useradd -ms /bin/bash -d ${AIRFLOW_HOME} azurelib && \
    pip install apache-airflow[postgres]==${AIRFLOW_VERSION} --constraint /constraints-3.8.txt

RUN apt install -y pkg-config

RUN pip install apache-airflow-providers-microsoft-mssql

RUN pip install apache-airflow-providers-mysql


RUN pip install 'apache-airflow[amazon]'

RUN pip install apache-airflow-providers-google

RUN pip install apache-airflow-providers-microsoft-azure

# Copy the airflow_init.sh from host to container (at path AIRFLOW_HOME)
COPY ./airflow_commands.sh ./airflow_commands.sh

# Set the airflow_init.sh file to be executable
RUN chmod +x ./airflow_commands.sh

# Set the owner of the files in AIRFLOW_HOME to the user airflow
RUN chown -R azurelib: ${AIRFLOW_HOME}

# Set the username to use
USER azurelib

# Set workdir (it's like a cd inside the container)
WORKDIR ${AIRFLOW_HOME}

#create dags directory
RUN mkdir dags

# Expose ports (just to indicate that this container needs to map port)
EXPOSE 8080

# Execute the airflow_commands.sh
ENTRYPOINT [ "/airflow_commands.sh" ]