# Streaming pipeline for processing of millions of record
## Overview
This repository contains procedures for setting up a streaming data pipeline for processing millions of records.

# Table of Contents
- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Execution Flow](#execution-flow)


## Introduction <a name="introduction"></a>
This streaming data pipeline simulates a production environment for data ingestion in Kafka and processing in Spark on a multi-node architecture.

## Prerequisites <a name="prerequisites"></a>
Before running this streaming data pipeline, the following prerequisite must be meant.

- Installed Python 3.x
- Docker Compose is installed
- Virtual environment in directory with project directory

## Installation <a name="installation"></a>

Clone the repository to your local machine:

``` 
git clone https://github.com/Arshavin023/data_streaming_with_kafka_and_spark.git
```

Install the required Python packages:

```
pip install -r requirements.txt
```


## Execution Flow
- Start Docker Compose
- Activate virtual environment
```
source virtual_env/bin/activate
```
- Pull required images
```
docker compose up
```
- Access Web UI on dedicated ports