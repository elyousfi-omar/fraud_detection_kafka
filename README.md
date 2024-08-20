# Real Time Fraud Detection using Kafka and Spark - Isolation Forest

### 1. Introduction

Welcome! This project is designed to detect fraudulent transactions using a Kafka consumer implemented with PySpark. The system reads transaction data from a Kafka topic, processes it, and writes the results to a Delta table for further analysis.
* Prerequisites:
`Python`
`Databricks`
`Confluent Kafka`


- Clone the repository:
```
git clone https://github.com/elyousfi-omar/fraud_system.git
```
- Create and activate a virtual environment:
```
python -m venv venv
source venv/bin/activate  # On Windows use 'venv\Scripts\activate'
```
- Install the required packages:
```
pip install -r requirements.txt
```
- Set up environment variables:
```
CONFLUENT_API=<your_confluent_api_url>
API_KEY=<your_api_key>
CLUSTER_ID=<your_cluster_id>
```
- Set up client properties:
```
bootstrap.servers= <server>
sasl.username=<username>
sasl.password=<password>
```

### 2. Workflow and Infrastructure

![real_time_fraud_detection drawio (1)](https://github.com/user-attachments/assets/feb14833-e843-4670-88bc-d9f1157427c7)


### 3. Project Structure
```
fraud_system/
│
├── src/
│   ├── FraudDetection/
│   │   ├── __init__.py            # Initializes the FraudDetection module
│   │   ├── FraudDetection.ipynb   # Contains the Fraud Detection forecasting in Pyspark
│   │
│   ├── KafkaConsumer/
│   │   ├── __init__.py            # Initializes the KafkaConsumer module
│   │   ├── KafkaConsumer.py       # Contains the KafkaConsumer class
│   │   ├── KafkaConsumer.ipynb    # Contains the Notebook Consumer in a jupyter notebook
│   │
│   ├── KafkaProducer/
│   │   ├── __init__.py            # Initializes the KafkaProducer module
│   │   ├── client.properties      # Contains client properties
│   │   ├── producer.py            # Contains the KafkaProducer class
│   │
│   ├── ModelBuilding/
│   │   ├── __init__.py            # Initializes the ModelBuilding module
│   │   ├── data_processing.py     # Contains data-processing steps using to build the model
│   │   ├── model_training.ipynb   # The model training notebook
│   │
│   ├── __init__.py                # Initializes the src module
│   ├── main.py                    # Producer simulation
│   ├── utils.py                   # Util functions
│   └── .env.py                    # Environment variables (not included in the repository)
│
├── requirements.txt               # Lists the required Python packages
├── .gitignore                     # File to ignore while pushing to remote repository
└── README.md                      # Project documentation
```
### 4. Project 
- Simulating transactions streaming to a Kafka topic
![image](https://github.com/user-attachments/assets/033e045b-8feb-4bbd-8441-5e35c84ce26e)
You could reproduce this, by running **src/main.py**
- Spark job to consume data and store it in data lake
![image](https://github.com/user-attachments/assets/467e3daa-1d27-4e85-889f-9b83423aab7c)
The job is always on and consuming transactions from the kafka topic.
- Fraud Detection using Isolation Forest
![image](https://github.com/user-attachments/assets/8bc9d48b-afcd-4aa2-9155-247d39d5586a)
This job is triggerd on file arrival:
![image](https://github.com/user-attachments/assets/5a4fe9c6-3f2b-4aa5-983d-6e5ab3858366)
Once we get a new transaction consumed, we execute the job, save the predicted result as parquet file, and move the consumed transaction from data folder, so we won't trigger the fraud model on it again.
![image](https://github.com/user-attachments/assets/7c0a7b0b-9ad8-48d6-a5d7-a4e340ec3eb7)

### 5. Next Steps:

- Visualization: Develop a dashboard to visualize fraudulent transactions, providing clear insights and trends for better decision-making.
- Data Quality: Implement a data quality pipeline using the Great Expectations library in Python. This pipeline will ensure that all consumed transactions adhere to established data quality standards.
- Real-Time Performance Monitoring: Given the critical nature of fraud detection in banking, it's essential to ensure high-quality detection and real-time response capabilities. This will enhance the effectiveness of the fraud detection system and protect against potential threats.
