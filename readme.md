## Crimes-in-Chicago Streaming Project

This README provides step-by-step instructions to set up and run the streaming crime data processing pipeline on Google Cloud Dataproc with Spark Structured Streaming.

---

## Running instructions
### 1. Create Dataproc cluster.
Run the following command to create a Dataproc cluster:

```bash
gcloud dataproc clusters create "${CLUSTER_NAME}" \
    --enable-component-gateway \
    --region "${REGION}" \
    --subnet default \
    --master-machine-type n1-standard-4 \
    --master-boot-disk-size 50 \
    --num-workers 2 \
    --worker-machine-type n1-standard-2 \
    --worker-boot-disk-size 50 \
    --image-version 2.1-debian11 \
    --optional-components JUPYTER,ZOOKEEPER,DOCKER \
    --project "${PROJECT_ID}" \
    --max-age=3h \
    --metadata "run-on-master=true" \
    --initialization-actions "gs://goog-dataproc-initialization-actions-${REGION}/kafka/kafka.sh"
```

---

### 2. SSH into the Master Node and clone the repository

1. SSH into the master node of your newly created cluster:
2. Clone and enter the project repository:

   ```bash
   git clone https://github.com/StaGlo/Crimes-in-Chicago.git
   cd Crimes-in-Chicago
   ```
3. Make all scripts executable:

   ```bash
   chmod +x scripts/
   ```

---

### 3. Set Environment Variables

Define the following variables for use in subsequent commands:

```bash
CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
BOOTSTRAP_SERVER="${CLUSTER_NAME}-m:9092"
TOPIC_NAME="crimes-in-chicago-topic"
```
---

### TODO describe
- get data
- create topic
- etc

### 4. Submit the Spark Structured Streaming Job

Use `spark-submit` to launch the Python processing application:

```bash
spark-submit \
    --master yarn \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 \
    scripts/processing-app.py \
    --bootstrap-servers $BOOTSTRAP_SERVER \
    --input-topic $TOPIC_NAME \
    --static-file /streaming/static/IUCR_codes.csv \
    --checkpoint-location /streaming/checkpoints/
```
---

