## Crimes-in-Chicago Streaming Project

This README provides step-by-step instructions to set up and run the streaming crime data processing pipeline on Google Cloud Dataproc with Spark Structured Streaming.

---

## Running instructions
### 1. Create Dataproc cluster
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

### 2. SSH into the Master Node and prepare script files

1. SSH into the master node of your newly created cluster:
2. Upload projekt2.zip to the master node.
3. Unzip and enter projekt2 directory.
4. Make all scripts executable:
   ```bash
   chmod +x *.sh
   ```

--- 

### 3. Prepare environment
1. Run following script to reset the environment and create Kafka topic:
   ```bash
   ./1-reset_and_create_topic.sh
   ```
2. Run script to re-create or create PostreSQL database:
   ```bash
   ./5-prepare_database.sh
   ```
   If above script fails, re-run it.
3. Download used files and upload static files to HDFS:
   ```bash
   ./2a-get_data.sh
   ```
4. Run Kafka input streaming script (preferably in a new, separate terminal):
   ```bash
   ./2b-stream_data.sh
   ```
5. Run Python application as YARN application using `spark-submit` (use `A` or `C` mode):
   ```bash
   ./4-process_data.sh A
   ```
   ```bash
   ./4-process_data.sh C
   ```
6. To query data from the database run script:
   ```bash
   ./6-query_data.sh
   ```

---