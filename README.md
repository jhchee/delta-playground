# delta-scd-examples
## How to setup locally
1. Navigate to `docker` folder `cd docker`
2. Run the docker containers `docker compose -p delta-scd-example up -d`
3. Spark-submit command
```bash
/opt/spark-3.3.2-bin-hadoop3/bin/spark-submit \
--master local --deploy-mode client \
--class github.jhchee.deltaplayground.WriteScdType1 \
--name WriteScdType1 \
--packages io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.2.2 \
./target/delta-playground-0.0.1-SNAPSHOT.jar
```

## Example walkthrough


