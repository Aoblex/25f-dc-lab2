download-dataset:
	bash scripts/download.sh 2010 01 02

sample-dataset: download-dataset
	python scripts/sampling.py \
	--input ./datasets \
	--output ./datasets/sample \
	--rows 20000 \
	--batch-size 1000

taxi-zone:
	@if [ ! -d "datasets/taxi_zones" ]; then \
		echo "Directory not found, downloading taxi_zones.zip..."; \
		curl -O https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip; \
		unzip taxi_zones.zip -d datasets/taxi_zones; \
		rm taxi_zones.zip; \
	else \
		echo "Directory datasets/taxi_zones already exists, skipping download."; \
	fi

put-dataset: taxi-zone download-dataset
	hdfs dfs -mkdir -p /input/nyc_taxi/
	hdfs dfs -put -f ./datasets/* /input/nyc_taxi/

dataset: sample-dataset put-dataset

# Original starter script targets
test: dataset
	hdfs dfs -mkdir -p /spark-logs
	spark-submit \
	--master yarn \
	--deploy-mode cluster \
	--name Taxi-Rideshare-Recommendation \
	--conf spark.eventLog.enabled=true \
	--conf spark.eventLog.dir=hdfs:///spark-logs \
	scripts/starter_script.py --taxi_path hdfs:///input/nyc_taxi/sample

# Partially optimized targets
coding-optimized-test: dataset
	hdfs dfs -mkdir -p /spark-logs
	spark-submit \
	--master yarn \
	--deploy-mode cluster \
	--name Taxi-Rideshare-Recommendation \
	--conf spark.eventLog.enabled=true \
	--conf spark.eventLog.dir=hdfs:///spark-logs \
	scripts/optimize_coding.py --taxi_path hdfs:///input/nyc_taxi/sample

communication-optimized-test: dataset
	hdfs dfs -mkdir -p /spark-logs
	spark-submit \
	--master yarn \
	--deploy-mode cluster \
	--name Taxi-Rideshare-Recommendation \
	--conf spark.eventLog.enabled=true \
	--conf spark.eventLog.dir=hdfs:///spark-logs \
	scripts/optimize_communication.py --taxi_path hdfs:///input/nyc_taxi/sample

configuration-optimized-test: dataset
	hdfs dfs -mkdir -p /spark-logs
	spark-submit \
	--master yarn \
	--deploy-mode cluster \
	--conf spark.sql.adaptive.enabled=true \
	--conf spark.sql.adaptive.coalescePartitions.enabled=true \
	--conf spark.sql.adaptive.skewJoin.enabled=true \
	--conf spark.sql.shuffle.partitions=200 \
	--conf spark.default.parallelism=24 \
	--conf spark.eventLog.enabled=true \
	--conf spark.eventLog.dir=hdfs:///spark-logs \
	--name Taxi-Rideshare-Recommendation-Optimized \
	scripts/starter_script.py --taxi_path hdfs:///input/nyc_taxi/sample

# Fully optimized version targets
optimized-test: dataset
	hdfs dfs -mkdir -p /spark-logs
	spark-submit \
	--master yarn \
	--deploy-mode cluster \
	--conf spark.sql.adaptive.enabled=true \
	--conf spark.sql.adaptive.coalescePartitions.enabled=true \
	--conf spark.sql.adaptive.skewJoin.enabled=true \
	--conf spark.sql.shuffle.partitions=200 \
	--conf spark.default.parallelism=24 \
	--conf spark.eventLog.enabled=true \
	--conf spark.eventLog.dir=hdfs:///spark-logs \
	--name Taxi-Rideshare-Recommendation-Optimized \
	scripts/optimized.py --taxi_path hdfs:///input/nyc_taxi/sample

compare-tests: test coding-optimized-test communication-optimized-test configuration-optimized-test optimized-test

full: dataset
	hdfs dfs -mkdir -p /spark-logs
	spark-submit \
	--master yarn \
	--deploy-mode cluster \
	--name Taxi-Rideshare-Recommendation \
	--conf spark.eventLog.enabled=true \
	--conf spark.eventLog.dir=hdfs:///spark-logs \
	scripts/starter_script.py --taxi_path hdfs:///input/nyc_taxi/

optimized-full: dataset
	hdfs dfs -mkdir -p /spark-logs
	spark-submit \
	--master yarn \
	--deploy-mode cluster \
	--conf spark.sql.adaptive.enabled=true \
	--conf spark.sql.adaptive.coalescePartitions.enabled=true \
	--conf spark.sql.adaptive.skewJoin.enabled=true \
	--conf spark.sql.shuffle.partitions=200 \
	--conf spark.default.parallelism=24 \
	--conf spark.eventLog.enabled=true \
	--conf spark.eventLog.dir=hdfs:///spark-logs \
	--name Taxi-Rideshare-Recommendation-Optimized \
	scripts/optimized.py --taxi_path hdfs:///input/nyc_taxi/

clean:
	rm -rf ./datasets/*
	hdfs dfs -rm -r -f /input/nyc_taxi/