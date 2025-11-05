download-dataset:
	bash scripts/download.sh 2010 01 02 03

sample-dataset: download-dataset
	python scripts/sampling.py \
	--input ./datasets \
	--output ./datasets/sample \
	--rows 1000 \
	--batch-size 100

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

test: dataset
	spark-submit \
	--master yarn \
	--deploy-mode cluster \
	--name Taxi-Rideshare-Recommendation \
	scripts/main.py --taxi_path hdfs:///input/nyc_taxi/sample

clean:
	rm -rf ./datasets/*
	hdfs dfs -rm -r -f /input/nyc_taxi/