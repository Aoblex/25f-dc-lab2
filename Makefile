download-dataset:
	bash scripts/download.sh 2024 01 02 03

put-dataset:
	hdfs dfs -mkdir -p /input/nyc_taxi/
	hdfs dfs -put -f ./datasets/* /input/nyc_taxi/

dataset: download-dataset put-dataset