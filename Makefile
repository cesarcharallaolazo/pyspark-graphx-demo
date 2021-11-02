build:
	cp ./spark/run_migration.py ./target
	cd ./spark && zip -x run_migration.py -r ../target/spark.zip .
