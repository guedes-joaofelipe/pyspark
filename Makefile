pyspark-notebook:
	docker run \
		--name pyspark \
		--rm \
		-it \
		-p 8888:8888 \
		-p 4040:4040 \
		--mount type=bind,source="$(shell pwd)",target=/workspace \
		--workdir /workspace \
		jupyter/pyspark-notebook

clean:
	rm -r data/output