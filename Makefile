data:
	mkdir -p out
	python data.py

duckdb:
	python duckdb-perf.py

polars:
	python polars-perf.py

pandas:
	python pandas-perf.py

clean:
	rm -rf out