# timeseries

Comparision of [duckdb](https://duckdb.org/), [polars](https://pola.rs/) and [pandas](https://pandas.pydata.org/) regarding timeseries.

Generate data files:

```
make generate
```

Compare execution times:

```
make polars pandas duckdb
```