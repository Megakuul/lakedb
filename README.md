# LakeDB


LakeDB is a simple realtime analytics engine running on s3 using parquet.

```go

```

## Limitations

> [!WARNING]
> LakeDB is slow. It was written due to the lack of alternatives.
> With this being said, the speed will eventually improve in the future, however, for now engines like DuckDB will beat this by magnitudes in every single performance aspect.


- LakeDB performs around 100x-1000x slower in most operations compared to DuckDB.
- High cardinality grouping is not supported.
