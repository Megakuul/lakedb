# LakeDB


LakeDB is a simple realtime analytics engine running on s3 using parquet.

```go

```

## Ingestion

```go

```

> [!TIP]
> It is absolutely permitted and often required to commit only a *few* ingested entries (e.g. in serverless scenarios or to make the data available quickly).
> To avoid "paruqet-file-churn" on the long run, use the compactor in the next section to compact the emitted files into large parquet chunks.

## Compaction

```go

```

> [!NOTE]
> Compaction and ingestion operations are fully atomic usign optimistic locking.
> You will receive a `lake.ErrOptimisticLock` in case the lock is interrupted.

## Limitations

> [!WARNING]
> LakeDB is slow. It was written due to the lack of alternatives.
> With this being said, the speed will eventually improve in the future, however, for now engines like DuckDB will beat this by magnitudes in every single performance aspect.


- LakeDB performs around 100x-1000x slower in most operations compared to DuckDB.
- High cardinality grouping is not supported.
