# Hudi Test Resources

## Generating Hudi Resources

Follow these steps to create the `hudi_non_part_cow` test table and utilize it for testing. `hudi_non_part_cow` resource is generated using `423` trino version.

### Start the Hudi environment

Execute the following command in the terminal to initiate the Hudi environment:

```shell
testing/bin/ptl env up --environment singlenode-hudi
```

### Generate Resources

* Open the `spark-sql` terminal and initiate the `spark-sql` shell in the `ptl-spark` container.
* Execute the following Spark SQL queries to create the `hudi_non_part_cow` table:

```
spark-sql> CREATE TABLE default.hudi_non_part_cow (
               id bigint,
               name string,
               ts bigint,
               dt string,
               hh string
           )
           USING hudi
           TBLPROPERTIES (
               type = 'cow',
               primaryKey = 'id',
               preCombineField = 'ts'
           )
           LOCATION 's3://test-bucket/hudi_non_part_cow';

spark-sql> INSERT INTO default.hudi_non_part_cow (id, name, ts, dt, hh) VALUES 
               (1, 'a1', 1000, '2021-12-09', '10'), 
               (2, 'a2', 2000, '2021-12-09', '11');
```

### Download Resources

Download the `hudi_non_part_cow` table from the MinIO client http://localhost:9001/buckets/test-bucket/browse.

### Use Resources

Unzip the downloaded `hudi_non_part_cow.zip`. Remove any unnecessary files obtained after unzipping to prepare the resource for testing.
