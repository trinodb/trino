/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveBucketProperty;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TestingTypeManager;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static io.trino.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.trino.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V1;
import static io.trino.plugin.hudi.model.HudiTableType.COPY_ON_WRITE;
import static org.testng.Assert.assertEquals;

public class TestHudiPartitionManager
{
    private static final String SCHEMA_NAME = "schema";
    private static final String TABLE_NAME = "table";
    private static final String USER_NAME = "user";
    private static final String LOCATION = "somewhere/over/the/rainbow";
    private static final Column PARTITION_COLUMN = new Column("ds", HIVE_STRING, Optional.empty());
    private static final Column BUCKET_COLUMN = new Column("c1", HIVE_INT, Optional.empty());
    private static final Table TABLE = new Table(
            SCHEMA_NAME,
            TABLE_NAME,
            Optional.of(USER_NAME),
            MANAGED_TABLE.name(),
            new Storage(
                    fromHiveStorageFormat(PARQUET),
                    Optional.of(LOCATION),
                    Optional.of(new HiveBucketProperty(
                            ImmutableList.of(BUCKET_COLUMN.getName()),
                            BUCKETING_V1,
                            2,
                            ImmutableList.of())),
                    false,
                    ImmutableMap.of()),
            ImmutableList.of(BUCKET_COLUMN),
            ImmutableList.of(PARTITION_COLUMN),
            ImmutableMap.of(),
            Optional.empty(),
            Optional.empty(),
            OptionalLong.empty());
    private static final List<String> PARTITIONS = ImmutableList.of("ds=2019-07-23", "ds=2019-08-23");
    private final HudiPartitionManager hudiPartitionManager = new HudiPartitionManager(new TestingTypeManager());
    private final TestingExtendedHiveMetastore metastore = new TestingExtendedHiveMetastore(TABLE, PARTITIONS);

    @Test
    public void testParseValuesAndFilterPartition()
    {
        HudiTableHandle tableHandle = new HudiTableHandle(
                SCHEMA_NAME,
                TABLE_NAME,
                TABLE.getStorage().getLocation(),
                COPY_ON_WRITE,
                TupleDomain.all(),
                TupleDomain.all());
        List<String> actualPartitions = hudiPartitionManager.getEffectivePartitions(
                tableHandle,
                metastore);
        assertEquals(actualPartitions, PARTITIONS);
    }
}
