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
package io.prestosql.plugin.hive.acid;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.hive.DeleteDeltaLocations;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HivePageSourceProvider;
import io.prestosql.plugin.hive.HiveSplit;
import io.prestosql.plugin.hive.HiveTableHandle;
import io.prestosql.plugin.hive.HiveTransactionHandle;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.HiveTypeTranslator;
import io.prestosql.plugin.hive.orc.OrcPageSourceFactory;
import io.prestosql.plugin.hive.orc.acid.AcidOrcPageSourceFactory;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.prestosql.plugin.hive.HiveTestUtils.SESSION;
import static io.prestosql.plugin.hive.HiveTestUtils.TYPE_MANAGER;
import static io.prestosql.plugin.hive.acid.AcidNationRow.getExpectedResult;
import static io.prestosql.plugin.hive.acid.AcidNationRow.readFileCols;
import static io.prestosql.plugin.hive.acid.AcidPageProcessorProvider.CONFIG;
import static io.prestosql.plugin.hive.acid.AcidPageProcessorProvider.ORC_CONFIG;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_DDL;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.testng.Assert.assertTrue;

public class TestHivePageSourceProvider
{
    private List<String> columnNames = ImmutableList.of("n_nationkey", "n_name", "n_regionkey", "n_comment");
    private List<Type> columnTypes = ImmutableList.of(INTEGER, VARCHAR, INTEGER, VARCHAR);

    @Test
    public void testAcidTableWithoutDeletedRows()
            throws IOException
    {
        OrcPageSourceFactory orcPageSourceFactory = new OrcPageSourceFactory(TYPE_MANAGER, ORC_CONFIG, HDFS_ENVIRONMENT, new FileFormatDataSourceStats());
        AcidOrcPageSourceFactory acidOrcPageSourceFactory = new AcidOrcPageSourceFactory(TYPE_MANAGER, CONFIG, ORC_CONFIG, HDFS_ENVIRONMENT, new FileFormatDataSourceStats(), orcPageSourceFactory);
        HivePageSourceProvider pageSourceProvider = new HivePageSourceProvider(CONFIG, HDFS_ENVIRONMENT, ImmutableSet.of(), ImmutableSet.of(acidOrcPageSourceFactory), TYPE_MANAGER);

        HiveSplit split = createHiveSplit(Optional.empty());
        HiveTableHandle table = new HiveTableHandle("test", "test", ImmutableMap.of(), ImmutableList.of(), Optional.empty());
        ConnectorPageSource pageSource = pageSourceProvider.createPageSource(new HiveTransactionHandle(), SESSION, split, table, getColumnHandles());
        List<AcidNationRow> rows = readFileCols(pageSource, columnNames, columnTypes, true);

        assertTrue(rows.size() == 25000, "Unexpected number of rows read: " + rows.size());

        List<AcidNationRow> expected = getExpectedResult(Optional.empty(), Optional.empty(), Optional.empty());
        assertTrue(Objects.equals(expected, rows));
    }

    @Test
    public void testAcidTableWithDeletedRows()
            throws IOException
    {
        OrcPageSourceFactory orcPageSourceFactory = new OrcPageSourceFactory(TYPE_MANAGER, ORC_CONFIG, HDFS_ENVIRONMENT, new FileFormatDataSourceStats());
        AcidOrcPageSourceFactory acidOrcPageSourceFactory = new AcidOrcPageSourceFactory(TYPE_MANAGER, CONFIG, ORC_CONFIG, HDFS_ENVIRONMENT, new FileFormatDataSourceStats(), orcPageSourceFactory);
        HivePageSourceProvider pageSourceProvider = new HivePageSourceProvider(CONFIG, HDFS_ENVIRONMENT, ImmutableSet.of(), ImmutableSet.of(acidOrcPageSourceFactory), TYPE_MANAGER);

        HiveSplit split = createHiveSplit(Optional.of(AcidNationRow.getDeletaDeltaLocations()));
        HiveTableHandle table = new HiveTableHandle("test", "test", ImmutableMap.of(), ImmutableList.of(), Optional.empty());
        ConnectorPageSource pageSource = pageSourceProvider.createPageSource(new HiveTransactionHandle(), SESSION, split, table, getColumnHandles());
        List<AcidNationRow> rows = readFileCols(pageSource, columnNames, columnTypes, true);

        assertTrue(rows.size() == 23000, "Unexpected number of rows read: " + rows.size());

        List<AcidNationRow> expected = getExpectedResult(Optional.empty(), Optional.empty(), Optional.of(ImmutableList.of(5, 19)));
        assertTrue(Objects.equals(expected, rows));
    }

    private List<ColumnHandle> getColumnHandles()
    {
        ImmutableList.Builder<ColumnHandle> builder = ImmutableList.builder();
        for (int i = 0; i < columnNames.size(); i++) {
            builder.add(new HiveColumnHandle(
                    columnNames.get(i),
                    HiveType.toHiveType(new HiveTypeTranslator(), columnTypes.get(i)),
                    columnTypes.get(i).getTypeSignature(),
                    i,
                    REGULAR,
                    Optional.empty()));
        }
        return builder.build();
    }

    private HiveSplit createHiveSplit(Optional<DeleteDeltaLocations> deleteDeltaLocations)
            throws IOException
    {
        Configuration config = new JobConf(new Configuration(false));
        config.set("fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
        Path path = new Path(Thread.currentThread().getContextClassLoader().getResource("nationFile25kRowsSortedOnNationKey/bucket_00000").getPath());
        FileSystem fs = path.getFileSystem(config);
        FileStatus fileStatus = fs.getFileStatus(path);
        return new HiveSplit("default",
                "nation_acid",
                "UNPARTITIONED",
                path.toString(),
                0,
                fileStatus.getLen(),
                fileStatus.getLen(),
                Instant.now().toEpochMilli(),
                createSchema(),
                ImmutableList.of(),
                ImmutableList.of(),
                OptionalInt.empty(),
                false,
                ImmutableMap.of(),
                Optional.empty(),
                false,
                deleteDeltaLocations);
    }

    private Properties createSchema()
    {
        Properties schema = new Properties();
        schema.put(META_TABLE_COLUMNS, "n_nationkey,n_name,n_regionkey,n_comment");
        schema.put(META_TABLE_COLUMN_TYPES, "int:string:int:string");
        schema.put("transactional_properties", "default");
        schema.put(SERIALIZATION_DDL, "struct nation_acid { i32 n_nationkey, string n_name, i32 n_regionkey, string n_comment}");
        schema.put(SERIALIZATION_LIB, "org.apache.hadoop.hive.ql.io.orc.OrcSerde");
        schema.put("transactional", "true");
        return schema;
    }
}
