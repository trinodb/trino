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
package io.prestosql.plugin.hive.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.hive.AcidInfo;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HivePageSourceFactory;
import io.prestosql.plugin.hive.HivePageSourceFactory.ReaderPageSourceWithProjections;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import io.prestosql.tpch.Nation;
import io.prestosql.tpch.NationColumn;
import io.prestosql.tpch.NationGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongPredicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.prestosql.plugin.hive.HiveStorageFormat.ORC;
import static io.prestosql.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.prestosql.plugin.hive.HiveTestUtils.SESSION;
import static io.prestosql.plugin.hive.HiveType.toHiveType;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.tpch.NationColumn.COMMENT;
import static io.prestosql.tpch.NationColumn.NAME;
import static io.prestosql.tpch.NationColumn.NATION_KEY;
import static io.prestosql.tpch.NationColumn.REGION_KEY;
import static java.util.Collections.nCopies;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.TABLE_IS_TRANSACTIONAL;
import static org.apache.hadoop.hive.ql.io.AcidUtils.deleteDeltaSubdir;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestOrcPageSourceFactory
{
    // This file has the contains the TPC-H nation table which each row repeated 1000 times
    private static final File TEST_FILE = new File(TestOrcPageSourceFactory.class.getClassLoader().getResource("nationFile25kRowsSortedOnNationKey/bucket_00000").getPath());
    private static final HivePageSourceFactory PAGE_SOURCE_FACTORY = new OrcPageSourceFactory(
            new OrcReaderConfig(),
            HDFS_ENVIRONMENT,
            new FileFormatDataSourceStats(),
            new HiveConfig());

    @Test
    public void testFullFileRead()
    {
        assertRead(ImmutableSet.copyOf(NationColumn.values()), OptionalLong.empty(), Optional.empty(), nationKey -> false);
    }

    @Test
    public void testSingleColumnRead()
    {
        assertRead(ImmutableSet.of(REGION_KEY), OptionalLong.empty(), Optional.empty(), nationKey -> false);
    }

    /**
     * tests file stats based pruning works fine
     */
    @Test
    public void testFullFileSkipped()
    {
        assertRead(ImmutableSet.copyOf(NationColumn.values()), OptionalLong.of(100L), Optional.empty(), nationKey -> false);
    }

    /**
     * Tests stripe stats and row groups stats based pruning works fine
     */
    @Test
    public void testSomeStripesAndRowGroupRead()
    {
        assertRead(ImmutableSet.copyOf(NationColumn.values()), OptionalLong.of(5L), Optional.empty(), nationKey -> false);
    }

    @Test
    public void testDeletedRows()
    {
        Path partitionLocation = new Path(getClass().getClassLoader().getResource("nation_delete_deltas") + "/");
        Optional<AcidInfo> acidInfo = AcidInfo.builder(partitionLocation)
                .addDeleteDelta(new Path(partitionLocation, deleteDeltaSubdir(3L, 3L, 0)))
                .addDeleteDelta(new Path(partitionLocation, deleteDeltaSubdir(4L, 4L, 0)))
                .build();

        assertRead(ImmutableSet.copyOf(NationColumn.values()), OptionalLong.empty(), acidInfo, nationKey -> nationKey == 5 || nationKey == 19);
    }

    @Test
    public void testFullFileReadOriginalFilesTable()
    {
        File tableFile = new File(TestOrcPageSourceFactory.class.getClassLoader().getResource("fullacidNationTableWithOriginalFiles/000000_0").getPath());
        String tablePath = tableFile.getParent();

        AcidInfo acidInfo = AcidInfo.builder(new Path(tablePath))
                .addDeleteDelta(new Path(tablePath, deleteDeltaSubdir(10000001, 10000001, 0)))
                .addOriginalFile(new Path(tablePath, "000000_0"), 1780, 0)
                .buildWithRequiredOriginalFiles(0);

        List<Nation> expected = expectedResult(OptionalLong.empty(), nationKey -> nationKey == 24, 1);
        List<Nation> result = readFile(ImmutableSet.copyOf(NationColumn.values()), OptionalLong.empty(), Optional.of(acidInfo), tablePath + "/000000_0", 1780);

        assertEquals(result.size(), expected.size());
        int deletedRowKey = 24;
        String deletedRowNameColumn = "UNITED STATES";
        assertFalse(result.stream().anyMatch(acidNationRow -> acidNationRow.getName().equals(deletedRowNameColumn) && acidNationRow.getNationKey() == deletedRowKey),
                "Deleted row shouldn't be present in the result");
    }

    private static void assertRead(Set<NationColumn> columns, OptionalLong nationKeyPredicate, Optional<AcidInfo> acidInfo, LongPredicate deletedRows)
    {
        List<Nation> actual = readFile(columns, nationKeyPredicate, acidInfo);

        List<Nation> expected = expectedResult(nationKeyPredicate, deletedRows, 1000);

        assertEqualsByColumns(columns, actual, expected);
    }

    private static List<Nation> expectedResult(OptionalLong nationKeyPredicate, LongPredicate deletedRows, int replicationFactor)
    {
        List<Nation> expected = new ArrayList<>();
        for (Nation nation : ImmutableList.copyOf(new NationGenerator().iterator())) {
            if (nationKeyPredicate.isPresent() && nationKeyPredicate.getAsLong() != nation.getNationKey()) {
                continue;
            }
            if (deletedRows.test(nation.getNationKey())) {
                continue;
            }
            expected.addAll(nCopies(replicationFactor, nation));
        }
        return expected;
    }

    private static List<Nation> readFile(Set<NationColumn> columns, OptionalLong nationKeyPredicate, Optional<AcidInfo> acidInfo)
    {
        return readFile(columns, nationKeyPredicate, acidInfo, TEST_FILE.toURI().getPath(), TEST_FILE.length());
    }

    private static List<Nation> readFile(Set<NationColumn> columns, OptionalLong nationKeyPredicate, Optional<AcidInfo> acidInfo, String filePath, long fileSize)
    {
        TupleDomain<HiveColumnHandle> tupleDomain = TupleDomain.all();
        if (nationKeyPredicate.isPresent()) {
            tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(toHiveColumnHandle(NATION_KEY, 0), Domain.singleValue(INTEGER, nationKeyPredicate.getAsLong())));
        }

        AtomicInteger atomicInteger = new AtomicInteger(0);
        List<HiveColumnHandle> columnHandles = columns.stream()
                .map(nationColumn -> toHiveColumnHandle(nationColumn, atomicInteger.getAndIncrement()))
                .collect(toImmutableList());

        List<String> columnNames = columnHandles.stream()
                .map(HiveColumnHandle::getName)
                .collect(toImmutableList());

        Optional<ReaderPageSourceWithProjections> pageSourceWithProjections = PAGE_SOURCE_FACTORY.createPageSource(
                new JobConf(new Configuration(false)),
                SESSION,
                new Path(filePath),
                0,
                fileSize,
                fileSize,
                createSchema(),
                columnHandles,
                tupleDomain,
                acidInfo);

        checkArgument(pageSourceWithProjections.isPresent());
        checkArgument(pageSourceWithProjections.get().getProjectedReaderColumns().isEmpty(),
                "projected columns not expected here");

        ConnectorPageSource pageSource = pageSourceWithProjections.get().getConnectorPageSource();

        int nationKeyColumn = columnNames.indexOf("n_nationkey");
        int nameColumn = columnNames.indexOf("n_name");
        int regionKeyColumn = columnNames.indexOf("n_regionkey");
        int commentColumn = columnNames.indexOf("n_comment");

        ImmutableList.Builder<Nation> rows = ImmutableList.builder();
        while (!pageSource.isFinished()) {
            Page page = pageSource.getNextPage();
            if (page == null) {
                continue;
            }

            page = page.getLoadedPage();
            for (int position = 0; position < page.getPositionCount(); position++) {
                long nationKey = -42;
                if (nationKeyColumn >= 0) {
                    nationKey = BIGINT.getLong(page.getBlock(nationKeyColumn), position);
                }

                String name = "<not read>";
                if (nameColumn >= 0) {
                    name = VARCHAR.getSlice(page.getBlock(nameColumn), position).toStringUtf8();
                }

                long regionKey = -42;
                if (regionKeyColumn >= 0) {
                    regionKey = BIGINT.getLong(page.getBlock(regionKeyColumn), position);
                }

                String comment = "<not read>";
                if (commentColumn >= 0) {
                    comment = VARCHAR.getSlice(page.getBlock(commentColumn), position).toStringUtf8();
                }

                rows.add(new Nation(position, nationKey, name, regionKey, comment));
            }
        }
        return rows.build();
    }

    private static HiveColumnHandle toHiveColumnHandle(NationColumn nationColumn, int hiveColumnIndex)
    {
        Type prestoType;
        switch (nationColumn.getType().getBase()) {
            case IDENTIFIER:
                prestoType = BIGINT;
                break;
            case VARCHAR:
                prestoType = VARCHAR;
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + nationColumn.getType().getBase());
        }

        return createBaseColumn(
                nationColumn.getColumnName(),
                hiveColumnIndex,
                toHiveType(prestoType),
                prestoType,
                REGULAR,
                Optional.empty());
    }

    private static Properties createSchema()
    {
        Properties schema = new Properties();
        schema.setProperty(SERIALIZATION_LIB, ORC.getSerDe());
        schema.setProperty(FILE_INPUT_FORMAT, ORC.getInputFormat());
        schema.setProperty(TABLE_IS_TRANSACTIONAL, "true");
        return schema;
    }

    private static void assertEqualsByColumns(Set<NationColumn> columns, List<Nation> actualRows, List<Nation> expectedRows)
    {
        assertEquals(actualRows.size(), expectedRows.size(), "row count");
        for (int i = 0; i < actualRows.size(); i++) {
            Nation actual = actualRows.get(i);
            Nation expected = expectedRows.get(i);
            assertEquals(actual.getNationKey(), columns.contains(NATION_KEY) ? expected.getNationKey() : -42);
            assertEquals(actual.getName(), columns.contains(NAME) ? expected.getName() : "<not read>");
            assertEquals(actual.getRegionKey(), columns.contains(REGION_KEY) ? expected.getRegionKey() : -42);
            assertEquals(actual.getComment(), columns.contains(COMMENT) ? expected.getComment() : "<not read>");
        }
    }
}
