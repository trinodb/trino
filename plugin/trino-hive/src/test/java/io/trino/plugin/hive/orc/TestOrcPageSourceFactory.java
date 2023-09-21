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
package io.trino.plugin.hive.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.filesystem.hdfs.HdfsFileSystemFactory;
import io.trino.plugin.hive.AcidInfo;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HivePageSourceFactory;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.tpch.Nation;
import io.trino.tpch.NationColumn;
import io.trino.tpch.NationGenerator;
import org.testng.annotations.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.function.LongPredicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.Resources.getResource;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_STATS;
import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.tpch.NationColumn.COMMENT;
import static io.trino.tpch.NationColumn.NAME;
import static io.trino.tpch.NationColumn.NATION_KEY;
import static io.trino.tpch.NationColumn.REGION_KEY;
import static java.util.Collections.nCopies;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.TABLE_IS_TRANSACTIONAL;
import static org.apache.hadoop.hive.ql.io.AcidUtils.deleteDeltaSubdir;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestOrcPageSourceFactory
{
    private static final Map<NationColumn, Integer> ALL_COLUMNS = ImmutableMap.of(NATION_KEY, 0, NAME, 1, REGION_KEY, 2, COMMENT, 3);
    private static final HivePageSourceFactory PAGE_SOURCE_FACTORY = new OrcPageSourceFactory(
            new OrcReaderConfig(),
            new HdfsFileSystemFactory(HDFS_ENVIRONMENT, HDFS_FILE_SYSTEM_STATS),
            new FileFormatDataSourceStats(),
            new HiveConfig());

    @Test
    public void testFullFileRead()
    {
        assertRead(ImmutableMap.of(NATION_KEY, 0, NAME, 1, REGION_KEY, 2, COMMENT, 3), OptionalLong.empty(), Optional.empty(), nationKey -> false);
    }

    @Test
    public void testSingleColumnRead()
    {
        assertRead(ImmutableMap.of(REGION_KEY, ALL_COLUMNS.get(REGION_KEY)), OptionalLong.empty(), Optional.empty(), nationKey -> false);
    }

    /**
     * tests file stats based pruning works fine
     */
    @Test
    public void testFullFileSkipped()
    {
        assertRead(ALL_COLUMNS, OptionalLong.of(100L), Optional.empty(), nationKey -> false);
    }

    /**
     * Tests stripe stats and row groups stats based pruning works fine
     */
    @Test
    public void testSomeStripesAndRowGroupRead()
    {
        assertRead(ALL_COLUMNS, OptionalLong.of(5L), Optional.empty(), nationKey -> false);
    }

    @Test
    public void testDeletedRows()
    {
        Location partitionLocation = Location.of(getResource("nation_delete_deltas").toString());
        Optional<AcidInfo> acidInfo = AcidInfo.builder(partitionLocation)
                .addDeleteDelta(partitionLocation.appendPath(deleteDeltaSubdir(3L, 3L, 0)))
                .addDeleteDelta(partitionLocation.appendPath(deleteDeltaSubdir(4L, 4L, 0)))
                .build();

        assertRead(ALL_COLUMNS, OptionalLong.empty(), acidInfo, nationKey -> nationKey == 5 || nationKey == 19);
    }

    @Test
    public void testReadWithAcidVersionValidationHive3()
            throws Exception
    {
        File tableFile = new File(getResource("acid_version_validation/acid_version_hive_3/00000_0").toURI());
        Location tablePath = Location.of(tableFile.getParentFile().toURI().toString());

        Optional<AcidInfo> acidInfo = AcidInfo.builder(tablePath)
                .setOrcAcidVersionValidated(false)
                .build();

        List<Nation> result = readFile(Map.of(), OptionalLong.empty(), acidInfo, tableFile.getPath(), 625);
        assertEquals(result.size(), 1);
    }

    @Test
    public void testReadWithAcidVersionValidationNoVersionInMetadata()
            throws Exception
    {
        File tableFile = new File(getResource("acid_version_validation/no_orc_acid_version_in_metadata/00000_0").toURI());
        Location tablePath = Location.of(tableFile.getParentFile().toURI().toString());

        Optional<AcidInfo> acidInfo = AcidInfo.builder(tablePath)
                .setOrcAcidVersionValidated(false)
                .build();

        assertThatThrownBy(() -> readFile(Map.of(), OptionalLong.empty(), acidInfo, tableFile.getPath(), 730))
                .hasMessageMatching("Hive transactional tables are supported since Hive 3.0. Expected `hive.acid.version` in ORC metadata" +
                        " in .*/acid_version_validation/no_orc_acid_version_in_metadata/00000_0 to be >=2 but was <empty>." +
                        " If you have upgraded from an older version of Hive, make sure a major compaction has been run at least once after the upgrade.");
    }

    @Test
    public void testFullFileReadOriginalFilesTable()
            throws Exception
    {
        File tableFile = new File(getResource("fullacidNationTableWithOriginalFiles/000000_0").toURI());
        Location tablePath = Location.of(tableFile.toURI().toString()).parentDirectory();

        AcidInfo acidInfo = AcidInfo.builder(tablePath)
                .addDeleteDelta(tablePath.appendPath(deleteDeltaSubdir(10000001, 10000001, 0)))
                .addOriginalFile(tablePath.appendPath("000000_0"), 1780, 0)
                .setOrcAcidVersionValidated(true)
                .buildWithRequiredOriginalFiles(0);

        List<Nation> expected = expectedResult(OptionalLong.empty(), nationKey -> nationKey == 24, 1);
        List<Nation> result = readFile(ALL_COLUMNS, OptionalLong.empty(), Optional.of(acidInfo), tablePath + "/000000_0", 1780);

        assertEquals(result.size(), expected.size());
        int deletedRowKey = 24;
        String deletedRowNameColumn = "UNITED STATES";
        assertFalse(result.stream().anyMatch(acidNationRow -> acidNationRow.getName().equals(deletedRowNameColumn) && acidNationRow.getNationKey() == deletedRowKey),
                "Deleted row shouldn't be present in the result");
    }

    private static void assertRead(Map<NationColumn, Integer> columns, OptionalLong nationKeyPredicate, Optional<AcidInfo> acidInfo, LongPredicate deletedRows)
    {
        List<Nation> actual = readFile(columns, nationKeyPredicate, acidInfo);

        List<Nation> expected = expectedResult(nationKeyPredicate, deletedRows, 1000);

        assertEqualsByColumns(columns.keySet(), actual, expected);
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

    private static List<Nation> readFile(Map<NationColumn, Integer> columns, OptionalLong nationKeyPredicate, Optional<AcidInfo> acidInfo)
    {
        // This file has the contains the TPC-H nation table which each row repeated 1000 times
        try {
            File testFile = new File(getResource("nationFile25kRowsSortedOnNationKey/bucket_00000").toURI());
            return readFile(columns, nationKeyPredicate, acidInfo, testFile.toURI().getPath(), testFile.length());
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<Nation> readFile(Map<NationColumn, Integer> columns, OptionalLong nationKeyPredicate, Optional<AcidInfo> acidInfo, String filePath, long fileSize)
    {
        TupleDomain<HiveColumnHandle> tupleDomain = TupleDomain.all();
        if (nationKeyPredicate.isPresent()) {
            tupleDomain = TupleDomain.withColumnDomains(ImmutableMap.of(toHiveColumnHandle(NATION_KEY, 0), Domain.singleValue(INTEGER, nationKeyPredicate.getAsLong())));
        }

        List<HiveColumnHandle> columnHandles = columns.entrySet().stream()
                .map(entry -> toHiveColumnHandle(entry.getKey(), entry.getValue()))
                .collect(toImmutableList());

        List<String> columnNames = columnHandles.stream()
                .map(HiveColumnHandle::getName)
                .collect(toImmutableList());

        Optional<ReaderPageSource> pageSourceWithProjections = PAGE_SOURCE_FACTORY.createPageSource(
                SESSION,
                Location.of(filePath),
                0,
                fileSize,
                fileSize,
                createSchema(),
                columnHandles,
                tupleDomain,
                acidInfo,
                OptionalInt.empty(),
                false,
                NO_ACID_TRANSACTION);

        checkArgument(pageSourceWithProjections.isPresent());
        checkArgument(pageSourceWithProjections.get().getReaderColumns().isEmpty(),
                "projected columns not expected here");

        ConnectorPageSource pageSource = pageSourceWithProjections.get().get();

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
        Type trinoType;
        switch (nationColumn.getType().getBase()) {
            case IDENTIFIER:
                trinoType = BIGINT;
                break;
            case VARCHAR:
                trinoType = VARCHAR;
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + nationColumn.getType().getBase());
        }

        return createBaseColumn(
                nationColumn.getColumnName(),
                hiveColumnIndex,
                toHiveType(trinoType),
                trinoType,
                REGULAR,
                Optional.empty());
    }

    private static Properties createSchema()
    {
        Properties schema = new Properties();
        schema.setProperty(SERIALIZATION_LIB, ORC.getSerde());
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
