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
import com.google.common.io.Resources;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
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
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.Type;
import io.trino.tpch.Nation;
import io.trino.tpch.NationColumn;
import io.trino.tpch.NationGenerator;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.LongPredicate;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.Resources.getResource;
import static io.trino.hive.thrift.metastore.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.HiveTableProperties.TRANSACTIONAL;
import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.util.SerdeConstants.SERIALIZATION_LIB;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.tpch.NationColumn.COMMENT;
import static io.trino.tpch.NationColumn.NAME;
import static io.trino.tpch.NationColumn.NATION_KEY;
import static io.trino.tpch.NationColumn.REGION_KEY;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestOrcPageSourceFactory
{
    private static final Map<NationColumn, Integer> ALL_COLUMNS = ImmutableMap.of(NATION_KEY, 0, NAME, 1, REGION_KEY, 2, COMMENT, 3);

    @Test
    public void testFullFileRead()
            throws IOException
    {
        assertRead(ImmutableMap.of(NATION_KEY, 0, NAME, 1, REGION_KEY, 2, COMMENT, 3), OptionalLong.empty(), Optional.empty(), nationKey -> false);
    }

    @Test
    public void testSingleColumnRead()
            throws IOException
    {
        assertRead(ImmutableMap.of(REGION_KEY, ALL_COLUMNS.get(REGION_KEY)), OptionalLong.empty(), Optional.empty(), nationKey -> false);
    }

    /**
     * tests file stats based pruning works fine
     */
    @Test
    public void testFullFileSkipped()
            throws IOException
    {
        assertRead(ALL_COLUMNS, OptionalLong.of(100L), Optional.empty(), nationKey -> false);
    }

    /**
     * Tests stripe stats and row groups stats based pruning works fine
     */
    @Test
    public void testSomeStripesAndRowGroupRead()
            throws IOException
    {
        assertRead(ALL_COLUMNS, OptionalLong.of(5L), Optional.empty(), nationKey -> false);
    }

    @Test
    public void testDeletedRows()
            throws IOException
    {
        TrinoFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        Location fileLocation = copyResource(fileSystemFactory, "nationFile25kRowsSortedOnNationKey/bucket_00000");
        long fileLength = fileSystemFactory.create(ConnectorIdentity.ofUser("test")).newInputFile(fileLocation).length();

        Location deleteFile3 = copyResource(fileSystemFactory, "nation_delete_deltas/delete_delta_0000003_0000003_0000/bucket_00000");
        Location deleteFile4 = copyResource(fileSystemFactory, "nation_delete_deltas/delete_delta_0000004_0000004_0000/bucket_00000");
        Optional<AcidInfo> acidInfo = AcidInfo.builder(deleteFile3.parentDirectory().parentDirectory())
                .addDeleteDelta(deleteFile3.parentDirectory())
                .addDeleteDelta(deleteFile4.parentDirectory())
                .build();

        List<Nation> actual = readFile(fileSystemFactory, ALL_COLUMNS, OptionalLong.empty(), acidInfo, fileLocation, fileLength);

        List<Nation> expected = expectedResult(OptionalLong.empty(), nationKey -> nationKey == 5 || nationKey == 19, 1000);
        assertEqualsByColumns(ALL_COLUMNS.keySet(), actual, expected);
    }

    @Test
    public void testReadWithAcidVersionValidationHive3()
            throws Exception
    {
        TrinoFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        Location fileLocation = copyResource(fileSystemFactory, "acid_version_validation/acid_version_hive_3/00000_0");

        Optional<AcidInfo> acidInfo = AcidInfo.builder(fileLocation.parentDirectory())
                .setOrcAcidVersionValidated(false)
                .build();

        List<Nation> result = readFile(fileSystemFactory, Map.of(), OptionalLong.empty(), acidInfo, fileLocation, 625);
        assertThat(result.size()).isEqualTo(1);
    }

    @Test
    public void testReadWithAcidVersionValidationNoVersionInMetadata()
            throws Exception
    {
        TrinoFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        Location fileLocation = copyResource(fileSystemFactory, "acid_version_validation/no_orc_acid_version_in_metadata/00000_0");

        Optional<AcidInfo> acidInfo = AcidInfo.builder(fileLocation.parentDirectory())
                .setOrcAcidVersionValidated(false)
                .build();

        assertThatThrownBy(() -> readFile(fileSystemFactory, Map.of(), OptionalLong.empty(), acidInfo, fileLocation, 730))
                .hasMessageMatching("Hive transactional tables are supported since Hive 3.0. Expected `hive.acid.version` in ORC metadata" +
                        " in .*/acid_version_validation/no_orc_acid_version_in_metadata/00000_0 to be >=2 but was <empty>." +
                        " If you have upgraded from an older version of Hive, make sure a major compaction has been run at least once after the upgrade.");
    }

    @Test
    public void testFullFileReadOriginalFilesTable()
            throws Exception
    {
        TrinoFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        Location fileLocation = copyResource(fileSystemFactory, "fullacidNationTableWithOriginalFiles/000000_0");
        Location deleteDeltaLocation = copyResource(fileSystemFactory, "fullacidNationTableWithOriginalFiles/delete_delta_10000001_10000001_0000/bucket_00000");
        Location tablePath = fileLocation.parentDirectory();

        AcidInfo acidInfo = AcidInfo.builder(tablePath)
                .addDeleteDelta(deleteDeltaLocation.parentDirectory())
                .addOriginalFile(fileLocation, 1780, 0)
                .setOrcAcidVersionValidated(true)
                .buildWithRequiredOriginalFiles(0);

        List<Nation> expected = expectedResult(OptionalLong.empty(), nationKey -> nationKey == 24, 1);
        List<Nation> result = readFile(fileSystemFactory, ALL_COLUMNS, OptionalLong.empty(), Optional.of(acidInfo), fileLocation, 1780);

        assertThat(result.size()).isEqualTo(expected.size());
        int deletedRowKey = 24;
        String deletedRowNameColumn = "UNITED STATES";
        assertThat(result.stream().anyMatch(acidNationRow -> acidNationRow.name().equals(deletedRowNameColumn) && acidNationRow.nationKey() == deletedRowKey))
                .describedAs("Deleted row shouldn't be present in the result")
                .isFalse();
    }

    private static void assertRead(Map<NationColumn, Integer> columns, OptionalLong nationKeyPredicate, Optional<AcidInfo> acidInfo, LongPredicate deletedRows)
            throws IOException
    {
        List<Nation> actual = readFile(columns, nationKeyPredicate, acidInfo);

        List<Nation> expected = expectedResult(nationKeyPredicate, deletedRows, 1000);

        assertEqualsByColumns(columns.keySet(), actual, expected);
    }

    private static List<Nation> expectedResult(OptionalLong nationKeyPredicate, LongPredicate deletedRows, int replicationFactor)
    {
        List<Nation> expected = new ArrayList<>();
        for (Nation nation : ImmutableList.copyOf(new NationGenerator().iterator())) {
            if (nationKeyPredicate.isPresent() && nationKeyPredicate.getAsLong() != nation.nationKey()) {
                continue;
            }
            if (deletedRows.test(nation.nationKey())) {
                continue;
            }
            expected.addAll(nCopies(replicationFactor, nation));
        }
        return expected;
    }

    private static List<Nation> readFile(Map<NationColumn, Integer> columns, OptionalLong nationKeyPredicate, Optional<AcidInfo> acidInfo)
            throws IOException
    {
        // This file has the contains the TPC-H nation table which each row repeated 1000 times
        TrinoFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        Location fileLocation = copyResource(fileSystemFactory, "nationFile25kRowsSortedOnNationKey/bucket_00000");
        long fileLength = fileSystemFactory.create(ConnectorIdentity.ofUser("test")).newInputFile(fileLocation).length();
        return readFile(fileSystemFactory, columns, nationKeyPredicate, acidInfo, fileLocation, fileLength);
    }

    private static List<Nation> readFile(
            TrinoFileSystemFactory fileSystemFactory,
            Map<NationColumn, Integer> columns,
            OptionalLong nationKeyPredicate,
            Optional<AcidInfo> acidInfo,
            Location location,
            long fileSize)
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

        HivePageSourceFactory pageSourceFactory = new OrcPageSourceFactory(
                new OrcReaderConfig(),
                fileSystemFactory,
                new FileFormatDataSourceStats(),
                new HiveConfig());

        Optional<ReaderPageSource> pageSourceWithProjections = pageSourceFactory.createPageSource(
                SESSION,
                location,
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
        Type trinoType = switch (nationColumn.getType().getBase()) {
            case IDENTIFIER -> BIGINT;
            case VARCHAR -> VARCHAR;
            default -> throw new IllegalStateException("Unexpected value: " + nationColumn.getType().getBase());
        };

        return createBaseColumn(
                nationColumn.getColumnName(),
                hiveColumnIndex,
                toHiveType(trinoType),
                trinoType,
                REGULAR,
                Optional.empty());
    }

    private static Map<String, String> createSchema()
    {
        return ImmutableMap.<String, String>builder()
                .put(SERIALIZATION_LIB, ORC.getSerde())
                .put(FILE_INPUT_FORMAT, ORC.getInputFormat())
                .put(TRANSACTIONAL, "true")
                .buildOrThrow();
    }

    private static void assertEqualsByColumns(Set<NationColumn> columns, List<Nation> actualRows, List<Nation> expectedRows)
    {
        assertThat(actualRows.size())
                .describedAs("row count")
                .isEqualTo(expectedRows.size());
        for (int i = 0; i < actualRows.size(); i++) {
            Nation actual = actualRows.get(i);
            Nation expected = expectedRows.get(i);
            assertThat(actual.nationKey()).isEqualTo(columns.contains(NATION_KEY) ? expected.nationKey() : -42);
            assertThat(actual.name()).isEqualTo(columns.contains(NAME) ? expected.name() : "<not read>");
            assertThat(actual.regionKey()).isEqualTo(columns.contains(REGION_KEY) ? expected.regionKey() : -42);
            assertThat(actual.comment()).isEqualTo(columns.contains(COMMENT) ? expected.comment() : "<not read>");
        }
    }

    private static Location copyResource(TrinoFileSystemFactory fileSystemFactory, String resourceName)
            throws IOException
    {
        Location location = Location.of("memory:///" + resourceName);
        TrinoFileSystem fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser("test"));
        try (OutputStream outputStream = fileSystem.newOutputFile(location).create()) {
            Resources.copy(getResource(resourceName), outputStream);
        }
        return location;
    }
}
