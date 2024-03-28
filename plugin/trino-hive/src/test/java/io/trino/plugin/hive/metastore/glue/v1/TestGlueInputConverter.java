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
package io.trino.plugin.hive.metastore.glue.v1;

import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.StorageDescriptor;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UserDefinedFunction;
import com.amazonaws.services.glue.model.UserDefinedFunctionInput;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.plugin.hive.HiveBucketProperty;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.glue.v1.converter.GlueInputConverter;
import io.trino.plugin.hive.metastore.glue.v1.converter.GlueToTrinoConverter;
import io.trino.spi.function.LanguageFunction;
import org.junit.jupiter.api.Test;

import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static io.trino.plugin.hive.metastore.glue.v1.TestingMetastoreObjects.getTrinoTestDatabase;
import static io.trino.plugin.hive.metastore.glue.v1.TestingMetastoreObjects.getTrinoTestPartition;
import static io.trino.plugin.hive.metastore.glue.v1.TestingMetastoreObjects.getTrinoTestTable;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGlueInputConverter
{
    private final Database testDb = getTrinoTestDatabase();
    private final Table testTbl = getTrinoTestTable(testDb.getDatabaseName());
    private final Partition testPartition = getTrinoTestPartition(testDb.getDatabaseName(), testTbl.getTableName(), ImmutableList.of("val1"));

    @Test
    public void testConvertDatabase()
    {
        DatabaseInput dbInput = GlueInputConverter.convertDatabase(testDb);

        assertThat(dbInput.getName()).isEqualTo(testDb.getDatabaseName());
        assertThat(dbInput.getDescription()).isEqualTo(testDb.getComment().get());
        assertThat(dbInput.getLocationUri()).isEqualTo(testDb.getLocation().get());
        assertThat(dbInput.getParameters()).isEqualTo(testDb.getParameters());
    }

    @Test
    public void testConvertTable()
    {
        TableInput tblInput = GlueInputConverter.convertTable(testTbl);

        assertThat(tblInput.getName()).isEqualTo(testTbl.getTableName());
        assertThat(tblInput.getOwner()).isEqualTo(testTbl.getOwner().orElse(null));
        assertThat(tblInput.getTableType()).isEqualTo(testTbl.getTableType());
        assertThat(tblInput.getParameters()).isEqualTo(testTbl.getParameters());
        assertColumnList(tblInput.getStorageDescriptor().getColumns(), testTbl.getDataColumns());
        assertColumnList(tblInput.getPartitionKeys(), testTbl.getPartitionColumns());
        assertStorage(tblInput.getStorageDescriptor(), testTbl.getStorage());
        assertThat(tblInput.getViewExpandedText()).isEqualTo(testTbl.getViewExpandedText().get());
        assertThat(tblInput.getViewOriginalText()).isEqualTo(testTbl.getViewOriginalText().get());
    }

    @Test
    public void testConvertPartition()
    {
        PartitionInput partitionInput = GlueInputConverter.convertPartition(testPartition);

        assertThat(partitionInput.getParameters()).isEqualTo(testPartition.getParameters());
        assertStorage(partitionInput.getStorageDescriptor(), testPartition.getStorage());
        assertThat(partitionInput.getValues()).isEqualTo(testPartition.getValues());
    }

    @Test
    public void testConvertFunction()
    {
        // random data to avoid compression, but deterministic for size assertion
        String sql = HexFormat.of().formatHex(Slices.random(2000, new Random(0)).getBytes());
        LanguageFunction expected = new LanguageFunction("(integer,bigint,varchar)", sql, List.of(), Optional.of("owner"));

        UserDefinedFunctionInput input = GlueInputConverter.convertFunction("test_name", expected);
        assertThat(input.getOwnerName()).isEqualTo(expected.owner().orElseThrow());

        UserDefinedFunction function = new UserDefinedFunction()
                .withOwnerName(input.getOwnerName())
                .withResourceUris(input.getResourceUris());
        LanguageFunction actual = GlueToTrinoConverter.convertFunction(function);

        assertThat(input.getResourceUris().size()).isEqualTo(4);
        assertThat(actual).isEqualTo(expected);

        // verify that the owner comes from the metastore
        function.setOwnerName("other");
        actual = GlueToTrinoConverter.convertFunction(function);
        assertThat(actual.owner()).isEqualTo(Optional.of("other"));
    }

    private static void assertColumnList(List<com.amazonaws.services.glue.model.Column> actual, List<Column> expected)
    {
        if (expected == null) {
            assertThat(actual).isNull();
        }
        assertThat(actual.size()).isEqualTo(expected.size());

        for (int i = 0; i < expected.size(); i++) {
            assertColumn(actual.get(i), expected.get(i));
        }
    }

    private static void assertColumn(com.amazonaws.services.glue.model.Column actual, Column expected)
    {
        assertThat(actual.getName()).isEqualTo(expected.getName());
        assertThat(actual.getType()).isEqualTo(expected.getType().getHiveTypeName().toString());
        assertThat(actual.getComment()).isEqualTo(expected.getComment().get());
    }

    private static void assertStorage(StorageDescriptor actual, Storage expected)
    {
        assertThat(actual.getLocation()).isEqualTo(expected.getLocation());
        assertThat(actual.getSerdeInfo().getSerializationLibrary()).isEqualTo(expected.getStorageFormat().getSerde());
        assertThat(actual.getInputFormat()).isEqualTo(expected.getStorageFormat().getInputFormat());
        assertThat(actual.getOutputFormat()).isEqualTo(expected.getStorageFormat().getOutputFormat());

        if (expected.getBucketProperty().isPresent()) {
            HiveBucketProperty bucketProperty = expected.getBucketProperty().get();
            assertThat(actual.getBucketColumns()).isEqualTo(bucketProperty.getBucketedBy());
            assertThat(actual.getNumberOfBuckets().intValue()).isEqualTo(bucketProperty.getBucketCount());
        }
    }
}
