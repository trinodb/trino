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
package io.trino.plugin.hive.metastore.glue.v2;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.metastore.Column;
import io.trino.metastore.Database;
import io.trino.metastore.HiveBucketProperty;
import io.trino.metastore.Partition;
import io.trino.metastore.Storage;
import io.trino.metastore.Table;
import io.trino.plugin.hive.metastore.glue.v2.converter.GlueInputConverter;
import io.trino.plugin.hive.metastore.glue.v2.converter.GlueToTrinoConverter;
import io.trino.spi.function.LanguageFunction;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UserDefinedFunction;
import software.amazon.awssdk.services.glue.model.UserDefinedFunctionInput;

import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static io.trino.plugin.hive.metastore.glue.v2.TestingMetastoreObjects.getTrinoTestDatabase;
import static io.trino.plugin.hive.metastore.glue.v2.TestingMetastoreObjects.getTrinoTestPartition;
import static io.trino.plugin.hive.metastore.glue.v2.TestingMetastoreObjects.getTrinoTestTable;
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

        assertThat(dbInput.name()).isEqualTo(testDb.getDatabaseName());
        assertThat(dbInput.description()).isEqualTo(testDb.getComment().get());
        assertThat(dbInput.locationUri()).isEqualTo(testDb.getLocation().get());
        assertThat(dbInput.parameters()).isEqualTo(testDb.getParameters());
    }

    @Test
    public void testConvertTable()
    {
        TableInput tblInput = GlueInputConverter.convertTable(testTbl);

        assertThat(tblInput.name()).isEqualTo(testTbl.getTableName());
        assertThat(tblInput.owner()).isEqualTo(testTbl.getOwner().orElse(null));
        assertThat(tblInput.tableType()).isEqualTo(testTbl.getTableType());
        assertThat(tblInput.parameters()).isEqualTo(testTbl.getParameters());
        assertColumnList(tblInput.storageDescriptor().columns(), testTbl.getDataColumns());
        assertColumnList(tblInput.partitionKeys(), testTbl.getPartitionColumns());
        assertStorage(tblInput.storageDescriptor(), testTbl.getStorage());
        assertThat(tblInput.viewExpandedText()).isEqualTo(testTbl.getViewExpandedText().get());
        assertThat(tblInput.viewOriginalText()).isEqualTo(testTbl.getViewOriginalText().get());
    }

    @Test
    public void testConvertPartition()
    {
        PartitionInput partitionInput = GlueInputConverter.convertPartition(testPartition);

        assertThat(partitionInput.parameters()).isEqualTo(testPartition.getParameters());
        assertStorage(partitionInput.storageDescriptor(), testPartition.getStorage());
        assertThat(partitionInput.values()).isEqualTo(testPartition.getValues());
    }

    @Test
    public void testConvertFunction()
    {
        // random data to avoid compression, but deterministic for size assertion
        String sql = HexFormat.of().formatHex(Slices.random(2000, new Random(0)).getBytes());
        LanguageFunction expected = new LanguageFunction("(integer,bigint,varchar)", sql, List.of(), Optional.of("owner"));

        UserDefinedFunctionInput input = GlueInputConverter.convertFunction("test_name", expected);
        assertThat(input.ownerName()).isEqualTo(expected.owner().orElseThrow());

        UserDefinedFunction function = UserDefinedFunction.builder()
                .ownerName(input.ownerName())
                .resourceUris(input.resourceUris())
                .build();
        LanguageFunction actual = GlueToTrinoConverter.convertFunction(function);

        assertThat(input.resourceUris().size()).isEqualTo(3);
        assertThat(actual).isEqualTo(expected);

        // verify that the owner comes from the metastore
        actual = GlueToTrinoConverter.convertFunction(function.toBuilder().ownerName("other").build());
        assertThat(actual.owner()).isEqualTo(Optional.of("other"));
    }

    private static void assertColumnList(List<software.amazon.awssdk.services.glue.model.Column> actual, List<Column> expected)
    {
        if (expected == null) {
            assertThat(actual).isNull();
        }
        assertThat(actual.size()).isEqualTo(expected.size());

        for (int i = 0; i < expected.size(); i++) {
            assertColumn(actual.get(i), expected.get(i));
        }
    }

    private static void assertColumn(software.amazon.awssdk.services.glue.model.Column actual, Column expected)
    {
        assertThat(actual.name()).isEqualTo(expected.getName());
        assertThat(actual.type()).isEqualTo(expected.getType().getHiveTypeName().toString());
        assertThat(actual.comment()).isEqualTo(expected.getComment().get());
    }

    private static void assertStorage(StorageDescriptor actual, Storage expected)
    {
        assertThat(actual.location()).isEqualTo(expected.getLocation());
        assertThat(actual.serdeInfo().serializationLibrary()).isEqualTo(expected.getStorageFormat().getSerde());
        assertThat(actual.inputFormat()).isEqualTo(expected.getStorageFormat().getInputFormat());
        assertThat(actual.outputFormat()).isEqualTo(expected.getStorageFormat().getOutputFormat());

        if (expected.getBucketProperty().isPresent()) {
            HiveBucketProperty bucketProperty = expected.getBucketProperty().get();
            assertThat(actual.bucketColumns()).isEqualTo(bucketProperty.bucketedBy());
            assertThat(actual.numberOfBuckets().intValue()).isEqualTo(bucketProperty.bucketCount());
        }
    }
}
