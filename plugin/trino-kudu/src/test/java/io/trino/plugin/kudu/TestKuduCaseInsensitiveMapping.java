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
package io.trino.plugin.kudu;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.kudu.properties.ColumnDesign;
import io.trino.plugin.kudu.properties.HashPartitionDefinition;
import io.trino.plugin.kudu.properties.KuduTableProperties;
import io.trino.plugin.kudu.properties.PartitionDesign;
import io.trino.plugin.kudu.properties.RangePartition;
import io.trino.plugin.kudu.properties.RangePartitionDefinition;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.PartialRow;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.kudu.KuduQueryRunnerFactory.createKuduClient;
import static io.trino.plugin.kudu.KuduQueryRunnerFactory.createKuduQueryRunner;
import static io.trino.plugin.kudu.properties.KuduTableProperties.PARTITION_BY_HASH_BUCKETS;
import static io.trino.plugin.kudu.properties.KuduTableProperties.PARTITION_BY_HASH_COLUMNS;
import static io.trino.plugin.kudu.properties.KuduTableProperties.PARTITION_BY_RANGE_COLUMNS;
import static io.trino.plugin.kudu.properties.KuduTableProperties.PRIMARY_KEY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class TestKuduCaseInsensitiveMapping
        extends AbstractTestQueryFramework
{
    private KuduClient kuduClient;
    private TestingKuduServer kuduServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        kuduServer = new TestingKuduServer();
        kuduClient = createKuduClient(kuduServer);
        return createKuduQueryRunner(closeAfterClass(kuduServer), "testcase", true);
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
            throws Exception
    {
        kuduServer.close();
        kuduServer = null;
        kuduClient.close();
        kuduClient = null;
    }

    @Test
    public void testCaseInsensitive()
            throws Exception
    {
        createTestTable("testcase.testInsensitive");

        assertQuery("SHOW SCHEMAS IN kudu LIKE 'testcase'", "SELECT 'testcase'");
        assertQuery("SHOW TABLES IN testcase", "SELECT 'testinsensitive'");
        assertQuery(
                "SELECT column_name, data_type FROM kudu.information_schema.columns WHERE table_name = 'testinsensitive' AND column_name = 'name'",
                "SELECT 'name', 'varchar'");
        assertQueryReturnsEmptyResult("SELECT name, value FROM testcase.testInsensitive");
        assertUpdate("INSERT INTO testcase.testinsensitive VALUES('def', 2)", 1);
        assertQuery("SELECT value FROM testCase.testinsensitive WHERE name = 'def'", "SELECT 2");

        assertUpdate("DROP TABLE testcase.testinsensitive");
        assertQueryReturnsEmptyResult("SHOW TABLES IN testcase");

        assertUpdate("DROP SCHEMA testcase");
        assertQueryReturnsEmptyResult("SHOW SCHEMAS IN kudu LIKE 'testcase'");
    }

    @Test
    public void testCaseInsensitiveRenameTable()
            throws Exception
    {
        createTestTable("testcase.testInsensitive_RenameTable");

        assertQuery("SHOW TABLES IN testcase", "SELECT 'testinsensitive_renametable'");
        assertUpdate("INSERT INTO testcase.testinsensitive_renametable VALUES('def', 2)", 1);
        assertQuery("SELECT value FROM testcase.testinsensitive_renametable WHERE name = 'def'", "SELECT 2");

        assertUpdate("ALTER TABLE testcase.testinsensitive_renametable RENAME TO testcase.testinsensitive_renamed_table");

        assertQuery("SHOW TABLES IN testcase", "SELECT 'testinsensitive_renamed_table'");
        assertQuery("SELECT value FROM testcase.testinsensitive_renamed_table WHERE name = 'def'", "SELECT 2");
        assertUpdate("DROP TABLE testcase.testinsensitive_renamed_table");
    }

    private void createTestTable(String name)
            throws KuduException
    {
        ImmutableList.Builder<ColumnMetadata> trinoColumns = ImmutableList.builder();
        ImmutableMap.Builder<String, Object> primaryKeyBuilder = ImmutableMap.builder();
        primaryKeyBuilder.put(PRIMARY_KEY, true);
        ColumnMetadata nameColumn = ColumnMetadata.builder()
                .setName("Name")
                .setType(VARCHAR)
                .setProperties(primaryKeyBuilder.buildOrThrow())
                .build();
        trinoColumns.add(nameColumn);
        ColumnMetadata valueColumn = new ColumnMetadata("value", BIGINT);
        trinoColumns.add(valueColumn);
        Schema schema = buildSchema(trinoColumns.build());
        ImmutableMap.Builder<String, Object> kuduTablePropertiesBuilder = ImmutableMap.builder();
        Map<String, Object> kuduTableProperties = kuduTablePropertiesBuilder.put(PARTITION_BY_HASH_COLUMNS, ImmutableList.of("name"))
                .put(PARTITION_BY_HASH_BUCKETS, 2)
                .put(PARTITION_BY_RANGE_COLUMNS, ImmutableList.of())
                .buildOrThrow();
        CreateTableOptions options = buildCreateTableOptions(schema, kuduTableProperties);
        kuduClient.createTable(name, schema, options);
    }

    private Schema buildSchema(List<ColumnMetadata> columns)
    {
        List<ColumnSchema> kuduColumns = columns.stream()
                .map(this::toColumnSchema)
                .collect(toImmutableList());
        return new Schema(kuduColumns);
    }

    private ColumnSchema toColumnSchema(ColumnMetadata columnMetadata)
    {
        String name = columnMetadata.getName();
        ColumnDesign design = KuduTableProperties.getColumnDesign(columnMetadata.getProperties());
        Type ktype = TypeHelper.toKuduClientType(columnMetadata.getType());
        ColumnSchema.ColumnSchemaBuilder builder = new ColumnSchema.ColumnSchemaBuilder(name, ktype);
        builder.key(design.isPrimaryKey()).nullable(design.isNullable());
        return builder.build();
    }

    private CreateTableOptions buildCreateTableOptions(Schema schema, Map<String, Object> properties)
    {
        CreateTableOptions options = new CreateTableOptions();

        RangePartitionDefinition rangePartitionDefinition = null;
        PartitionDesign partitionDesign = KuduTableProperties.getPartitionDesign(properties);
        if (partitionDesign.getHash() != null) {
            for (HashPartitionDefinition partition : partitionDesign.getHash()) {
                options.addHashPartitions(partition.getColumns(), partition.getBuckets());
            }
        }
        if (partitionDesign.getRange() != null) {
            rangePartitionDefinition = partitionDesign.getRange();
            options.setRangePartitionColumns(rangePartitionDefinition.getColumns());
        }

        List<RangePartition> rangePartitions = KuduTableProperties.getRangePartitions(properties);
        if (rangePartitionDefinition != null && !rangePartitions.isEmpty()) {
            for (RangePartition rangePartition : rangePartitions) {
                PartialRow lower = KuduTableProperties.toRangeBoundToPartialRow(schema, rangePartitionDefinition, rangePartition.getLower());
                PartialRow upper = KuduTableProperties.toRangeBoundToPartialRow(schema, rangePartitionDefinition, rangePartition.getUpper());
                options.addRangePartition(lower, upper);
            }
        }

        Optional<Integer> numReplicas = KuduTableProperties.getNumReplicas(properties);
        numReplicas.ifPresent(options::setNumReplicas);

        return options;
    }
}
