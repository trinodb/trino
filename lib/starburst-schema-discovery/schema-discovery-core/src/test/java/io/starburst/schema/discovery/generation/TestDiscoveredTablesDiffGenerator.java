/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.generation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import io.starburst.schema.discovery.TableChanges.TableName;
import io.starburst.schema.discovery.generation.DiscoveredTablesDiffGenerator.ColumnPartitionProjection;
import io.starburst.schema.discovery.generation.DiscoveredTablesDiffGenerator.DroppedColumn;
import io.starburst.schema.discovery.generation.DiscoveredTablesDiffGenerator.DroppedPartitionValue;
import io.starburst.schema.discovery.generation.DiscoveredTablesDiffGenerator.DroppedTable;
import io.starburst.schema.discovery.generation.DiscoveredTablesDiffGenerator.NewColumn;
import io.starburst.schema.discovery.generation.DiscoveredTablesDiffGenerator.NewPartitionValue;
import io.starburst.schema.discovery.generation.DiscoveredTablesDiffGenerator.NewTable;
import io.starburst.schema.discovery.generation.DiscoveredTablesDiffGenerator.RecreatedTable;
import io.starburst.schema.discovery.generation.DiscoveredTablesDiffGenerator.UnchangedColumn;
import io.starburst.schema.discovery.generation.DiscoveredTablesDiffGenerator.UnchangedPartitionValue;
import io.starburst.schema.discovery.generation.DiscoveredTablesDiffGenerator.UnchangedTable;
import io.starburst.schema.discovery.generation.DiscoveredTablesDiffGenerator.UpdatedColumn;
import io.starburst.schema.discovery.generation.DiscoveredTablesDiffGenerator.UpdatedTable;
import io.starburst.schema.discovery.infer.InferredPartitionProjection;
import io.starburst.schema.discovery.internal.Column;
import io.starburst.schema.discovery.internal.HiveType;
import io.starburst.schema.discovery.internal.HiveTypes;
import io.starburst.schema.discovery.models.DiscoveredColumns;
import io.starburst.schema.discovery.models.DiscoveredPartitionValues;
import io.starburst.schema.discovery.models.DiscoveredPartitions;
import io.starburst.schema.discovery.models.DiscoveredTable;
import io.starburst.schema.discovery.models.TableFormat;
import io.trino.plugin.hive.projection.ProjectionType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.starburst.schema.discovery.models.LowerCaseString.toLowerCase;
import static io.starburst.schema.discovery.models.SlashEndedPath.ensureEndsWithSlash;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDiscoveredTablesDiffGenerator
{
    String schemaName = "schema123";
    DiscoveredTable prevTableToUpdate1 = new DiscoveredTable(
            true,
            ensureEndsWithSlash("s3://dummy/ptable123"),
            new TableName(Optional.of(toLowerCase(schemaName)), toLowerCase("ptable123")),
            TableFormat.CSV,
            ImmutableMap.of(),
            new DiscoveredColumns(ImmutableList.of(
                    new Column(toLowerCase("pcolumn1234"), new HiveType(HiveTypes.STRING_TYPE)),
                    new Column(toLowerCase("pcolumn1235"), new HiveType(HiveTypes.HIVE_INT)),
                    new Column(toLowerCase("pcolumn1236"), new HiveType(HiveTypes.HIVE_DATE))
            ), ImmutableList.of()),
            new DiscoveredPartitions(
                    ImmutableList.of(new Column(toLowerCase("date"), new HiveType(HiveTypes.HIVE_DATE))),
                    ImmutableList.of(
                            new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy/date=2022-03"), ImmutableMap.of(toLowerCase("date"), "2022-03")),
                            new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy/date=2022-04"), ImmutableMap.of(toLowerCase("date"), "2022-04")))),
            ImmutableList.of(toLowerCase("s3://dummy/ptable123")));

    DiscoveredTable prevTableToUpdate2 = new DiscoveredTable(
            true,
            ensureEndsWithSlash("s3://dummy/ptable123999"),
            new TableName(Optional.of(toLowerCase(schemaName)), toLowerCase("ptable123999")),
            TableFormat.CSV,
            ImmutableMap.of(),
            new DiscoveredColumns(ImmutableList.of(
                    new Column(toLowerCase("pcolumn1234"), new HiveType(HiveTypes.STRING_TYPE))
            ), ImmutableList.of()),
            new DiscoveredPartitions(
                    ImmutableList.of(new Column(toLowerCase("projection_1"), new HiveType(HiveTypes.HIVE_INT))),
                    ImmutableList.of(
                            new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy/2022"), ImmutableMap.of(toLowerCase("projection_1"), "2022")),
                            new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy/2023"), ImmutableMap.of(toLowerCase("projection_1"), "2023"))),
                    ImmutableMap.of(toLowerCase("projection_1"), new InferredPartitionProjection(false, ProjectionType.INTEGER))),
            ImmutableList.of(toLowerCase("s3://dummy/ptable123999")));

    DiscoveredTable prevTableToDrop = new DiscoveredTable(
            true,
            ensureEndsWithSlash("s3://dummy/ptable1234"),
            new TableName(Optional.of(toLowerCase(schemaName)), toLowerCase("ptable1234")),
            TableFormat.CSV,
            ImmutableMap.of(),
            new DiscoveredColumns(ImmutableList.of(new Column(toLowerCase("pcolumn1234"), new HiveType(HiveTypes.STRING_TYPE))), ImmutableList.of()),
            new DiscoveredPartitions(
                    ImmutableList.of(new Column(toLowerCase("date"), new HiveType(HiveTypes.HIVE_DATE))),
                    ImmutableList.of(
                            new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy/date=2022-03"), ImmutableMap.of(toLowerCase("date"), "2022-03")),
                            new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy/date=2022-04"), ImmutableMap.of(toLowerCase("date"), "2022-04")))),
            ImmutableList.of(toLowerCase("s3://dummy/ptable1234")));

    DiscoveredTable prevTableToNotModify = new DiscoveredTable(
            true,
            ensureEndsWithSlash("s3://dummy/ptable12345"),
            new TableName(Optional.of(toLowerCase(schemaName)), toLowerCase("ptable12345")),
            TableFormat.CSV,
            ImmutableMap.of(),
            new DiscoveredColumns(ImmutableList.of(new Column(toLowerCase("pcolumn12345"), new HiveType(HiveTypes.STRING_TYPE))), ImmutableList.of()),
            new DiscoveredPartitions(
                    ImmutableList.of(new Column(toLowerCase("date"), new HiveType(HiveTypes.HIVE_DATE))),
                    ImmutableList.of(
                            new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy/date=2022-03"), ImmutableMap.of(toLowerCase("date"), "2022-03")),
                            new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy/date=2022-04"), ImmutableMap.of(toLowerCase("date"), "2022-04")))),
            ImmutableList.of(toLowerCase("s3://dummy/ptable12345")));

    DiscoveredTable updatedTable1 = new DiscoveredTable(
            true,
            ensureEndsWithSlash("s3://dummy/ptable123"),
            new TableName(Optional.of(toLowerCase(schemaName)), toLowerCase("ptable123")),
            TableFormat.CSV,
            ImmutableMap.of(),
            new DiscoveredColumns(ImmutableList.of(
                    new Column(toLowerCase("pcolumn12344"), new HiveType(HiveTypes.STRING_TYPE)),
                    new Column(toLowerCase("pcolumn1235"), new HiveType(HiveTypes.HIVE_INT))
            ), ImmutableList.of()),
            new DiscoveredPartitions(
                    ImmutableList.of(new Column(toLowerCase("date"), new HiveType(HiveTypes.HIVE_DATE))),
                    ImmutableList.of(
                            new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy/date=2022-04"), ImmutableMap.of(toLowerCase("date"), "2022-04")),
                            new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy/date=2022-05"), ImmutableMap.of(toLowerCase("date"), "2022-05")))),
            ImmutableList.of(toLowerCase("s3://dummy/ptable123")));

    DiscoveredTable updatedTable2 = new DiscoveredTable(
            true,
            ensureEndsWithSlash("s3://dummy/ptable123999"),
            new TableName(Optional.of(toLowerCase(schemaName)), toLowerCase("ptable123999")),
            TableFormat.CSV,
            ImmutableMap.of(),
            new DiscoveredColumns(ImmutableList.of(
                    new Column(toLowerCase("pcolumn1234"), new HiveType(HiveTypes.STRING_TYPE))
            ), ImmutableList.of()),
            new DiscoveredPartitions(
                    ImmutableList.of(new Column(toLowerCase("projection_1"), new HiveType(HiveTypes.HIVE_INT))),
                    ImmutableList.of(
                            new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy/2023"), ImmutableMap.of(toLowerCase("projection_1"), "2023")),
                            new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy/2024"), ImmutableMap.of(toLowerCase("projection_1"), "2024"))),
                    ImmutableMap.of(toLowerCase("projection_1"), new InferredPartitionProjection(false, ProjectionType.INTEGER))),
            ImmutableList.of(toLowerCase("s3://dummy/ptable123999")));

    DiscoveredTable newTable2 = new DiscoveredTable(
            true,
            ensureEndsWithSlash("s3://dummy/ntable1234"),
            new TableName(Optional.of(toLowerCase(schemaName)), toLowerCase("ntable123")),
            TableFormat.CSV,
            ImmutableMap.of(),
            new DiscoveredColumns(ImmutableList.of(new Column(toLowerCase("ncolumn123"), new HiveType(HiveTypes.STRING_TYPE))), ImmutableList.of()),
            new DiscoveredPartitions(
                    ImmutableList.of(new Column(toLowerCase("date"), new HiveType(HiveTypes.HIVE_DATE))),
                    ImmutableList.of(
                            new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy/date=2022-03"), ImmutableMap.of(toLowerCase("date"), "2022-03")),
                            new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy/date=2022-04"), ImmutableMap.of(toLowerCase("date"), "2022-04")))),
            ImmutableList.of(toLowerCase("s3://dummy/ntable123")));

    DiscoveredTable notModifiedTable3 = new DiscoveredTable(
            true,
            ensureEndsWithSlash("s3://dummy/ptable12345"),
            new TableName(Optional.of(toLowerCase(schemaName)), toLowerCase("ptable12345")),
            TableFormat.CSV,
            ImmutableMap.of(),
            new DiscoveredColumns(ImmutableList.of(new Column(toLowerCase("pcolumn12345"), new HiveType(HiveTypes.STRING_TYPE))), ImmutableList.of()),
            new DiscoveredPartitions(
                    ImmutableList.of(new Column(toLowerCase("date"), new HiveType(HiveTypes.HIVE_DATE))),
                    ImmutableList.of(
                            new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy/date=2022-03"), ImmutableMap.of(toLowerCase("date"), "2022-03")),
                            new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy/date=2022-04"), ImmutableMap.of(toLowerCase("date"), "2022-04")))),
            ImmutableList.of(toLowerCase("s3://dummy/ptable12345")));

    @Test
    public void testDiff()
    {
        List<DiscoveredTablesDiffGenerator.DiffTable> diffTables = DiscoveredTablesDiffGenerator.generateDiff(
                "",
                ImmutableList.of(prevTableToUpdate1, prevTableToUpdate2, prevTableToDrop, prevTableToNotModify),
                ImmutableList.of(updatedTable1, updatedTable2, newTable2, notModifiedTable3));
        assertThat(diffTables)
                .hasSize(5)
                .containsExactly(
                        new NewTable(
                                new TableName(Optional.of(toLowerCase(schemaName)), newTable2.tableName().tableName()),
                                newTable2.path(),
                                newTable2.format(),
                                ImmutableList.of(new NewColumn(toLowerCase("ncolumn123"), new HiveType(HiveTypes.STRING_TYPE), Optional.empty())),
                                ImmutableList.of(new NewColumn(toLowerCase("date"), new HiveType(HiveTypes.HIVE_DATE), Optional.empty())),
                                ImmutableList.of(new NewPartitionValue(ensureEndsWithSlash("s3://dummy/date=2022-03"), ImmutableMap.of(toLowerCase("date"), "2022-03")),
                                        new NewPartitionValue(ensureEndsWithSlash("s3://dummy/date=2022-04"), ImmutableMap.of(toLowerCase("date"), "2022-04"))),
                                ImmutableList.of()),
                        new DroppedTable(
                                new TableName(Optional.of(toLowerCase(schemaName)), prevTableToDrop.tableName().tableName()),
                                prevTableToDrop.path(),
                                prevTableToDrop.format(),
                                ImmutableList.of(new DroppedColumn(toLowerCase("pcolumn1234"), new HiveType(HiveTypes.STRING_TYPE), Optional.empty())),
                                ImmutableList.of(new DroppedColumn(toLowerCase("date"), new HiveType(HiveTypes.HIVE_DATE), Optional.empty())),
                                ImmutableList.of(new DroppedPartitionValue(ensureEndsWithSlash("s3://dummy/date=2022-03"), ImmutableMap.of(toLowerCase("date"), "2022-03")),
                                        new DroppedPartitionValue(ensureEndsWithSlash("s3://dummy/date=2022-04"), ImmutableMap.of(toLowerCase("date"), "2022-04"))),
                                ImmutableList.of()),
                        new UpdatedTable(
                                new TableName(Optional.of(toLowerCase(schemaName)), updatedTable1.tableName().tableName()),
                                updatedTable1.path(),
                                updatedTable1.format(),
                                ImmutableList.of(
                                        new DroppedColumn(toLowerCase("pcolumn1236"), new HiveType(HiveTypes.HIVE_DATE), Optional.empty()),
                                        new UpdatedColumn(toLowerCase("pcolumn12344"), new HiveType(HiveTypes.STRING_TYPE), toLowerCase("pcolumn1234"), Optional.empty()),
                                        new UnchangedColumn(toLowerCase("pcolumn1235"), new HiveType(HiveTypes.HIVE_INT), Optional.empty())),
                                ImmutableList.of(new UnchangedColumn(toLowerCase("date"), new HiveType(HiveTypes.HIVE_DATE), Optional.empty())),
                                ImmutableList.of(new NewPartitionValue(ensureEndsWithSlash("s3://dummy/date=2022-05"), ImmutableMap.of(toLowerCase("date"), "2022-05")),
                                        new DroppedPartitionValue(ensureEndsWithSlash("s3://dummy/date=2022-03"), ImmutableMap.of(toLowerCase("date"), "2022-03")),
                                        new UnchangedPartitionValue(ensureEndsWithSlash("s3://dummy/date=2022-04"), ImmutableMap.of(toLowerCase("date"), "2022-04"))),
                                ImmutableList.of()),
                        new UnchangedTable(
                                new TableName(Optional.of(toLowerCase(schemaName)), notModifiedTable3.tableName().tableName()),
                                notModifiedTable3.path(),
                                notModifiedTable3.format(),
                                ImmutableList.of(new UnchangedColumn(toLowerCase("pcolumn12345"), new HiveType(HiveTypes.STRING_TYPE), Optional.empty())),
                                ImmutableList.of(new UnchangedColumn(toLowerCase("date"), new HiveType(HiveTypes.HIVE_DATE), Optional.empty())),
                                ImmutableList.of(new UnchangedPartitionValue(ensureEndsWithSlash("s3://dummy/date=2022-03"), ImmutableMap.of(toLowerCase("date"), "2022-03")),
                                        new UnchangedPartitionValue(ensureEndsWithSlash("s3://dummy/date=2022-04"), ImmutableMap.of(toLowerCase("date"), "2022-04"))),
                                ImmutableList.of()),
                        new RecreatedTable(
                                new TableName(Optional.of(toLowerCase(schemaName)), updatedTable2.tableName().tableName()),
                                updatedTable2.path(),
                                updatedTable2.format(),
                                ImmutableList.of(
                                        new NewColumn(toLowerCase("pcolumn1234"), new HiveType(HiveTypes.HIVE_STRING), Optional.empty())),
                                ImmutableList.of(new NewColumn(toLowerCase("projection_1"), new HiveType(HiveTypes.HIVE_INT), Optional.empty())),
                                ImmutableList.of(new NewPartitionValue(ensureEndsWithSlash("s3://dummy/2023"), ImmutableMap.of(toLowerCase("projection_1"), "2023")),
                                        new NewPartitionValue(ensureEndsWithSlash("s3://dummy/2024"), ImmutableMap.of(toLowerCase("projection_1"), "2024"))),
                                ImmutableList.of(new ColumnPartitionProjection(toLowerCase("projection_1"), new InferredPartitionProjection(false, ProjectionType.INTEGER)))));
    }

    @Test
    public void testSerialization()
            throws JsonProcessingException
    {
        ObjectMapper objectMapper = new ObjectMapperProvider().get();

        List<DiscoveredTablesDiffGenerator.DiffTable> diffTables = DiscoveredTablesDiffGenerator.generateDiff(
                "",
                ImmutableList.of(prevTableToUpdate1, prevTableToUpdate2, prevTableToDrop, prevTableToNotModify),
                ImmutableList.of(updatedTable1, updatedTable2, newTable2, notModifiedTable3));
        String serialized = objectMapper.writeValueAsString(diffTables);
        List<DiscoveredTablesDiffGenerator.DiffTable> deserializedDiff = objectMapper.readValue(serialized, new TypeReference<>() {});
        assertThat(deserializedDiff).isEqualTo(diffTables);
    }
}
