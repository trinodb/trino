/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery;

import com.google.common.collect.ImmutableMap;
import io.starburst.schema.discovery.infer.InferredPartitionProjection;
import io.starburst.schema.discovery.internal.Column;
import io.starburst.schema.discovery.internal.HiveType;
import io.starburst.schema.discovery.internal.HiveTypes;
import io.starburst.schema.discovery.io.DiscoveryTrinoFileSystem;
import io.starburst.schema.discovery.models.DiscoveredPartitionValues;
import io.starburst.schema.discovery.models.DiscoveredSchema;
import io.starburst.schema.discovery.models.DiscoveredTable;
import io.starburst.schema.discovery.models.TableFormat;
import io.starburst.schema.discovery.request.GuessRequest;
import io.trino.filesystem.Location;
import io.trino.plugin.hive.projection.ProjectionType;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.Map;
import java.util.function.Predicate;

import static io.starburst.schema.discovery.Util.orcDataSourceFactory;
import static io.starburst.schema.discovery.Util.parquetDataSourceFactory;
import static io.starburst.schema.discovery.generation.Dialect.TRINO;
import static io.starburst.schema.discovery.io.LocationUtils.uriFromLocation;
import static io.starburst.schema.discovery.models.DiscoveredPartitions.EMPTY_DISCOVERED_PARTITIONS;
import static io.starburst.schema.discovery.models.LowerCaseString.toLowerCase;
import static io.starburst.schema.discovery.options.GeneralOptions.DISCOVER_PARTITION_PROJECTION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDiscoveryPartitionProjection
{
    @Test
    public void testPartitionProjectionDiscovery()
            throws Exception
    {
        DiscoveryTrinoFileSystem fileSystem = Util.fileSystem();
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, parquetDataSourceFactory, orcDataSourceFactory, TRINO);
        Map<String, String> includeOnlyNestedOptions = ImmutableMap.of(
                DISCOVER_PARTITION_PROJECTION, "true");
        Location directory = Util.testFilePath("partition-projection/template/");

        DiscoveredSchema discoveredSchema = controller.guess(new GuessRequest(uriFromLocation(directory), includeOnlyNestedOptions)).get();

        assertThat(discoveredSchema.tables())
                .filteredOn(DiscoveredTable::valid)
                .hasSize(3)
                .allMatch(t -> !t.discoveredPartitions().columns().isEmpty())
                .allMatch(t -> !t.columns().columns().isEmpty())
                .allMatch(t -> t.valid() && t.format() == TableFormat.PARQUET)
                .allMatch(t -> t.errors().isEmpty());

        assertThat(discoveredSchema.tables())
                .filteredOn(Predicate.not(DiscoveredTable::valid))
                .hasSize(1);

        assertProjectedNestedTable(discoveredSchema);
        assertStartsWithProjectedTable(discoveredSchema);
        assertOnlyProjectedTable(discoveredSchema);
        assertWrongProjectionTable(discoveredSchema);
    }

    @Test
    public void testShallowDiscoveryPartitionProjection()
            throws Exception
    {
        DiscoveryTrinoFileSystem fileSystem = Util.fileSystem();
        SchemaDiscoveryController controller = new SchemaDiscoveryController(__ -> fileSystem, parquetDataSourceFactory, orcDataSourceFactory, TRINO);
        Location directory = Util.testFilePath("partition-projection/template");

        DiscoveredSchema discoveredShallowSchema = controller.discoverTablesShallow(new GuessRequest(uriFromLocation(directory), ImmutableMap.of(DISCOVER_PARTITION_PROJECTION, "true"))).get();

        assertThat(discoveredShallowSchema.tables())
                .hasSize(4)
                .anyMatch(t -> !t.discoveredPartitions().columns().isEmpty())
                .allMatch(t -> t.columns().columns().isEmpty())
                .allMatch(t -> !t.valid() && t.format() == TableFormat.ERROR);

        assertProjectedNestedTable(discoveredShallowSchema);
        assertStartsWithProjectedTable(discoveredShallowSchema);
        assertOnlyProjectedTable(discoveredShallowSchema);
        assertWrongProjectionTable(discoveredShallowSchema);
    }

    private static void assertStartsWithProjectedTable(DiscoveredSchema discoveredSchema)
    {
        DiscoveredTable startsWithProjectedTable = discoveredSchema.tables().stream().filter(t -> t.tableName().tableName().equals(toLowerCase("starts_with_projected"))).findFirst().orElseThrow();
        assertThat(startsWithProjectedTable.hasAnyProjectedPartition()).isTrue();
        assertThat(startsWithProjectedTable.discoveredPartitions().columns()).containsExactly(
                new Column(toLowerCase("partition_1"), new HiveType(HiveTypes.STRING_TYPE)),
                new Column(toLowerCase("year"), new HiveType(HiveTypes.HIVE_INT)),
                new Column(toLowerCase("month"), new HiveType(HiveTypes.HIVE_INT)),
                new Column(toLowerCase("day"), new HiveType(HiveTypes.HIVE_INT)));
        assertThat(startsWithProjectedTable.discoveredPartitions().values().stream().map(DiscoveredPartitionValues::values)).containsExactly(
                ImmutableMap.of(toLowerCase("partition_1"), "April-2023", toLowerCase("year"), "2018", toLowerCase("month"), "5", toLowerCase("day"), "2"),
                ImmutableMap.of(toLowerCase("partition_1"), "April-2023", toLowerCase("year"), "2018", toLowerCase("month"), "5", toLowerCase("day"), "3"),
                ImmutableMap.of(toLowerCase("partition_1"), "April-2023", toLowerCase("year"), "2018", toLowerCase("month"), "6", toLowerCase("day"), "2"),
                ImmutableMap.of(toLowerCase("partition_1"), "April-2023", toLowerCase("year"), "2018", toLowerCase("month"), "6", toLowerCase("day"), "3"),

                ImmutableMap.of(toLowerCase("partition_1"), "April-2023", toLowerCase("year"), "2019", toLowerCase("month"), "5", toLowerCase("day"), "2"),
                ImmutableMap.of(toLowerCase("partition_1"), "April-2023", toLowerCase("year"), "2019", toLowerCase("month"), "5", toLowerCase("day"), "3"),
                ImmutableMap.of(toLowerCase("partition_1"), "April-2023", toLowerCase("year"), "2019", toLowerCase("month"), "6", toLowerCase("day"), "2"),
                ImmutableMap.of(toLowerCase("partition_1"), "April-2023", toLowerCase("year"), "2019", toLowerCase("month"), "6", toLowerCase("day"), "3"),

                ImmutableMap.of(toLowerCase("partition_1"), "April-2023", toLowerCase("year"), "2020", toLowerCase("month"), "5", toLowerCase("day"), "2"),
                ImmutableMap.of(toLowerCase("partition_1"), "April-2023", toLowerCase("year"), "2020", toLowerCase("month"), "5", toLowerCase("day"), "3"),
                ImmutableMap.of(toLowerCase("partition_1"), "April-2023", toLowerCase("year"), "2020", toLowerCase("month"), "6", toLowerCase("day"), "2"),
                ImmutableMap.of(toLowerCase("partition_1"), "April-2023", toLowerCase("year"), "2020", toLowerCase("month"), "6", toLowerCase("day"), "3"),

                ImmutableMap.of(toLowerCase("partition_1"), "March-2023", toLowerCase("year"), "2018", toLowerCase("month"), "5", toLowerCase("day"), "2"),
                ImmutableMap.of(toLowerCase("partition_1"), "March-2023", toLowerCase("year"), "2018", toLowerCase("month"), "5", toLowerCase("day"), "3"),
                ImmutableMap.of(toLowerCase("partition_1"), "March-2023", toLowerCase("year"), "2018", toLowerCase("month"), "6", toLowerCase("day"), "2"),
                ImmutableMap.of(toLowerCase("partition_1"), "March-2023", toLowerCase("year"), "2018", toLowerCase("month"), "6", toLowerCase("day"), "3"),

                ImmutableMap.of(toLowerCase("partition_1"), "March-2023", toLowerCase("year"), "2019", toLowerCase("month"), "5", toLowerCase("day"), "2"),
                ImmutableMap.of(toLowerCase("partition_1"), "March-2023", toLowerCase("year"), "2019", toLowerCase("month"), "5", toLowerCase("day"), "3"),
                ImmutableMap.of(toLowerCase("partition_1"), "March-2023", toLowerCase("year"), "2019", toLowerCase("month"), "6", toLowerCase("day"), "2"),
                ImmutableMap.of(toLowerCase("partition_1"), "March-2023", toLowerCase("year"), "2019", toLowerCase("month"), "6", toLowerCase("day"), "3"),

                ImmutableMap.of(toLowerCase("partition_1"), "March-2023", toLowerCase("year"), "2020", toLowerCase("month"), "5", toLowerCase("day"), "2"),
                ImmutableMap.of(toLowerCase("partition_1"), "March-2023", toLowerCase("year"), "2020", toLowerCase("month"), "5", toLowerCase("day"), "3"),
                ImmutableMap.of(toLowerCase("partition_1"), "March-2023", toLowerCase("year"), "2020", toLowerCase("month"), "6", toLowerCase("day"), "2"),
                ImmutableMap.of(toLowerCase("partition_1"), "March-2023", toLowerCase("year"), "2020", toLowerCase("month"), "6", toLowerCase("day"), "3"),

                ImmutableMap.of(toLowerCase("partition_1"), "May-2023", toLowerCase("year"), "2018", toLowerCase("month"), "5", toLowerCase("day"), "2"),
                ImmutableMap.of(toLowerCase("partition_1"), "May-2023", toLowerCase("year"), "2018", toLowerCase("month"), "5", toLowerCase("day"), "3"),
                ImmutableMap.of(toLowerCase("partition_1"), "May-2023", toLowerCase("year"), "2018", toLowerCase("month"), "6", toLowerCase("day"), "2"),
                ImmutableMap.of(toLowerCase("partition_1"), "May-2023", toLowerCase("year"), "2018", toLowerCase("month"), "6", toLowerCase("day"), "3"),

                ImmutableMap.of(toLowerCase("partition_1"), "May-2023", toLowerCase("year"), "2019", toLowerCase("month"), "5", toLowerCase("day"), "2"),
                ImmutableMap.of(toLowerCase("partition_1"), "May-2023", toLowerCase("year"), "2019", toLowerCase("month"), "5", toLowerCase("day"), "3"),
                ImmutableMap.of(toLowerCase("partition_1"), "May-2023", toLowerCase("year"), "2019", toLowerCase("month"), "6", toLowerCase("day"), "2"),
                ImmutableMap.of(toLowerCase("partition_1"), "May-2023", toLowerCase("year"), "2019", toLowerCase("month"), "6", toLowerCase("day"), "3"),

                ImmutableMap.of(toLowerCase("partition_1"), "May-2023", toLowerCase("year"), "2020", toLowerCase("month"), "5", toLowerCase("day"), "2"),
                ImmutableMap.of(toLowerCase("partition_1"), "May-2023", toLowerCase("year"), "2020", toLowerCase("month"), "5", toLowerCase("day"), "3"),
                ImmutableMap.of(toLowerCase("partition_1"), "May-2023", toLowerCase("year"), "2020", toLowerCase("month"), "6", toLowerCase("day"), "2"),
                ImmutableMap.of(toLowerCase("partition_1"), "May-2023", toLowerCase("year"), "2020", toLowerCase("month"), "6", toLowerCase("day"), "3"));

        assertThat(startsWithProjectedTable.discoveredPartitions().columnProjections()).containsExactly(
                new AbstractMap.SimpleEntry<>(toLowerCase("partition_1"), new InferredPartitionProjection(false, ProjectionType.ENUM)),
                new AbstractMap.SimpleEntry<>(toLowerCase("year"), new InferredPartitionProjection(true, ProjectionType.INTEGER)),
                new AbstractMap.SimpleEntry<>(toLowerCase("month"), new InferredPartitionProjection(true, ProjectionType.INTEGER)),
                new AbstractMap.SimpleEntry<>(toLowerCase("day"), new InferredPartitionProjection(true, ProjectionType.INTEGER)));
    }

    private static void assertProjectedNestedTable(DiscoveredSchema discoveredSchema)
    {
        DiscoveredTable projectedNestedTable = discoveredSchema.tables().stream().filter(t -> t.tableName().tableName().equals(toLowerCase("projected_nested"))).findFirst().orElseThrow();
        assertThat(projectedNestedTable.hasAnyProjectedPartition()).isTrue();
        assertThat(projectedNestedTable.discoveredPartitions().columns()).containsExactly(
                new Column(toLowerCase("year"), new HiveType(HiveTypes.HIVE_INT)),
                new Column(toLowerCase("partition_1"), new HiveType(HiveTypes.STRING_TYPE)),
                new Column(toLowerCase("day"), new HiveType(HiveTypes.HIVE_INT)));
        assertThat(projectedNestedTable.discoveredPartitions().values().stream().map(DiscoveredPartitionValues::values)).containsExactly(
                ImmutableMap.of(toLowerCase("year"), "2020", toLowerCase("partition_1"), "month-1", toLowerCase("day"), "1"),
                ImmutableMap.of(toLowerCase("year"), "2021", toLowerCase("partition_1"), "month-1", toLowerCase("day"), "1"),
                ImmutableMap.of(toLowerCase("year"), "2021", toLowerCase("partition_1"), "month-2", toLowerCase("day"), "2"));
        assertThat(projectedNestedTable.discoveredPartitions().columnProjections()).containsExactly(
                new AbstractMap.SimpleEntry<>(toLowerCase("year"), new InferredPartitionProjection(true, ProjectionType.INTEGER)),
                new AbstractMap.SimpleEntry<>(toLowerCase("partition_1"), new InferredPartitionProjection(false, ProjectionType.ENUM)),
                new AbstractMap.SimpleEntry<>(toLowerCase("day"), new InferredPartitionProjection(true, ProjectionType.INTEGER)));
    }

    private static void assertOnlyProjectedTable(DiscoveredSchema discoveredSchema)
    {
        DiscoveredTable onlyProjectedTable = discoveredSchema.tables().stream().filter(t -> t.tableName().tableName().equals(toLowerCase("only_projected"))).findFirst().orElseThrow();
        assertThat(onlyProjectedTable.hasAnyProjectedPartition()).isTrue();
        assertThat(onlyProjectedTable.discoveredPartitions().columns()).containsExactly(
                new Column(toLowerCase("partition_1"), new HiveType(HiveTypes.STRING_TYPE)));
        assertThat(onlyProjectedTable.discoveredPartitions().values().stream().map(DiscoveredPartitionValues::values)).containsExactly(
                ImmutableMap.of(toLowerCase("partition_1"), "April-2023"),
                ImmutableMap.of(toLowerCase("partition_1"), "March-2023"),
                ImmutableMap.of(toLowerCase("partition_1"), "May-2023"));
        assertThat(onlyProjectedTable.discoveredPartitions().columnProjections()).containsExactly(
                new AbstractMap.SimpleEntry<>(toLowerCase("partition_1"), new InferredPartitionProjection(false, ProjectionType.ENUM)));
    }

    private static void assertWrongProjectionTable(DiscoveredSchema discoveredSchema)
    {
        DiscoveredTable wrongProjectionTable = discoveredSchema.tables().stream().filter(t -> t.tableName().tableName().equals(toLowerCase("wrong_projection"))).findFirst().orElseThrow();
        assertThat(wrongProjectionTable.hasAnyProjectedPartition()).isFalse();
        assertThat(wrongProjectionTable.discoveredPartitions()).isEqualTo(EMPTY_DISCOVERED_PARTITIONS);
        assertThat(wrongProjectionTable.errors()).isNotEmpty();
    }
}
