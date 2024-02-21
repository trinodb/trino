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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.starburst.schema.discovery.TableChanges.TableName;
import io.starburst.schema.discovery.infer.InferredPartitionProjection;
import io.starburst.schema.discovery.internal.Column;
import io.starburst.schema.discovery.internal.HiveType;
import io.starburst.schema.discovery.internal.HiveTypes;
import io.starburst.schema.discovery.models.DiscoveredColumns;
import io.starburst.schema.discovery.models.DiscoveredPartitionValues;
import io.starburst.schema.discovery.models.DiscoveredPartitions;
import io.starburst.schema.discovery.models.DiscoveredTable;
import io.starburst.schema.discovery.models.Operation;
import io.starburst.schema.discovery.models.Operation.AddBucket;
import io.starburst.schema.discovery.models.Operation.AddColumn;
import io.starburst.schema.discovery.models.Operation.AddPartitionColumn;
import io.starburst.schema.discovery.models.Operation.AddPartitionValue;
import io.starburst.schema.discovery.models.Operation.Comment;
import io.starburst.schema.discovery.models.Operation.CreateSchema;
import io.starburst.schema.discovery.models.Operation.CreateTable;
import io.starburst.schema.discovery.models.Operation.DropColumn;
import io.starburst.schema.discovery.models.Operation.DropPartitionColumn;
import io.starburst.schema.discovery.models.Operation.DropPartitionValue;
import io.starburst.schema.discovery.models.Operation.DropTable;
import io.starburst.schema.discovery.models.Operation.RegisterTable;
import io.starburst.schema.discovery.models.Operation.RenameColumn;
import io.starburst.schema.discovery.models.Operation.RenamePartitionColumn;
import io.starburst.schema.discovery.models.Operation.UnregisterTable;
import io.starburst.schema.discovery.models.SlashEndedPath;
import io.starburst.schema.discovery.models.TableFormat;
import io.starburst.schema.discovery.request.GenerateOptions;
import io.trino.plugin.hive.projection.ProjectionType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Optional;

import static io.starburst.schema.discovery.models.LowerCaseString.toLowerCase;
import static io.starburst.schema.discovery.models.SlashEndedPath.ensureEndsWithSlash;
import static io.trino.plugin.hive.type.TypeInfoFactory.getListTypeInfo;
import static io.trino.plugin.hive.type.TypeInfoFactory.getMapTypeInfo;
import static io.trino.plugin.hive.type.TypeInfoFactory.getStructTypeInfo;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("checkstyle:RegexpMultiline")
public class TestSqlGenerator
{
    private static final String DEFAULT_SCHEMA_NAME = "SCHema123";
    private static final String OTHER_SCHEMA_NAME = "SCHema123456";
    private static final Optional<String> NO_CATALOG_NAME = Optional.empty();
    private static final Optional<String> CATALOG_NAME = Optional.of("catalog123");

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testCreateTableWithPartitionsWithoutSchemaName(Dialect dialect)
    {
        CreateTable operation = new CreateTable(new DiscoveredTable(
                true,
                ensureEndsWithSlash("s3://dummy"),
                new TableName(Optional.empty(), toLowerCase("table123")),
                TableFormat.CSV,
                ImmutableMap.of(),
                new DiscoveredColumns(ImmutableList.of(new Column(toLowerCase("column123"), new HiveType(HiveTypes.STRING_TYPE))), ImmutableList.of()),
                new DiscoveredPartitions(
                        ImmutableList.of(new Column(toLowerCase("date"), new HiveType(HiveTypes.HIVE_DATE))),
                        ImmutableList.of(
                                new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy/date=2022-03"), ImmutableMap.of(toLowerCase("date"), "2022-03")),
                                new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy/date=2022-04"), ImmutableMap.of(toLowerCase("date"), "2022-04")))),
                ImmutableList.of(toLowerCase("s3://dummy"))));

        String expectedSql = switch (dialect) {
            case TRINO -> """
                CREATE TABLE "catalog123"."schema123"."table123" (
                    "column123" varchar,
                    "date" date
                )
                WITH (
                    format = 'TEXTFILE',
                    textfile_field_separator = ',',
                    textfile_field_separator_escape = '\\',
                    skip_header_line_count = 1,
                    external_location = 's3://dummy/',
                    partitioned_by = ARRAY['date'],
                    bucketed_by = ARRAY['s3://dummy'],
                    bucket_count = 10
                );
                                        
                CALL catalog123.system.sync_partition_metadata('schema123', 'table123', 'ADD');
                                        
                """;
            case GALAXY -> """
                CREATE TABLE "catalog123"."schema123"."table123" (
                    "column123" varchar,
                    "date" date
                )
                WITH (
                    type = 'hive',
                    format = 'TEXTFILE',
                    textfile_field_separator = ',',
                    textfile_field_separator_escape = '\\',
                    skip_header_line_count = 1,
                    external_location = 's3://dummy/',
                    partitioned_by = ARRAY['date'],
                    bucketed_by = ARRAY['s3://dummy'],
                    bucket_count = 10
                );
                                        
                CALL catalog123.system.sync_partition_metadata('schema123', 'table123', 'ADD');
                                        
                """;
        };
        String expectedSummary = "Created table: [table123], with location: [s3://dummy/]";

        testOperation(CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testCreateTableWithPartitionsWithCatalogName(Dialect dialect)
    {
        CreateTable operation = new CreateTable(new DiscoveredTable(
                true,
                ensureEndsWithSlash("s3://dummy"),
                new TableName(Optional.of(toLowerCase(OTHER_SCHEMA_NAME)), toLowerCase("table123")),
                TableFormat.CSV,
                ImmutableMap.of(),
                new DiscoveredColumns(ImmutableList.of(new Column(toLowerCase("column123"), new HiveType(HiveTypes.STRING_TYPE))), ImmutableList.of()),
                new DiscoveredPartitions(
                        ImmutableList.of(new Column(toLowerCase("date"), new HiveType(HiveTypes.HIVE_DATE))),
                        ImmutableList.of(
                                new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy/date=2022-03"), ImmutableMap.of(toLowerCase("date"), "2022-03")),
                                new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy/date=2022-04"), ImmutableMap.of(toLowerCase("date"), "2022-04")))),
                ImmutableList.of(toLowerCase("s3://dummy"))));

        String expectedSql = switch (dialect) {
            case TRINO -> """
                CREATE TABLE "catalog123"."schema123456"."table123" (
                    "column123" varchar,
                    "date" date
                )
                WITH (
                    format = 'TEXTFILE',
                    textfile_field_separator = ',',
                    textfile_field_separator_escape = '\\',
                    skip_header_line_count = 1,
                    external_location = 's3://dummy/',
                    partitioned_by = ARRAY['date'],
                    bucketed_by = ARRAY['s3://dummy'],
                    bucket_count = 10
                );
                                        
                CALL catalog123.system.sync_partition_metadata('schema123456', 'table123', 'ADD');
                                        
                """;
            case GALAXY -> """
                CREATE TABLE "catalog123"."schema123456"."table123" (
                    "column123" varchar,
                    "date" date
                )
                WITH (
                    type = 'hive',
                    format = 'TEXTFILE',
                    textfile_field_separator = ',',
                    textfile_field_separator_escape = '\\',
                    skip_header_line_count = 1,
                    external_location = 's3://dummy/',
                    partitioned_by = ARRAY['date'],
                    bucketed_by = ARRAY['s3://dummy'],
                    bucket_count = 10
                );
                                        
                CALL catalog123.system.sync_partition_metadata('schema123456', 'table123', 'ADD');
                                        
                """;
        };
        String expectedSummary = "Created table: [schema123456.table123], with location: [s3://dummy/]";

        testOperation(CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testCreateTableWithPartitionsWithoutCatalogName(Dialect dialect)
    {
        CreateTable operation = new CreateTable(new DiscoveredTable(
                true,
                ensureEndsWithSlash("s3://dummy"),
                new TableName(Optional.of(toLowerCase(OTHER_SCHEMA_NAME)), toLowerCase("table123")),
                TableFormat.CSV,
                ImmutableMap.of(),
                new DiscoveredColumns(ImmutableList.of(new Column(toLowerCase("column123"), new HiveType(HiveTypes.STRING_TYPE))), ImmutableList.of()),
                new DiscoveredPartitions(
                        ImmutableList.of(new Column(toLowerCase("date"), new HiveType(HiveTypes.HIVE_DATE))),
                        ImmutableList.of(
                                new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy/date=2022-03"), ImmutableMap.of(toLowerCase("date"), "2022-03")),
                                new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy/date=2022-04"), ImmutableMap.of(toLowerCase("date"), "2022-04")))),
                ImmutableList.of(toLowerCase("s3://dummy"))));
        String expectedSql = switch (dialect) {
            case TRINO -> """
                USE "schema123456";
                CREATE TABLE "table123" (
                    "column123" varchar,
                    "date" date
                )
                WITH (
                    format = 'TEXTFILE',
                    textfile_field_separator = ',',
                    textfile_field_separator_escape = '\\',
                    skip_header_line_count = 1,
                    external_location = 's3://dummy/',
                    partitioned_by = ARRAY['date'],
                    bucketed_by = ARRAY['s3://dummy'],
                    bucket_count = 10
                );
                                        
                CALL system.sync_partition_metadata('schema123456', 'table123', 'ADD');
                                        
                """;
            case GALAXY -> """
                USE "schema123456";
                CREATE TABLE "table123" (
                    "column123" varchar,
                    "date" date
                )
                WITH (
                    type = 'hive',
                    format = 'TEXTFILE',
                    textfile_field_separator = ',',
                    textfile_field_separator_escape = '\\',
                    skip_header_line_count = 1,
                    external_location = 's3://dummy/',
                    partitioned_by = ARRAY['date'],
                    bucketed_by = ARRAY['s3://dummy'],
                    bucket_count = 10
                );
                                        
                CALL system.sync_partition_metadata('schema123456', 'table123', 'ADD');
                                        
                """;
        };
        String expectedSummary = "Created table: [schema123456.table123], with location: [s3://dummy/]";

        testOperation(NO_CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testCreateTableWithPartitionProjection(Dialect dialect)
    {
        CreateTable operation = new CreateTable(new DiscoveredTable(
                true,
                ensureEndsWithSlash("s3://dummy"),
                new TableName(Optional.of(toLowerCase(OTHER_SCHEMA_NAME)), toLowerCase("table123")),
                TableFormat.CSV,
                ImmutableMap.of(),
                new DiscoveredColumns(ImmutableList.of(new Column(toLowerCase("column123"), new HiveType(HiveTypes.STRING_TYPE))), ImmutableList.of()),
                new DiscoveredPartitions(
                        ImmutableList.of(
                                new Column(toLowerCase("country"), new HiveType(HiveTypes.HIVE_STRING)),
                                new Column(toLowerCase("projection_1"), new HiveType(HiveTypes.HIVE_INT)),
                                new Column(toLowerCase("projection_2"), new HiveType(HiveTypes.HIVE_STRING)),
                                new Column(toLowerCase("projection_3"), new HiveType(HiveTypes.HIVE_STRING)),
                                new Column(toLowerCase("year"), new HiveType(HiveTypes.HIVE_INT))),
                        ImmutableList.of(
                                new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy/country=POLAND/5/May-2020/south/year=2020"), ImmutableMap.of(
                                        toLowerCase("country"), "POLAND",
                                        toLowerCase("projection_1"), "5",
                                        toLowerCase("projection_2"), "May-2020",
                                        toLowerCase("projection_3"), "south",
                                        toLowerCase("year"), "2020")),
                                new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy/country=GERMANY/6/April-2021/north/year=2020"), ImmutableMap.of(
                                        toLowerCase("country"), "GERMANY",
                                        toLowerCase("projection_1"), "6",
                                        toLowerCase("projection_2"), "April-2021",
                                        toLowerCase("projection_3"), "north",
                                        toLowerCase("year"), "2020"))),
                        ImmutableMap.of(
                                toLowerCase("country"), new InferredPartitionProjection(true, ProjectionType.ENUM),
                                toLowerCase("projection_1"), new InferredPartitionProjection(false, ProjectionType.INTEGER),
                                toLowerCase("projection_2"), new InferredPartitionProjection(false, ProjectionType.ENUM),
                                toLowerCase("projection_3"), new InferredPartitionProjection(false, ProjectionType.ENUM),
                                toLowerCase("year"), new InferredPartitionProjection(true, ProjectionType.INTEGER))),
                ImmutableList.of(toLowerCase("s3://dummy"))));
        String expectedSql = switch (dialect) {
            case TRINO -> """
                USE "schema123456";
                CREATE TABLE "table123" (
                    "column123" varchar,
                    "country" varchar WITH (
                        partition_projection_type = 'ENUM',
                        partition_projection_values = ARRAY['GERMANY', 'POLAND']
                    ),
                    "projection_1" int WITH (
                        partition_projection_type = 'INTEGER',
                        partition_projection_range = ARRAY['5', '6']
                    ),
                    "projection_2" varchar WITH (
                        partition_projection_type = 'ENUM',
                        partition_projection_values = ARRAY['April-2021', 'May-2020']
                    ),
                    "projection_3" varchar WITH (
                        partition_projection_type = 'ENUM',
                        partition_projection_values = ARRAY['north', 'south']
                    ),
                    "year" int WITH (
                        partition_projection_type = 'INTEGER',
                        partition_projection_range = ARRAY['2020', '2020']
                    )
                )
                WITH (
                    format = 'TEXTFILE',
                    textfile_field_separator = ',',
                    textfile_field_separator_escape = '\\',
                    skip_header_line_count = 1,
                    external_location = 's3://dummy/',
                    partition_projection_enabled = true,
                    partition_projection_location_template = 's3://dummy/country=${country}/${projection_1}/${projection_2}/${projection_3}/year=${year}/',
                    partitioned_by = ARRAY['country', 'projection_1', 'projection_2', 'projection_3', 'year'],
                    bucketed_by = ARRAY['s3://dummy'],
                    bucket_count = 10
                );
                                            
                    """;
            case GALAXY -> """
                USE "schema123456";
                CREATE TABLE "table123" (
                    "column123" varchar,
                    "country" varchar WITH (
                        partition_projection_type = 'ENUM',
                        partition_projection_values = ARRAY['GERMANY', 'POLAND']
                    ),
                    "projection_1" int WITH (
                        partition_projection_type = 'INTEGER',
                        partition_projection_range = ARRAY['5', '6']
                    ),
                    "projection_2" varchar WITH (
                        partition_projection_type = 'ENUM',
                        partition_projection_values = ARRAY['April-2021', 'May-2020']
                    ),
                    "projection_3" varchar WITH (
                        partition_projection_type = 'ENUM',
                        partition_projection_values = ARRAY['north', 'south']
                    ),
                    "year" int WITH (
                        partition_projection_type = 'INTEGER',
                        partition_projection_range = ARRAY['2020', '2020']
                    )
                )
                WITH (
                    type = 'hive',
                    format = 'TEXTFILE',
                    textfile_field_separator = ',',
                    textfile_field_separator_escape = '\\',
                    skip_header_line_count = 1,
                    external_location = 's3://dummy/',
                    partition_projection_enabled = true,
                    partition_projection_location_template = 's3://dummy/country=${country}/${projection_1}/${projection_2}/${projection_3}/year=${year}/',
                    partitioned_by = ARRAY['country', 'projection_1', 'projection_2', 'projection_3', 'year'],
                    bucketed_by = ARRAY['s3://dummy'],
                    bucket_count = 10
                );

                    """;
        };
        String expectedSummary = "Created table: [schema123456.table123], with location: [s3://dummy/]";

        testOperation(NO_CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testCreateTableWithNonPrimitiveType(Dialect dialect)
    {
        CreateTable operation = new CreateTable(new DiscoveredTable(
                true,
                ensureEndsWithSlash("s3://dummy"),
                new TableName(Optional.of(toLowerCase(OTHER_SCHEMA_NAME)), toLowerCase("non_primitive_type_table")),
                TableFormat.CSV,
                ImmutableMap.of(),
                new DiscoveredColumns(
                        ImmutableList.of(
                                new Column(toLowerCase("map_column"), new HiveType(getMapTypeInfo(HiveTypes.STRING_TYPE, HiveTypes.STRING_TYPE))),
                                new Column(toLowerCase("list_column"), new HiveType(getListTypeInfo(HiveTypes.STRING_TYPE))),
                                new Column(toLowerCase("struct_column"), new HiveType(getStructTypeInfo(ImmutableList.of("child_column"), ImmutableList.of(HiveTypes.STRING_TYPE))))),
                        ImmutableList.of()),
                new DiscoveredPartitions(ImmutableList.of(), ImmutableList.of()),
                ImmutableList.of(toLowerCase("s3://dummy"))));

        String expectedSql = switch (dialect) {
            case TRINO -> """
                CREATE TABLE "catalog123"."schema123456"."non_primitive_type_table" (
                    "map_column" map(varchar,varchar),
                    "list_column" array(varchar),
                    "struct_column" row("child_column" varchar)
                )
                WITH (
                    format = 'TEXTFILE',
                    textfile_field_separator = ',',
                    textfile_field_separator_escape = '\\',
                    skip_header_line_count = 1,
                    external_location = 's3://dummy/',
                    bucketed_by = ARRAY['s3://dummy'],
                    bucket_count = 10
                );
                                            
                """;
            case GALAXY -> """
                CREATE TABLE "catalog123"."schema123456"."non_primitive_type_table" (
                    "map_column" map(varchar,varchar),
                    "list_column" array(varchar),
                    "struct_column" row("child_column" varchar)
                )
                WITH (
                    type = 'hive',
                    format = 'TEXTFILE',
                    textfile_field_separator = ',',
                    textfile_field_separator_escape = '\\',
                    skip_header_line_count = 1,
                    external_location = 's3://dummy/',
                    bucketed_by = ARRAY['s3://dummy'],
                    bucket_count = 10
                );
                                            
                """;
        };
        String expectedSummary = "Created table: [schema123456.non_primitive_type_table], with location: [s3://dummy/]";

        testOperation(CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testAddPartitionColumnWithoutSchema(Dialect dialect)
    {
        AddPartitionColumn operation = new AddPartitionColumn(new TableName(Optional.empty(), toLowerCase("table2")), new Column(toLowerCase("column2"), new HiveType(HiveTypes.STRING_TYPE)));
        String expectedSql = """
                -- Partition column adds not supported. ["catalog123"."schema123"."table2"."column2"] will not be added.
                                        
                """;
        String expectedSummary = "Added partition column: [column2], of type: [string], to table: [table2]";

        testOperation(CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testAddPartitionColumnWithCatalogName(Dialect dialect)
    {
        AddPartitionColumn operation = new AddPartitionColumn(new TableName(Optional.of(toLowerCase(OTHER_SCHEMA_NAME)), toLowerCase("table2")), new Column(toLowerCase("column2"), new HiveType(HiveTypes.STRING_TYPE)));
        String expectedSql = """
                -- Partition column adds not supported. ["catalog123"."schema123456"."table2"."column2"] will not be added.
                                        
                """;
        String expectedSummary = "Added partition column: [column2], of type: [string], to table: [schema123456.table2]";

        testOperation(CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testAddPartitionColumnWithoutCatalogName(Dialect dialect)
    {
        AddPartitionColumn operation = new AddPartitionColumn(new TableName(Optional.of(toLowerCase(OTHER_SCHEMA_NAME)), toLowerCase("table2")), new Column(toLowerCase("column2"), new HiveType(HiveTypes.STRING_TYPE)));
        String expectedSql = """
                -- Partition column adds not supported. ["schema123456"."table2"."column2"] will not be added.
                                        
                """;
        String expectedSummary = "Added partition column: [column2], of type: [string], to table: [schema123456.table2]";

        testOperation(NO_CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testDropPartitionColumnWithoutSchema(Dialect dialect)
    {
        DropPartitionColumn operation = new DropPartitionColumn(new TableName(Optional.empty(), toLowerCase("table3")), toLowerCase("column3"));
        String expectedSql = """
                ALTER TABLE "catalog123"."schema123"."table3"
                    DROP COLUMN "column3";
                                        
                """;
        String expectedSummary = "Dropped partition column: [column3], from table: [table3]";

        testOperation(CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testDropPartitionColumnWithCatalogName(Dialect dialect)
    {
        DropPartitionColumn operation = new DropPartitionColumn(new TableName(Optional.of(toLowerCase(OTHER_SCHEMA_NAME)), toLowerCase("table3")), toLowerCase("column3"));
        String expectedSql = """
                ALTER TABLE "catalog123"."schema123456"."table3"
                    DROP COLUMN "column3";
                                        
                """;
        String expectedSummary = "Dropped partition column: [column3], from table: [schema123456.table3]";

        testOperation(CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testDropPartitionColumnWithoutCatalogName(Dialect dialect)
    {
        DropPartitionColumn operation = new DropPartitionColumn(new TableName(Optional.of(toLowerCase(OTHER_SCHEMA_NAME)), toLowerCase("table3")), toLowerCase("column3"));
        String expectedSql = """
                ALTER TABLE "schema123456"."table3"
                    DROP COLUMN "column3";
                                        
                """;
        String expectedSummary = "Dropped partition column: [column3], from table: [schema123456.table3]";

        testOperation(NO_CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testRenamePartitionColumnWithoutSchema(Dialect dialect)
    {
        RenamePartitionColumn operation = new RenamePartitionColumn(new TableName(Optional.empty(), toLowerCase("table3")), toLowerCase("rename_me"), toLowerCase("new_name"));
        String expectedSql = """
                -- Partition column renames not supported. ["catalog123"."schema123"."table3"."rename_me"] will not be renamed.
                                        
                """;
        String expectedSummary = "Nothing changed, partition column renames not supported";

        testOperation(CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testRenamePartitionColumnWithCatalogName(Dialect dialect)
    {
        RenamePartitionColumn operation = new RenamePartitionColumn(new TableName(Optional.of(toLowerCase(OTHER_SCHEMA_NAME)), toLowerCase("table3")), toLowerCase("rename_me"), toLowerCase("new_name"));
        String expectedSql = """
                -- Partition column renames not supported. ["catalog123"."schema123456"."table3"."rename_me"] will not be renamed.
                                        
                """;
        String expectedSummary = "Nothing changed, partition column renames not supported";

        testOperation(CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testRenamePartitionColumnWithoutCatalogName(Dialect dialect)
    {
        RenamePartitionColumn operation = new RenamePartitionColumn(new TableName(Optional.of(toLowerCase(OTHER_SCHEMA_NAME)), toLowerCase("table3")), toLowerCase("rename_me"), toLowerCase("new_name"));
        String expectedSql = """
                -- Partition column renames not supported. ["schema123456"."table3"."rename_me"] will not be renamed.
                                        
                """;
        String expectedSummary = "Nothing changed, partition column renames not supported";

        testOperation(NO_CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testAddPartitionValueWithoutSchema(Dialect dialect)
    {
        AddPartitionValue operation = new AddPartitionValue(
                new TableName(Optional.empty(), toLowerCase("table4")),
                ImmutableList.of(new Column(toLowerCase("country"), new HiveType(HiveTypes.STRING_TYPE))),
                new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy2/country=poland"), ImmutableMap.of(toLowerCase("country"), "poland")));
        String expectedSql = """
                CALL catalog123.system.register_partition(
                    schema_name => 'schema123',
                    table_name => 'table4',
                    partition_columns => ARRAY['country'],
                    partition_values => ARRAY['poland'],
                    location => 's3://dummy2/country=poland/'
                );
                                        
                """;
        String expectedSummary = "Added [1] partition values from path: [s3://dummy2/country=poland/], to table: [table4]";

        testOperation(CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testAddPartitionValueWithCatalogName(Dialect dialect)
    {
        AddPartitionValue operation = new AddPartitionValue(
                new TableName(Optional.of(toLowerCase(OTHER_SCHEMA_NAME)), toLowerCase("table4")),
                ImmutableList.of(new Column(toLowerCase("country"), new HiveType(HiveTypes.STRING_TYPE))),
                new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy2/country=poland"), ImmutableMap.of(toLowerCase("country"), "poland")));
        String expectedSql = """
                CALL catalog123.system.register_partition(
                    schema_name => 'schema123456',
                    table_name => 'table4',
                    partition_columns => ARRAY['country'],
                    partition_values => ARRAY['poland'],
                    location => 's3://dummy2/country=poland/'
                );
                                        
                """;
        String expectedSummary = "Added [1] partition values from path: [s3://dummy2/country=poland/], to table: [schema123456.table4]";

        testOperation(CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testAddPartitionValueWithoutCatalogName(Dialect dialect)
    {
        AddPartitionValue operation = new AddPartitionValue(
                new TableName(Optional.of(toLowerCase(OTHER_SCHEMA_NAME)), toLowerCase("table4")),
                ImmutableList.of(new Column(toLowerCase("country"), new HiveType(HiveTypes.STRING_TYPE))),
                new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy2/country=poland"), ImmutableMap.of(toLowerCase("country"), "poland")));
        String expectedSql = """
                CALL system.register_partition(
                    schema_name => 'schema123456',
                    table_name => 'table4',
                    partition_columns => ARRAY['country'],
                    partition_values => ARRAY['poland'],
                    location => 's3://dummy2/country=poland/'
                );
                                        
                """;
        String expectedSummary = "Added [1] partition values from path: [s3://dummy2/country=poland/], to table: [schema123456.table4]";

        testOperation(NO_CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testDropPartitionValueWithoutSchema(Dialect dialect)
    {
        DropPartitionValue operation = new DropPartitionValue(
                new TableName(Optional.empty(), toLowerCase("table5")),
                ImmutableList.of(new Column(toLowerCase("date"), new HiveType(HiveTypes.HIVE_DATE))),
                new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy2/date=2022-03"), ImmutableMap.of(toLowerCase("date"), "2022-03")));
        String expectedSql = """
                CALL catalog123.system.unregister_partition(
                    schema_name => 'schema123',
                    table_name => 'table5',
                    partition_columns => ARRAY['date'],
                    partition_values => ARRAY['2022-03']
                );
                                        
                """;
        String expectedSummary = "Dropped [1] partition values from path: [s3://dummy2/date=2022-03/], from table: [table5]";

        testOperation(CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testDropPartitionValueWithCatalogName(Dialect dialect)
    {
        DropPartitionValue operation = new DropPartitionValue(
                new TableName(Optional.of(toLowerCase(OTHER_SCHEMA_NAME)), toLowerCase("table5")),
                ImmutableList.of(new Column(toLowerCase("date"), new HiveType(HiveTypes.HIVE_DATE))),
                new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy2/date=2022-03"), ImmutableMap.of(toLowerCase("date"), "2022-03")));
        String expectedSql = """
                CALL catalog123.system.unregister_partition(
                    schema_name => 'schema123456',
                    table_name => 'table5',
                    partition_columns => ARRAY['date'],
                    partition_values => ARRAY['2022-03']
                );
                                        
                """;
        String expectedSummary = "Dropped [1] partition values from path: [s3://dummy2/date=2022-03/], from table: [schema123456.table5]";

        testOperation(CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testDropPartitionValueWithoutCatalogName(Dialect dialect)
    {
        DropPartitionValue operation = new DropPartitionValue(
                new TableName(Optional.of(toLowerCase(OTHER_SCHEMA_NAME)), toLowerCase("table5")),
                ImmutableList.of(new Column(toLowerCase("date"), new HiveType(HiveTypes.HIVE_DATE))),
                new DiscoveredPartitionValues(ensureEndsWithSlash("s3://dummy2/date=2022-03"), ImmutableMap.of(toLowerCase("date"), "2022-03")));

        String expectedSql = """
                CALL system.unregister_partition(
                    schema_name => 'schema123456',
                    table_name => 'table5',
                    partition_columns => ARRAY['date'],
                    partition_values => ARRAY['2022-03']
                );
                                        
                """;
        String expectedSummary = "Dropped [1] partition values from path: [s3://dummy2/date=2022-03/], from table: [schema123456.table5]";
        testOperation(NO_CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testCreateSchemaWithCatalogName(Dialect dialect)
    {
        CreateSchema operation = new CreateSchema(ensureEndsWithSlash("s3://dummy"), toLowerCase("test_schema"));
        String expectedSql = """
                CREATE SCHEMA IF NOT EXISTS "catalog123"."test_schema" WITH (location = 's3://dummy/');

                """;
        String expectedSummary = "Created schema: [test_schema], with location: [s3://dummy/]";

        testOperation(CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testCreateSchemaWithoutCatalogName(Dialect dialect)
    {
        CreateSchema operation = new CreateSchema(ensureEndsWithSlash("s3://dummy"), toLowerCase("test_schema"));
        String expectedSql = """
                CREATE SCHEMA IF NOT EXISTS "test_schema" WITH (location = 's3://dummy/');

                """;
        String expectedSummary = "Created schema: [test_schema], with location: [s3://dummy/]";

        testOperation(NO_CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testGenerateComment(Dialect dialect)
    {
        Comment operation = new Comment("Hello world");
        String expectedSql = """
                -- Hello world

                """;

        testOperation(NO_CATALOG_NAME, operation, expectedSql, "Hello world", dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testDropTableWithoutSchema(Dialect dialect)
    {
        DropTable operation = new DropTable(new TableName(Optional.empty(), toLowerCase("test_table")));
        String expectedSql = """
                DROP TABLE "catalog123"."schema123"."test_table";

                """;
        String expectedSummary = "Dropped table: [test_table]";

        testOperation(CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testDropTableWithCatalogName(Dialect dialect)
    {
        DropTable operation = new DropTable(new TableName(Optional.of(toLowerCase(OTHER_SCHEMA_NAME)), toLowerCase("test_table")));
        String expectedSql = """
                DROP TABLE "catalog123"."schema123456"."test_table";

                """;
        String expectedSummary = "Dropped table: [schema123456.test_table]";

        testOperation(CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testDropTableWithoutCatalogName(Dialect dialect)
    {
        DropTable operation = new DropTable(new TableName(Optional.of(toLowerCase(OTHER_SCHEMA_NAME)), toLowerCase("test_table")));
        String expectedSql = """
                DROP TABLE "schema123456"."test_table";

                """;
        String expectedSummary = "Dropped table: [schema123456.test_table]";

        testOperation(NO_CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testAddColumnWithoutSchema(Dialect dialect)
    {
        AddColumn operation = new AddColumn(new TableName(Optional.empty(), toLowerCase("test_table")), new Column(toLowerCase("col1"), new HiveType(HiveTypes.HIVE_DATE)));
        String expectedSql = """
                ALTER TABLE "catalog123"."schema123"."test_table"
                    ADD COLUMN "col1" date;

                """;
        String expectedSummary = "Added column: [col1], of type: [date], to table: [test_table]";

        testOperation(CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testAddColumnWithCatalogName(Dialect dialect)
    {
        AddColumn operation = new AddColumn(new TableName(Optional.of(toLowerCase(OTHER_SCHEMA_NAME)), toLowerCase("test_table")), new Column(toLowerCase("col1"), new HiveType(HiveTypes.HIVE_DATE)));
        String expectedSql = """
                ALTER TABLE "catalog123"."schema123456"."test_table"
                    ADD COLUMN "col1" date;

                """;
        String expectedSummary = "Added column: [col1], of type: [date], to table: [schema123456.test_table]";

        testOperation(CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testAddColumnWithoutCatalogName(Dialect dialect)
    {
        AddColumn operation = new AddColumn(new TableName(Optional.of(toLowerCase(OTHER_SCHEMA_NAME)), toLowerCase("test_table")), new Column(toLowerCase("col1"), new HiveType(HiveTypes.HIVE_DATE)));
        String expectedSql = """
                ALTER TABLE "schema123456"."test_table"
                    ADD COLUMN "col1" date;

                """;
        String expectedSummary = "Added column: [col1], of type: [date], to table: [schema123456.test_table]";

        testOperation(NO_CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testDropColumnWithoutSchema(Dialect dialect)
    {
        DropColumn operation = new DropColumn(new TableName(Optional.empty(), toLowerCase("test_table")), toLowerCase("col1"));
        String expectedSql = """
                ALTER TABLE "catalog123"."schema123"."test_table"
                    DROP COLUMN "col1";

                """;
        String expectedSummary = "Dropped column: [col1], from table: [test_table]";

        testOperation(CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testDropColumnWithCatalogName(Dialect dialect)
    {
        DropColumn operation = new DropColumn(new TableName(Optional.of(toLowerCase(OTHER_SCHEMA_NAME)), toLowerCase("test_table")), toLowerCase("col1"));
        String expectedSql = """
                ALTER TABLE "catalog123"."schema123456"."test_table"
                    DROP COLUMN "col1";

                """;
        String expectedSummary = "Dropped column: [col1], from table: [schema123456.test_table]";

        testOperation(CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testDropColumnWithoutCatalogName(Dialect dialect)
    {
        DropColumn operation = new DropColumn(new TableName(Optional.of(toLowerCase(OTHER_SCHEMA_NAME)), toLowerCase("test_table")), toLowerCase("col1"));
        String expectedSql = """
                ALTER TABLE "schema123456"."test_table"
                    DROP COLUMN "col1";

                """;
        String expectedSummary = "Dropped column: [col1], from table: [schema123456.test_table]";

        testOperation(NO_CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testRenameColumnWithoutSchema(Dialect dialect)
    {
        RenameColumn operation = new RenameColumn(new TableName(Optional.empty(), toLowerCase("test_table")), toLowerCase("test_column"), toLowerCase("new_column"));

        String expectedSql = """
                ALTER TABLE "catalog123"."schema123"."test_table"
                    RENAME COLUMN "test_column" TO "new_column";

                """;
        String expectedSummary = "Renamed column: [test_column] to: [new_column], in table: [test_table]";
        testOperation(CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testRenameColumnWithCatalogName(Dialect dialect)
    {
        RenameColumn operation = new RenameColumn(new TableName(Optional.of(toLowerCase(OTHER_SCHEMA_NAME)), toLowerCase("test_table")), toLowerCase("test_column"), toLowerCase("new_column"));

        String expectedSql = """
                ALTER TABLE "catalog123"."schema123456"."test_table"
                    RENAME COLUMN "test_column" TO "new_column";

                """;
        String expectedSummary = "Renamed column: [test_column] to: [new_column], in table: [schema123456.test_table]";
        testOperation(CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testRenameColumnWithoutCatalogName(Dialect dialect)
    {
        RenameColumn operation = new RenameColumn(new TableName(Optional.of(toLowerCase(OTHER_SCHEMA_NAME)), toLowerCase("test_table")), toLowerCase("test_column"), toLowerCase("new_column"));
        String expectedSql = """
                ALTER TABLE "schema123456"."test_table"
                    RENAME COLUMN "test_column" TO "new_column";

                """;
        String expectedSummary = "Renamed column: [test_column] to: [new_column], in table: [schema123456.test_table]";

        testOperation(NO_CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testAddBucketWithoutSchema(Dialect dialect)
    {
        AddBucket operation = new AddBucket(new TableName(Optional.empty(), toLowerCase("test_table")), toLowerCase("test_bucket"));
        String expectedSql = """
                -- Bucket adds not supported. ["catalog123"."schema123"."test_table"."test_bucket"] will not be added.

                """;
        String expectedSummary = "Nothing changed, bucket adds not supported";

        testOperation(CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testAddBucketWithCatalogName(Dialect dialect)
    {
        AddBucket operation = new AddBucket(new TableName(Optional.of(toLowerCase(OTHER_SCHEMA_NAME)), toLowerCase("test_table")), toLowerCase("test_bucket"));
        String expectedSql = """
                -- Bucket adds not supported. ["catalog123"."schema123456"."test_table"."test_bucket"] will not be added.

                """;
        String expectedSummary = "Nothing changed, bucket adds not supported";

        testOperation(CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testDropBucketWithoutSchema(Dialect dialect)
    {
        AddBucket operation = new AddBucket(new TableName(Optional.empty(), toLowerCase("test_table")), toLowerCase("test_bucket"));
        String expectedSql = """
                -- Bucket adds not supported. ["schema123"."test_table"."test_bucket"] will not be added.

                """;
        String expectedSummary = "Nothing changed, bucket adds not supported";

        testOperation(NO_CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testDropBucketWithoutCatalogName(Dialect dialect)
    {
        AddBucket operation = new AddBucket(new TableName(Optional.of(toLowerCase(OTHER_SCHEMA_NAME)), toLowerCase("test_table")), toLowerCase("test_bucket"));
        String expectedSql = """
                -- Bucket adds not supported. ["schema123456"."test_table"."test_bucket"] will not be added.

                """;
        String expectedSummary = "Nothing changed, bucket adds not supported";

        testOperation(NO_CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testRegisterTableWithoutSchema(Dialect dialect)
    {
        RegisterTable operation = new RegisterTable(SlashEndedPath.ensureEndsWithSlash("s3://dummy"), new TableName(Optional.empty(), toLowerCase("test_table")));

        String expectedSql = """
                CALL catalog123.system.register_table(schema_name => 'schema123', table_name => 'test_table', table_location => 's3://dummy/');

                """;
        String expectedSummary = "Registered table: [test_table], with location: [s3://dummy/]";
        testOperation(CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testRegisterTableWithCatalogName(Dialect dialect)
    {
        RegisterTable operation = new RegisterTable(SlashEndedPath.ensureEndsWithSlash("s3://dummy"), new TableName(Optional.of(toLowerCase(OTHER_SCHEMA_NAME)), toLowerCase("test_table")));
        String expectedSql = """
                CALL catalog123.system.register_table(schema_name => 'schema123456', table_name => 'test_table', table_location => 's3://dummy/');

                """;
        String expectedSummary = "Registered table: [schema123456.test_table], with location: [s3://dummy/]";
        testOperation(CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testRegisterTableWithoutCatalogName(Dialect dialect)
    {
        RegisterTable operation = new RegisterTable(SlashEndedPath.ensureEndsWithSlash("s3://dummy"), new TableName(Optional.of(toLowerCase(OTHER_SCHEMA_NAME)), toLowerCase("test_table")));
        String expectedSql = """
                CALL system.register_table(schema_name => 'schema123456', table_name => 'test_table', table_location => 's3://dummy/');

                """;
        String expectedSummary = "Registered table: [schema123456.test_table], with location: [s3://dummy/]";

        testOperation(NO_CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testUnregisterTableWithoutSchema(Dialect dialect)
    {
        UnregisterTable operation = new UnregisterTable(new TableName(Optional.empty(), toLowerCase("test_table")));
        String expectedSql = """
                CALL catalog123.system.unregister_table(schema_name => 'schema123', table_name => 'test_table');

                """;
        String expectedSummary = "Unregister table: [test_table]";
        testOperation(CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testUnregisterTableWithCatalogName(Dialect dialect)
    {
        UnregisterTable operation = new UnregisterTable(new TableName(Optional.of(toLowerCase(OTHER_SCHEMA_NAME)), toLowerCase("test_table")));
        String expectedSql = """
                CALL catalog123.system.unregister_table(schema_name => 'schema123456', table_name => 'test_table');

                """;
        String expectedSummary = "Unregister table: [schema123456.test_table]";
        testOperation(CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    public void testUnregisterTableWithoutCatalogName(Dialect dialect)
    {
        UnregisterTable operation = new UnregisterTable(new TableName(Optional.of(toLowerCase(OTHER_SCHEMA_NAME)), toLowerCase("test_table")));
        String expectedSql = """
                CALL system.unregister_table(schema_name => 'schema123456', table_name => 'test_table');

                """;
        String expectedSummary = "Unregister table: [schema123456.test_table]";

        testOperation(NO_CATALOG_NAME, operation, expectedSql, expectedSummary, dialect);
    }

    private void testOperation(Optional<String> catalogName, Operation operation, String expectedSql, String expectedSummary, Dialect dialect)
    {
        SimpleWriter writer = new SimpleWriter();
        SqlGenerator sqlGenerator = new SqlGenerator(toLowerCase(DEFAULT_SCHEMA_NAME), new GenerateOptions(DEFAULT_SCHEMA_NAME, 10, true, catalogName), writer, dialect);
        sqlGenerator.apply(operation);
        assertThat(writer.toString())
                .isEqualTo(expectedSql);
        assertThat(operation.operationSummary()).isEqualTo(expectedSummary);
    }
}
