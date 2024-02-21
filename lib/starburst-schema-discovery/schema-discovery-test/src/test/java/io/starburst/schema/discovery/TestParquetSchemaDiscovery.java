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
import io.starburst.schema.discovery.internal.HiveTypes;
import io.starburst.schema.discovery.models.DiscoveredColumns;
import io.trino.plugin.hive.type.DecimalTypeInfo;
import io.trino.plugin.hive.type.TypeInfo;
import org.junit.jupiter.api.Test;

import static io.starburst.schema.discovery.Util.arrayType;
import static io.starburst.schema.discovery.Util.column;
import static io.starburst.schema.discovery.Util.mapType;
import static io.starburst.schema.discovery.Util.parquetSchemaDiscovery;
import static io.starburst.schema.discovery.Util.struct;
import static io.starburst.schema.discovery.Util.testFile;
import static org.assertj.core.api.Assertions.assertThat;

public class TestParquetSchemaDiscovery
{
    @Test
    public void testProtoStructWthArrayMany()
    {
        DiscoveredColumns schemaColumns = parquetSchemaDiscovery.discoverColumns(testFile("parquet/proto-struct-with-array-many.parquet"), ImmutableMap.of());
        TypeInfo structType = struct("one", HiveTypes.HIVE_STRING, "two", HiveTypes.HIVE_STRING, "three", HiveTypes.HIVE_STRING);
        assertThat(schemaColumns.columns()).containsExactly(column("inner", structType));
    }

    @Test
    public void testAnnotatedIntTypes()
    {
        DiscoveredColumns schemaColumns = parquetSchemaDiscovery.discoverColumns(testFile("parquet/annotated-int-types.parquet"), ImmutableMap.of());
        assertThat(schemaColumns.columns()).containsExactly(
                column("nationkey_bigint", HiveTypes.HIVE_LONG),
                column("nationkey_int", HiveTypes.HIVE_INT),
                column("nationkey_short", HiveTypes.HIVE_SHORT),
                column("nationkey_byte", HiveTypes.HIVE_BYTE));
    }

    @Test
    public void testFromTrino()
    {
        DiscoveredColumns schemaColumns = parquetSchemaDiscovery.discoverColumns(testFile("parquet/from-trino.parquet"), ImmutableMap.of());
        TypeInfo structType = struct("s_string", HiveTypes.HIVE_STRING, "s_double", HiveTypes.HIVE_DOUBLE);
        assertThat(schemaColumns.columns()).containsExactly(
                column("t_string", HiveTypes.HIVE_STRING),
                column("t_tinyint", HiveTypes.HIVE_INT),
                column("t_smallint", HiveTypes.HIVE_INT),
                column("t_int", HiveTypes.HIVE_INT),
                column("t_bigint", HiveTypes.HIVE_LONG),
                column("t_float", HiveTypes.HIVE_FLOAT),
                column("t_double", HiveTypes.HIVE_DOUBLE),
                column("t_boolean", HiveTypes.HIVE_BOOLEAN),
                column("t_timestamp", HiveTypes.HIVE_TIMESTAMP),
                column("t_binary", HiveTypes.HIVE_BINARY),
                column("t_date", HiveTypes.HIVE_DATE),
                column("t_varchar", HiveTypes.HIVE_STRING),
                column("t_char", HiveTypes.HIVE_STRING),
                column("t_map", mapType(HiveTypes.HIVE_STRING, HiveTypes.HIVE_STRING)),
                column("t_array_string", arrayType(HiveTypes.HIVE_STRING)),
                column("t_array_timestamp", arrayType(HiveTypes.HIVE_TIMESTAMP)),
                column("t_array_struct", arrayType(structType)),
                column("t_struct", structType),
                column("t_complex", mapType(HiveTypes.HIVE_INT, arrayType(structType))));
    }

    @Test
    public void testWithUuidAndTime()
    {
        DiscoveredColumns schemaColumns = parquetSchemaDiscovery.discoverColumns(testFile("parquet/parquet_time_uuid.parquet"), ImmutableMap.of());
        assertThat(schemaColumns.columns()).containsExactly(
                column("type_boolean", HiveTypes.HIVE_BOOLEAN),
                column("type_integer", HiveTypes.HIVE_INT),
                column("type_bigint", HiveTypes.HIVE_LONG),
                column("type_real", HiveTypes.HIVE_FLOAT),
                column("type_double", HiveTypes.HIVE_DOUBLE),
                column("type_decimal", new DecimalTypeInfo(38, 0)),
                column("type_varchar", HiveTypes.HIVE_STRING),
                column("type_varbinary", HiveTypes.HIVE_BINARY),
                column("type_time6", HiveTypes.HIVE_LONG),
                column("type_timestamp6", HiveTypes.HIVE_TIMESTAMP),
                column("type_array", arrayType(HiveTypes.HIVE_STRING)),
                column("type_map", mapType(HiveTypes.HIVE_STRING, HiveTypes.HIVE_STRING)),
                column("type_row", struct("row_nested", HiveTypes.HIVE_STRING)),
                column("type_uuid", HiveTypes.HIVE_STRING));
    }

    @Test
    public void testWithChar()
    {
        DiscoveredColumns schemaColumns = parquetSchemaDiscovery.discoverColumns(Util.testFile("parquet/char.parquet"), ImmutableMap.of());
        assertThat(schemaColumns.columns()).containsExactly(
                column("type_char", HiveTypes.HIVE_STRING));
    }
}
