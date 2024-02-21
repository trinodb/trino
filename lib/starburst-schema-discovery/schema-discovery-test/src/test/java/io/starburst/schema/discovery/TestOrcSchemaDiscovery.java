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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.starburst.schema.discovery.internal.HiveTypes;
import io.starburst.schema.discovery.models.DiscoveredColumns;
import io.trino.plugin.hive.type.TypeInfo;
import org.junit.jupiter.api.Test;

import static io.starburst.schema.discovery.Util.column;
import static io.starburst.schema.discovery.Util.orcSchemaDiscovery;
import static io.starburst.schema.discovery.Util.struct;
import static io.starburst.schema.discovery.Util.toColumn;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_BINARY;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_BOOLEAN;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_BYTE;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_DATE;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_DOUBLE;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_FLOAT;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_INT;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_LONG;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_SHORT;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_STRING;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_TIMESTAMP;
import static io.starburst.schema.discovery.internal.HiveTypes.arrayType;
import static io.starburst.schema.discovery.internal.HiveTypes.mapType;
import static io.starburst.schema.discovery.internal.HiveTypes.structType;
import static io.trino.plugin.hive.type.TypeInfoFactory.getCharTypeInfo;
import static io.trino.plugin.hive.type.TypeInfoFactory.getDecimalTypeInfo;
import static org.assertj.core.api.Assertions.assertThat;

public class TestOrcSchemaDiscovery
{
    @Test
    public void testUsers()
    {
        DiscoveredColumns schemaColumns = orcSchemaDiscovery.discoverColumns(Util.testFile("orc/users.orc"), ImmutableMap.of());
        assertThat(schemaColumns.columns()).containsExactly(toColumn("name", HIVE_STRING),
                toColumn("favorite_color", HIVE_STRING),
                toColumn("favorite_numbers", arrayType(HIVE_INT)));
    }

    @Test
    public void testDateSnappy()
    {
        DiscoveredColumns schemaColumns = orcSchemaDiscovery.discoverColumns(Util.testFile("orc/before-1582-date-v2-4.snappy.orc"), ImmutableMap.of());
        assertThat(schemaColumns.columns()).containsExactly(toColumn("dt", HIVE_DATE));
    }

    @Test
    public void testTimestampSnappy()
    {
        DiscoveredColumns schemaColumns = orcSchemaDiscovery.discoverColumns(Util.testFile("orc/before-1582-ts-v2-4.snappy.orc"), ImmutableMap.of());
        assertThat(schemaColumns.columns()).containsExactly(toColumn("ts", HIVE_TIMESTAMP));
    }

    @Test
    public void testFromTrino()
    {
        DiscoveredColumns schemaColumns = orcSchemaDiscovery.discoverColumns(Util.testFile("orc/from-trino.orc"), ImmutableMap.of());
        TypeInfo structType = structType(ImmutableList.of("s_string", "s_double"), ImmutableList.of(HIVE_STRING, HIVE_DOUBLE));
        assertThat(schemaColumns.columns()).containsExactly(toColumn("_col0", HIVE_STRING),
                toColumn("_col1", HIVE_BYTE),
                toColumn("_col2", HIVE_SHORT),
                toColumn("_col3", HIVE_INT),
                toColumn("_col4", HIVE_LONG),
                toColumn("_col5", HIVE_FLOAT),
                toColumn("_col6", HIVE_DOUBLE),
                toColumn("_col7", HIVE_BOOLEAN),
                toColumn("_col8", HIVE_TIMESTAMP),
                toColumn("_col9", HIVE_BINARY),
                toColumn("_col10", HIVE_DATE),
                toColumn("_col11", HIVE_STRING),
                toColumn("_col12", getCharTypeInfo(25)),
                toColumn("_col13", mapType(HIVE_STRING, HIVE_STRING)),
                toColumn("_col14", arrayType(HIVE_STRING)),
                toColumn("_col15", arrayType(HIVE_TIMESTAMP)),
                toColumn("_col16", arrayType(structType)),
                toColumn("_col17", structType),
                toColumn("_col18", mapType(HIVE_INT, arrayType(structType))));
    }

    @Test
    public void testWithUuidAndTime()
    {
        DiscoveredColumns schemaColumns = orcSchemaDiscovery.discoverColumns(Util.testFile("orc/orc_time_uuid.orc"), ImmutableMap.of());
        assertThat(schemaColumns.columns()).containsExactly(
                column("type_boolean", HiveTypes.HIVE_BOOLEAN),
                column("type_integer", HiveTypes.HIVE_INT),
                column("type_bigint", HiveTypes.HIVE_LONG),
                column("type_real", HiveTypes.HIVE_FLOAT),
                column("type_double", HiveTypes.HIVE_DOUBLE),
                column("type_decimal", getDecimalTypeInfo(38, 0)),
                column("type_varchar", HiveTypes.HIVE_STRING),
                column("type_varbinary", HiveTypes.HIVE_BINARY),
                column("type_time6", HiveTypes.HIVE_LONG),
                column("type_timestamp6", HiveTypes.HIVE_TIMESTAMP),
                column("type_array", Util.arrayType(HiveTypes.HIVE_STRING)),
                column("type_map", Util.mapType(HiveTypes.HIVE_STRING, HiveTypes.HIVE_STRING)),
                column("type_row", struct("row_nested", HiveTypes.HIVE_STRING)),
                column("type_uuid", HiveTypes.HIVE_BINARY));
    }

    @Test
    public void testWithChar()
    {
        DiscoveredColumns schemaColumns = orcSchemaDiscovery.discoverColumns(Util.testFile("orc/char.orc"), ImmutableMap.of());
        assertThat(schemaColumns.columns()).containsExactly(
                column("type_char", getCharTypeInfo(1)));
    }
}
