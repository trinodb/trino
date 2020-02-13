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
package io.prestosql.plugin.bigquery;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.TimeWithTimeZoneType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

@Test
public class TestTypeConversions
{
    @Test
    public void testConvertBooleanField()
    {
        assertSimpleFieldTypeConversion(LegacySQLTypeName.BOOLEAN, BooleanType.BOOLEAN);
    }

    @Test
    public void testConvertBytesField()
    {
        assertSimpleFieldTypeConversion(LegacySQLTypeName.BYTES, VarbinaryType.VARBINARY);
    }

    @Test
    public void testConvertDateField()
    {
        assertSimpleFieldTypeConversion(LegacySQLTypeName.DATE, DateType.DATE);
    }

    @Test
    public void testConvertDateTimeField()
    {
        assertSimpleFieldTypeConversion(LegacySQLTypeName.DATETIME, VarcharType.VARCHAR);
    }

    @Test
    public void testConvertFloatField()
    {
        assertSimpleFieldTypeConversion(LegacySQLTypeName.FLOAT, DoubleType.DOUBLE);
    }

    @Test
    public void testConvertGeographyField()
    {
        assertSimpleFieldTypeConversion(LegacySQLTypeName.GEOGRAPHY, VarcharType.VARCHAR);
    }

    @Test
    public void testConvertIntegerField()
    {
        assertSimpleFieldTypeConversion(LegacySQLTypeName.INTEGER, BigintType.BIGINT);
    }

    @Test
    public void testConvertNumericField()
    {
        assertSimpleFieldTypeConversion(LegacySQLTypeName.NUMERIC, DecimalType.createDecimalType(38, 9));
    }

    @Test
    public void testConvertStringField()
    {
        assertSimpleFieldTypeConversion(LegacySQLTypeName.STRING, VarcharType.VARCHAR);
    }

    @Test
    public void testConvertTimeField()
    {
        assertSimpleFieldTypeConversion(LegacySQLTypeName.TIME, TimeWithTimeZoneType.TIME_WITH_TIME_ZONE);
    }

    @Test
    public void testConvertTimestampField()
    {
        assertSimpleFieldTypeConversion(LegacySQLTypeName.TIMESTAMP, TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE);
    }

    @Test
    public void testConvertOneLevelRecordField()
    {
        Field field = Field.of(
                "rec",
                LegacySQLTypeName.RECORD,
                Field.of("sub_s", LegacySQLTypeName.STRING),
                Field.of("sub_i", LegacySQLTypeName.INTEGER));
        ColumnMetadata metadata = Conversions.toColumnMetadata(field);
        RowType targetType = RowType.rowType(
                RowType.field("sub_s", VarcharType.VARCHAR),
                RowType.field("sub_i", BigintType.BIGINT));
        assertThat(metadata.getType()).isEqualTo(targetType);
    }

    @Test
    public void testConvertTwoLevelsRecordField()
    {
        Field field = Field.of(
                "rec",
                LegacySQLTypeName.RECORD,
                Field.of("sub_rec", LegacySQLTypeName.RECORD,
                        Field.of("sub_sub_s", LegacySQLTypeName.STRING),
                        Field.of("sub_sub_i", LegacySQLTypeName.INTEGER)),
                Field.of("sub_s", LegacySQLTypeName.STRING),
                Field.of("sub_i", LegacySQLTypeName.INTEGER));
        ColumnMetadata metadata = Conversions.toColumnMetadata(field);
        RowType targetType = RowType.rowType(
                RowType.field("sub_rec", RowType.rowType(
                        RowType.field("sub_sub_s", VarcharType.VARCHAR),
                        RowType.field("sub_sub_i", BigintType.BIGINT))),
                RowType.field("sub_s", VarcharType.VARCHAR),
                RowType.field("sub_i", BigintType.BIGINT));
        assertThat(metadata.getType()).isEqualTo(targetType);
    }

    @Test
    public void testConvertStringArrayField()
    {
        Field field = Field.newBuilder("test", LegacySQLTypeName.STRING)
                .setMode(Field.Mode.REPEATED)
                .build();
        ColumnMetadata metadata = Conversions.toColumnMetadata(field);
        assertThat(metadata.getType()).isEqualTo(new ArrayType(VarcharType.VARCHAR));
    }

    void assertSimpleFieldTypeConversion(LegacySQLTypeName from, Type to)
    {
        ColumnMetadata metadata = Conversions.toColumnMetadata(createField(from));
        assertThat(metadata.getType()).isEqualTo(to);
    }

    private Field createField(LegacySQLTypeName type)
    {
        return Field.of("test", type);
    }

    @Test
    public void testConvertBooleanColumn()
    {
        assertSimpleColumnTypeConversion(LegacySQLTypeName.BOOLEAN, BooleanType.BOOLEAN);
    }

    @Test
    public void testConvertBytesColumn()
    {
        assertSimpleColumnTypeConversion(LegacySQLTypeName.BYTES, VarbinaryType.VARBINARY);
    }

    @Test
    public void testConvertDateColumn()
    {
        assertSimpleColumnTypeConversion(LegacySQLTypeName.DATE, DateType.DATE);
    }

    @Test
    public void testConvertDateTimeColumn()
    {
        assertSimpleColumnTypeConversion(LegacySQLTypeName.DATETIME, VarcharType.VARCHAR);
    }

    @Test
    public void testConvertFloatColumn()
    {
        assertSimpleColumnTypeConversion(LegacySQLTypeName.FLOAT, DoubleType.DOUBLE);
    }

    @Test
    public void testConvertGeographyColumn()
    {
        assertSimpleColumnTypeConversion(LegacySQLTypeName.GEOGRAPHY, VarcharType.VARCHAR);
    }

    @Test
    public void testConvertIntegerColumn()
    {
        assertSimpleColumnTypeConversion(LegacySQLTypeName.INTEGER, BigintType.BIGINT);
    }

    @Test
    public void testConvertNumericColumn()
    {
        assertSimpleColumnTypeConversion(LegacySQLTypeName.NUMERIC, DecimalType.createDecimalType(38, 9));
    }

    @Test
    public void testConvertStringColumn()
    {
        assertSimpleColumnTypeConversion(LegacySQLTypeName.STRING, VarcharType.VARCHAR);
    }

    @Test
    public void testConvertTimeColumn()
    {
        assertSimpleColumnTypeConversion(LegacySQLTypeName.TIME, TimeWithTimeZoneType.TIME_WITH_TIME_ZONE);
    }

    @Test
    public void testConvertTimestampColumn()
    {
        assertSimpleColumnTypeConversion(LegacySQLTypeName.TIMESTAMP, TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE);
    }

    @Test
    public void testConvertOneLevelRecordColumn()
    {
        BigQueryColumnHandle column = new BigQueryColumnHandle("rec", BigQueryType.RECORD, Field.Mode.NULLABLE, ImmutableList.of(
                new BigQueryColumnHandle("sub_s", BigQueryType.STRING, Field.Mode.NULLABLE, ImmutableList.of(), null),
                new BigQueryColumnHandle("sub_i", BigQueryType.INTEGER, Field.Mode.NULLABLE, ImmutableList.of(), null)
        ), null);
        ColumnMetadata metadata = column.getColumnMetadata();
        RowType targetType = RowType.rowType(
                RowType.field("sub_s", VarcharType.VARCHAR),
                RowType.field("sub_i", BigintType.BIGINT));
        assertThat(metadata.getType()).isEqualTo(targetType);
    }

    @Test
    public void testConvertTwoLevelsRecordColumn()
    {
        BigQueryColumnHandle column = new BigQueryColumnHandle("rec", BigQueryType.RECORD, Field.Mode.NULLABLE, ImmutableList.of(
                new BigQueryColumnHandle("sub_rec", BigQueryType.RECORD, Field.Mode.NULLABLE, ImmutableList.of(
                        new BigQueryColumnHandle("sub_sub_s", BigQueryType.STRING, Field.Mode.NULLABLE, ImmutableList.of(), null),
                        new BigQueryColumnHandle("sub_sub_i", BigQueryType.INTEGER, Field.Mode.NULLABLE, ImmutableList.of(), null)
                ), null),
                new BigQueryColumnHandle("sub_s", BigQueryType.STRING, Field.Mode.NULLABLE, ImmutableList.of(), null),
                new BigQueryColumnHandle("sub_i", BigQueryType.INTEGER, Field.Mode.NULLABLE, ImmutableList.of(), null)
        ), null);
        ColumnMetadata metadata = column.getColumnMetadata();
        RowType targetType = RowType.rowType(
                RowType.field("sub_rec", RowType.rowType(
                        RowType.field("sub_sub_s", VarcharType.VARCHAR),
                        RowType.field("sub_sub_i", BigintType.BIGINT))),
                RowType.field("sub_s", VarcharType.VARCHAR),
                RowType.field("sub_i", BigintType.BIGINT));
        assertThat(metadata.getType()).isEqualTo(targetType);
    }

    @Test
    public void testConvertStringArrayColumn()
    {
        BigQueryColumnHandle column = new BigQueryColumnHandle("test", BigQueryType.STRING, Field.Mode.REPEATED, ImmutableList.of(), null);
        ColumnMetadata metadata = column.getColumnMetadata();
        assertThat(metadata.getType()).isEqualTo(new ArrayType(VarcharType.VARCHAR));
    }

    void assertSimpleColumnTypeConversion(LegacySQLTypeName from, Type to)
    {
        ColumnMetadata metadata = createColumn(from).getColumnMetadata();
        assertThat(metadata.getType()).isEqualTo(to);
    }

    private BigQueryColumnHandle createColumn(LegacySQLTypeName type)
    {
        return new BigQueryColumnHandle("test", BigQueryType.valueOf(type.name()), Field.Mode.NULLABLE, ImmutableList.of(), null);
    }
}
