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
package io.trino.plugin.hive.s3select;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.type.TypeInfo;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveType.HIVE_INT;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.s3select.S3SelectRecordCursor.updateSplitSchema;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMNS;
import static org.apache.hadoop.hive.serde.serdeConstants.LIST_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_DDL;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestS3SelectRecordCursor
{
    private static final String LAZY_SERDE_CLASS_NAME = LazySimpleSerDe.class.getName();

    protected static final HiveColumnHandle ARTICLE_COLUMN = createBaseColumn("article", 1, HIVE_STRING, VARCHAR, REGULAR, Optional.empty());
    protected static final HiveColumnHandle AUTHOR_COLUMN = createBaseColumn("author", 1, HIVE_STRING, VARCHAR, REGULAR, Optional.empty());
    protected static final HiveColumnHandle DATE_ARTICLE_COLUMN = createBaseColumn("date_pub", 1, HIVE_INT, DATE, REGULAR, Optional.empty());
    protected static final HiveColumnHandle QUANTITY_COLUMN = createBaseColumn("quantity", 1, HIVE_INT, INTEGER, REGULAR, Optional.empty());
    private static final HiveColumnHandle[] DEFAULT_TEST_COLUMNS = {ARTICLE_COLUMN, AUTHOR_COLUMN, DATE_ARTICLE_COLUMN, QUANTITY_COLUMN};

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenSerialDDLHasNoColumns()
    {
        String ddlSerializationValue = "struct article { }";
        assertThatThrownBy(() -> buildSplitSchema(ddlSerializationValue, DEFAULT_TEST_COLUMNS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Invalid Thrift DDL struct article \\{ \\}");
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenSerialDDLNotStartingWithStruct()
    {
        String ddlSerializationValue = "foo article { varchar article varchar }";
        assertThatThrownBy(() -> buildSplitSchema(ddlSerializationValue, DEFAULT_TEST_COLUMNS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Thrift DDL should start with struct");
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenSerialDDLNotStartingWithStruct2()
    {
        String ddlSerializationValue = "struct article {varchar article}";
        assertThatThrownBy(() -> buildSplitSchema(ddlSerializationValue, DEFAULT_TEST_COLUMNS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Invalid Thrift DDL struct article \\{varchar article\\}");
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenMissingOpenStartStruct()
    {
        String ddlSerializationValue = "struct article varchar article varchar }";
        assertThatThrownBy(() -> buildSplitSchema(ddlSerializationValue, DEFAULT_TEST_COLUMNS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Invalid Thrift DDL struct article varchar article varchar \\}");
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenDDlFormatNotCorrect()
    {
        String ddlSerializationValue = "struct article{varchar article varchar author date date_pub int quantity";
        assertThatThrownBy(() -> buildSplitSchema(ddlSerializationValue, DEFAULT_TEST_COLUMNS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Invalid Thrift DDL struct article\\{varchar article varchar author date date_pub int quantity");
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenEndOfStructNotFound()
    {
        String ddlSerializationValue = "struct article { varchar article varchar author date date_pub int quantity ";
        assertThatThrownBy(() -> buildSplitSchema(ddlSerializationValue, DEFAULT_TEST_COLUMNS))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Invalid Thrift DDL struct article \\{ varchar article varchar author date date_pub int quantity ");
    }

    @Test
    public void shouldFilterColumnsWhichDoesNotMatchInTheHiveTable()
    {
        String ddlSerializationValue = "struct article { varchar address varchar company date date_pub int quantity}";
        String expectedDDLSerialization = "struct article { date date_pub, int quantity}";
        assertEquals(buildSplitSchema(ddlSerializationValue, DEFAULT_TEST_COLUMNS),
                buildExpectedProperties(expectedDDLSerialization, DEFAULT_TEST_COLUMNS));
    }

    @Test
    public void shouldReturnOnlyQuantityColumnInTheDDl()
    {
        String ddlSerializationValue = "struct article { varchar address varchar company date date_pub int quantity}";
        String expectedDDLSerialization = "struct article { int quantity}";
        assertEquals(buildSplitSchema(ddlSerializationValue, ARTICLE_COLUMN, QUANTITY_COLUMN),
                buildExpectedProperties(expectedDDLSerialization, ARTICLE_COLUMN, QUANTITY_COLUMN));
    }

    @Test
    public void shouldReturnProperties()
    {
        String ddlSerializationValue = "struct article { varchar article varchar author date date_pub int quantity}";
        String expectedDDLSerialization = "struct article { varchar article, varchar author, date date_pub, int quantity}";
        assertEquals(buildSplitSchema(ddlSerializationValue, DEFAULT_TEST_COLUMNS),
                buildExpectedProperties(expectedDDLSerialization, DEFAULT_TEST_COLUMNS));
    }

    @Test
    public void shouldReturnPropertiesWithoutDoubleCommaInColumnsNameLastColumnNameWithEndStruct()
    {
        String ddlSerializationValue = "struct article { varchar article, varchar author, date date_pub, int quantity}";
        String expectedDDLSerialization = "struct article { varchar article, varchar author, date date_pub, int quantity}";
        assertEquals(buildSplitSchema(ddlSerializationValue, DEFAULT_TEST_COLUMNS),
                buildExpectedProperties(expectedDDLSerialization, DEFAULT_TEST_COLUMNS));
    }

    @Test
    public void shouldReturnPropertiesWithoutDoubleCommaInColumnsNameLastColumnNameWithoutEndStruct()
    {
        String ddlSerializationValue = "struct article { varchar article, varchar author, date date_pub, int quantity }";
        String expectedDDLSerialization = "struct article { varchar article, varchar author, date date_pub, int quantity}";
        assertEquals(buildSplitSchema(ddlSerializationValue, DEFAULT_TEST_COLUMNS),
                buildExpectedProperties(expectedDDLSerialization, DEFAULT_TEST_COLUMNS));
    }

    @Test
    public void shouldOnlyGetColumnTypeFromHiveObjectAndNotFromDDLSerialLastColumnNameWithEndStruct()
    {
        String ddlSerializationValue = "struct article { int article, double author, xxxx date_pub, int quantity}";
        String expectedDDLSerialization = "struct article { int article, double author, xxxx date_pub, int quantity}";
        assertEquals(buildSplitSchema(ddlSerializationValue, DEFAULT_TEST_COLUMNS),
                buildExpectedProperties(expectedDDLSerialization, DEFAULT_TEST_COLUMNS));
    }

    @Test
    public void shouldOnlyGetColumnTypeFromHiveObjectAndNotFromDDLSerialLastColumnNameWithoutEndStruct()
    {
        String ddlSerializationValue = "struct article { int article, double author, xxxx date_pub, int quantity }";
        String expectedDDLSerialization = "struct article { int article, double author, xxxx date_pub, int quantity}";
        assertEquals(buildSplitSchema(ddlSerializationValue, DEFAULT_TEST_COLUMNS),
                buildExpectedProperties(expectedDDLSerialization, DEFAULT_TEST_COLUMNS));
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void shouldThrowNullPointerExceptionWhenColumnsIsNull()
    {
        updateSplitSchema(new Properties(), null);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void shouldThrowNullPointerExceptionWhenSchemaIsNull()
    {
        updateSplitSchema(null, ImmutableList.of());
    }

    private Properties buildSplitSchema(String ddlSerializationValue, HiveColumnHandle... columns)
    {
        Properties properties = new Properties();
        properties.setProperty(SERIALIZATION_LIB, LAZY_SERDE_CLASS_NAME);
        properties.setProperty(SERIALIZATION_DDL, ddlSerializationValue);
        return updateSplitSchema(properties, asList(columns));
    }

    private Properties buildExpectedProperties(String expectedDDLSerialization, HiveColumnHandle... expectedColumns)
    {
        String expectedColumnsType = getTypes(expectedColumns);
        String expectedColumnsName = getName(expectedColumns);
        Properties propExpected = new Properties();
        propExpected.setProperty(LIST_COLUMNS, expectedColumnsName);
        propExpected.setProperty(SERIALIZATION_LIB, LAZY_SERDE_CLASS_NAME);
        propExpected.setProperty(SERIALIZATION_DDL, expectedDDLSerialization);
        propExpected.setProperty(LIST_COLUMN_TYPES, expectedColumnsType);
        return propExpected;
    }

    private String getName(HiveColumnHandle[] expectedColumns)
    {
        return Stream.of(expectedColumns)
                .map(HiveColumnHandle::getName)
                .collect(joining(","));
    }

    private String getTypes(HiveColumnHandle[] expectedColumns)
    {
        return Stream.of(expectedColumns)
                .map(HiveColumnHandle::getHiveType)
                .map(HiveType::getTypeInfo)
                .map(TypeInfo::getTypeName)
                .collect(joining(","));
    }
}
