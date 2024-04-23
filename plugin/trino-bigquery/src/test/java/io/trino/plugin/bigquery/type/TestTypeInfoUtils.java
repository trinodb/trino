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
package io.trino.plugin.bigquery.type;

import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.plugin.bigquery.type.TypeInfoFactory.getDecimalTypeInfo;
import static io.trino.plugin.bigquery.type.TypeInfoFactory.getListTypeInfo;
import static io.trino.plugin.bigquery.type.TypeInfoFactory.getPrimitiveTypeInfo;
import static io.trino.plugin.bigquery.type.TypeInfoFactory.getStructTypeInfo;
import static io.trino.plugin.bigquery.type.TypeInfoUtils.getTypeInfoFromTypeString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTypeInfoUtils
{
    @Test
    public void testBasicPrimitive()
    {
        assertTypeInfo("BOOL").isEqualTo(getPrimitiveTypeInfo("BOOL"));
        assertTypeInfo("INT64").isEqualTo(getPrimitiveTypeInfo("INT64"));
        assertTypeInfo("FLOAT64").isEqualTo(getPrimitiveTypeInfo("FLOAT64"));
        assertTypeInfo("STRING").isEqualTo(getPrimitiveTypeInfo("STRING"));
        assertTypeInfo("STRING(10)", "STRING").isEqualTo(getPrimitiveTypeInfo("STRING"));
        assertTypeInfo("BYTES").isEqualTo(getPrimitiveTypeInfo("BYTES"));
        assertTypeInfo("DATE").isEqualTo(getPrimitiveTypeInfo("DATE"));
        assertTypeInfo("TIME").isEqualTo(getPrimitiveTypeInfo("TIME"));
        assertTypeInfo("DATETIME").isEqualTo(getPrimitiveTypeInfo("DATETIME"));
        assertTypeInfo("TIMESTAMP").isEqualTo(getPrimitiveTypeInfo("TIMESTAMP"));
        assertTypeInfo("GEOGRAPHY").isEqualTo(getPrimitiveTypeInfo("GEOGRAPHY"));
        assertTypeInfo("JSON").isEqualTo(getPrimitiveTypeInfo("JSON"));
    }

    @Test
    public void testNumeric()
    {
        testNumeric("NUMERIC");
    }

    @Test
    public void testBignumeric()
    {
        testNumeric("BIGNUMERIC");
    }

    private void testNumeric(String typeName)
    {
        assertTypeInfo(typeName, "NUMERIC(10, 0)").isEqualTo(getDecimalTypeInfo(10, 0));
        assertTypeInfo(typeName + "(1)", "NUMERIC(1, 0)").isEqualTo(getDecimalTypeInfo(1, 0));
        assertTypeInfo(typeName + "(5)", "NUMERIC(5, 0)").isEqualTo(getDecimalTypeInfo(5, 0));
        assertTypeInfo(typeName + "(38)", "NUMERIC(38, 0)").isEqualTo(getDecimalTypeInfo(38, 0));
        assertTypeInfo(typeName + "(1, 1)", "NUMERIC(1, 1)").isEqualTo(getDecimalTypeInfo(1, 1));
        assertTypeInfo(typeName + "(10, 5)", "NUMERIC(10, 5)").isEqualTo(getDecimalTypeInfo(10, 5));
        assertTypeInfo(typeName + "(38, 38)", "NUMERIC(38, 38)").isEqualTo(getDecimalTypeInfo(38, 38));

        assertThatThrownBy(() -> getTypeInfoFromTypeString(typeName + "(0)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid decimal precision: 0");

        assertThatThrownBy(() -> getTypeInfoFromTypeString(typeName + "(39)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid decimal precision: 39");

        assertThatThrownBy(() -> getTypeInfoFromTypeString(typeName + "(38,39)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid decimal scale: 39");

        assertThatThrownBy(() -> getTypeInfoFromTypeString(typeName + "(4,5)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("decimal precision (4) is greater than scale (5)");
    }

    @Test
    public void testArray()
    {
        assertTypeInfo("ARRAY<INT64>")
                .isEqualTo(getListTypeInfo(getPrimitiveTypeInfo("INT64")));
    }

    @Test
    public void testStruct()
    {
        assertTypeInfo("STRUCT<x INT64, y STRING>")
                .isEqualTo(getStructTypeInfo(
                        List.of("x", "y"),
                        List.of(getPrimitiveTypeInfo("INT64"),
                                getPrimitiveTypeInfo("STRING"))));

        assertTypeInfo("STRUCT<x INT64, y ARRAY<STRING>>")
                .isEqualTo(getStructTypeInfo(
                        List.of("x", "y"),
                        List.of(getPrimitiveTypeInfo("INT64"),
                                getListTypeInfo(getPrimitiveTypeInfo("STRING")))));
    }

    private static ObjectAssert<TypeInfo> assertTypeInfo(String typeString)
    {
        assertThat(getBigQueryTypeInfo(typeString))
                .hasToString(typeString);

        return assertThat(getTypeInfoFromTypeString(typeString))
                .hasToString(typeString);
    }

    private static ObjectAssert<TypeInfo> assertTypeInfo(String typeString, String toString)
    {
        assertThat(getBigQueryTypeInfo(typeString))
                .hasToString(toString);

        return assertThat(getTypeInfoFromTypeString(typeString))
                .hasToString(toString);
    }

    private static TypeInfo getBigQueryTypeInfo(String typeString)
    {
        return TypeInfoUtils.getTypeInfoFromTypeString(typeString);
    }
}
