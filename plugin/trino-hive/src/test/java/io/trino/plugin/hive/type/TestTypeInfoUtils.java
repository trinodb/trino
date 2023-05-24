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
package io.trino.plugin.hive.type;

import org.apache.hadoop.hive.serde2.typeinfo.TimestampLocalTZTypeInfo;
import org.assertj.core.api.ObjectAssert;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.plugin.hive.type.TypeInfoFactory.getCharTypeInfo;
import static io.trino.plugin.hive.type.TypeInfoFactory.getDecimalTypeInfo;
import static io.trino.plugin.hive.type.TypeInfoFactory.getListTypeInfo;
import static io.trino.plugin.hive.type.TypeInfoFactory.getMapTypeInfo;
import static io.trino.plugin.hive.type.TypeInfoFactory.getPrimitiveTypeInfo;
import static io.trino.plugin.hive.type.TypeInfoFactory.getStructTypeInfo;
import static io.trino.plugin.hive.type.TypeInfoFactory.getVarcharTypeInfo;
import static io.trino.plugin.hive.type.TypeInfoUtils.getTypeInfoFromTypeString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTypeInfoUtils
{
    @Test
    public void testBasicPrimitive()
    {
        assertTypeInfo("void").isEqualTo(getPrimitiveTypeInfo("void"));
        assertTypeInfo("tinyint").isEqualTo(getPrimitiveTypeInfo("tinyint"));
        assertTypeInfo("smallint").isEqualTo(getPrimitiveTypeInfo("smallint"));
        assertTypeInfo("int").isEqualTo(getPrimitiveTypeInfo("int"));
        assertTypeInfo("bigint").isEqualTo(getPrimitiveTypeInfo("bigint"));
        assertTypeInfo("float").isEqualTo(getPrimitiveTypeInfo("float"));
        assertTypeInfo("double").isEqualTo(getPrimitiveTypeInfo("double"));
        assertTypeInfo("boolean").isEqualTo(getPrimitiveTypeInfo("boolean"));
        assertTypeInfo("string").isEqualTo(getPrimitiveTypeInfo("string"));
        assertTypeInfo("binary").isEqualTo(getPrimitiveTypeInfo("binary"));
        assertTypeInfo("date").isEqualTo(getPrimitiveTypeInfo("date"));
        assertTypeInfo("timestamp").isEqualTo(getPrimitiveTypeInfo("timestamp"));
        assertTypeInfo("interval_year_month").isEqualTo(getPrimitiveTypeInfo("interval_year_month"));
        assertTypeInfo("interval_day_time").isEqualTo(getPrimitiveTypeInfo("interval_day_time"));
    }

    @Test
    public void testChar()
    {
        assertTypeInfo("char(1)").isEqualTo(getCharTypeInfo(1));
        assertTypeInfo("char(50)").isEqualTo(getCharTypeInfo(50));
        assertTypeInfo("char(255)").isEqualTo(getCharTypeInfo(255));

        assertThatThrownBy(() -> getTypeInfoFromTypeString("char"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("char type is specified without length: char");

        assertThatThrownBy(() -> getTypeInfoFromTypeString("char(0)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid char length: 0");

        assertThatThrownBy(() -> getTypeInfoFromTypeString("char(256)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid char length: 256");
    }

    @Test
    public void testVarchar()
    {
        assertTypeInfo("varchar(1)").isEqualTo(getVarcharTypeInfo(1));
        assertTypeInfo("varchar(42)").isEqualTo(getVarcharTypeInfo(42));
        assertTypeInfo("varchar(65535)").isEqualTo(getVarcharTypeInfo(65535));

        assertThatThrownBy(() -> getTypeInfoFromTypeString("varchar"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("varchar type is specified without length: varchar");

        assertThatThrownBy(() -> getTypeInfoFromTypeString("varchar(0)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid varchar length: 0");

        assertThatThrownBy(() -> getTypeInfoFromTypeString("varchar(65536)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid varchar length: 65536");
    }

    @Test
    public void testDecimal()
    {
        assertTypeInfo("decimal", "decimal(10,0)").isEqualTo(getDecimalTypeInfo(10, 0));
        assertTypeInfo("decimal(1)", "decimal(1,0)").isEqualTo(getDecimalTypeInfo(1, 0));
        assertTypeInfo("decimal(5)", "decimal(5,0)").isEqualTo(getDecimalTypeInfo(5, 0));
        assertTypeInfo("decimal(38)", "decimal(38,0)").isEqualTo(getDecimalTypeInfo(38, 0));
        assertTypeInfo("decimal(1,1)", "decimal(1,1)").isEqualTo(getDecimalTypeInfo(1, 1));
        assertTypeInfo("decimal(10,5)", "decimal(10,5)").isEqualTo(getDecimalTypeInfo(10, 5));
        assertTypeInfo("decimal(38,38)", "decimal(38,38)").isEqualTo(getDecimalTypeInfo(38, 38));

        assertThatThrownBy(() -> getTypeInfoFromTypeString("decimal(0)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid decimal precision: 0");

        assertThatThrownBy(() -> getTypeInfoFromTypeString("decimal(39)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid decimal precision: 39");

        assertThatThrownBy(() -> getTypeInfoFromTypeString("decimal(38,39)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid decimal scale: 39");

        assertThatThrownBy(() -> getTypeInfoFromTypeString("decimal(4,5)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("decimal precision (4) is greater than scale (5)");
    }

    @Test
    public void testTimestampLocalTZ()
    {
        assertThat(getHiveTypeInfo("timestamp with local time zone"))
                .isInstanceOf(TimestampLocalTZTypeInfo.class);

        assertThat(getTypeInfoFromTypeString("timestamp with local time zone"))
                .isInstanceOf(PrimitiveTypeInfo.class)
                .hasToString("timestamp with local time zone")
                .isEqualTo(getPrimitiveTypeInfo("timestamp with local time zone"));
    }

    @Test
    public void testArray()
    {
        assertTypeInfo("array<int>")
                .isEqualTo(getListTypeInfo(getPrimitiveTypeInfo("int")));
    }

    @Test
    public void testMap()
    {
        assertTypeInfo("map<int,string>")
                .isEqualTo(getMapTypeInfo(
                        getPrimitiveTypeInfo("int"),
                        getPrimitiveTypeInfo("string")));
    }

    @Test
    public void testStruct()
    {
        assertTypeInfo("struct<x:int,y:bigint>")
                .isEqualTo(getStructTypeInfo(
                        List.of("x", "y"),
                        List.of(getPrimitiveTypeInfo("int"),
                                getPrimitiveTypeInfo("bigint"))));

        assertTypeInfo("struct<x:int,y:array<string>>")
                .isEqualTo(getStructTypeInfo(
                        List.of("x", "y"),
                        List.of(getPrimitiveTypeInfo("int"),
                                getListTypeInfo(getPrimitiveTypeInfo("string")))));
    }

    private static ObjectAssert<TypeInfo> assertTypeInfo(String typeString)
    {
        assertThat(getHiveTypeInfo(typeString))
                .hasToString(typeString);

        return assertThat(getTypeInfoFromTypeString(typeString))
                .hasToString(typeString);
    }

    private static ObjectAssert<TypeInfo> assertTypeInfo(String typeString, String toString)
    {
        assertThat(getHiveTypeInfo(typeString))
                .hasToString(toString);

        return assertThat(getTypeInfoFromTypeString(typeString))
                .hasToString(toString);
    }

    private static org.apache.hadoop.hive.serde2.typeinfo.TypeInfo getHiveTypeInfo(String typeString)
    {
        return org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils.getTypeInfoFromTypeString(typeString);
    }
}
