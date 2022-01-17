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
package io.trino.plugin.hive.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveType;
import io.trino.spi.ErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static io.airlift.testing.Assertions.assertContains;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestHiveTypeTranslator
{
    private static final Map<Type, HiveType> TRANSLATION_MAP = ImmutableMap.<Type, HiveType>builder()
            .put(BIGINT, HiveType.HIVE_LONG)
            .put(INTEGER, HiveType.HIVE_INT)
            .put(SMALLINT, HiveType.HIVE_SHORT)
            .put(TINYINT, HiveType.HIVE_BYTE)
            .put(DOUBLE, HiveType.HIVE_DOUBLE)
            .put(createVarcharType(3), HiveType.valueOf("varchar(3)"))
            .put(VARCHAR, HiveType.HIVE_STRING)
            .put(DATE, HiveType.HIVE_DATE)
            .put(TIMESTAMP_MILLIS, HiveType.HIVE_TIMESTAMP)
            .put(createDecimalType(5, 3), HiveType.valueOf("decimal(5,3)"))
            .put(VARBINARY, HiveType.HIVE_BINARY)
            .put(new ArrayType(TIMESTAMP_MILLIS), HiveType.valueOf("array<timestamp>"))
            .put(TESTING_TYPE_MANAGER.getType(mapType(BOOLEAN.getTypeSignature(), VARBINARY.getTypeSignature())), HiveType.valueOf("map<boolean,binary>"))
            .put(RowType.from(List.of(field("col0", INTEGER), field("col1", VARBINARY))), HiveType.valueOf("struct<col0:int,col1:binary>"))
            .buildOrThrow();

    @Test
    public void testTypeTranslator()
    {
        TRANSLATION_MAP.forEach(TestHiveTypeTranslator::assertTypeTranslation);

        assertInvalidTypeTranslation(
                RowType.anonymous(ImmutableList.of(INTEGER, VARBINARY)),
                NOT_SUPPORTED.toErrorCode(),
                "Anonymous row type is not supported in Hive. Please give each field a name: row(integer, varbinary)");
    }

    private static void assertTypeTranslation(Type type, HiveType hiveType)
    {
        assertEquals(toHiveType(type), hiveType);
    }

    private static void assertInvalidTypeTranslation(Type type, ErrorCode errorCode, String message)
    {
        try {
            toHiveType(type);
            fail("expected exception");
        }
        catch (TrinoException e) {
            try {
                assertEquals(e.getErrorCode(), errorCode);
                assertContains(e.getMessage(), message);
            }
            catch (Throwable failure) {
                failure.addSuppressed(e);
                throw failure;
            }
        }
    }
}
