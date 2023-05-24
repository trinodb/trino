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
package io.trino.operator.scalar;

import io.airlift.slice.Slice;
import io.trino.metadata.InternalFunctionBundle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.IsNull;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestIsNullAnnotation
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        assertions.addFunctions(InternalFunctionBundle.builder()
                .scalars(TestIsNullAnnotation.class)
                .build());
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @ScalarFunction("test_is_null_simple")
    @SqlType(StandardTypes.BIGINT)
    public static long testIsNullSimple(@SqlType(StandardTypes.BIGINT) long value, @IsNull boolean isNull)
    {
        if (isNull) {
            return 100;
        }
        return 2 * value;
    }

    @ScalarFunction("test_is_null")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice testIsNull(
            ConnectorSession session,
            @SqlType(StandardTypes.INTEGER) long longValue,
            @IsNull boolean isNullLong,
            @SqlType(StandardTypes.VARCHAR) Slice varcharNotNullable,
            @SqlType(StandardTypes.VARCHAR)
            @SqlNullable Slice varcharNullable,
            @SqlType(StandardTypes.VARCHAR) Slice varcharIsNull,
            @IsNull boolean isNullVarchar)
    {
        checkArgument(session != null, "session is null");

        StringBuilder builder = new StringBuilder();

        if (!isNullLong) {
            builder.append(longValue);
        }
        builder.append(":");

        checkArgument(varcharNotNullable != null, "varcharNotNullable is null while it doesn't has @SqlNullable");
        builder.append(varcharNotNullable.toStringUtf8())
                .append(":");

        if (varcharNullable != null) {
            builder.append(varcharNullable.toStringUtf8());
        }
        builder.append(":");

        if (!isNullVarchar) {
            builder.append(varcharIsNull.toStringUtf8());
        }
        return utf8Slice(builder.toString());
    }

    @ScalarFunction("test_is_null_void")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean testIsNullVoid(@SqlType("unknown") boolean value, @IsNull boolean isNull)
    {
        return isNull;
    }

    @Test
    public void testIsNull()
    {
        assertThat(assertions.function("test_is_null_simple", "-100"))
                .isEqualTo(-200L);

        assertThat(assertions.function("test_is_null_simple", "23"))
                .isEqualTo(46L);

        assertThat(assertions.function("test_is_null_simple", "null"))
                .isEqualTo(100L);

        assertThat(assertions.function("test_is_null_simple", "cast(null as bigint)"))
                .isEqualTo(100L);

        assertThat(assertions.function("test_is_null", "23", "'aaa'", "'bbb'", "'ccc'"))
                .hasType(VARCHAR)
                .isEqualTo("23:aaa:bbb:ccc");

        assertThat(assertions.function("test_is_null", "null", "'aaa'", "'bbb'", "'ccc'"))
                .hasType(VARCHAR)
                .isEqualTo(":aaa:bbb:ccc");

        assertThat(assertions.function("test_is_null", "null", "'aaa'", "null", "'ccc'"))
                .hasType(VARCHAR)
                .isEqualTo(":aaa::ccc");

        assertThat(assertions.function("test_is_null", "23", "'aaa'", "null", "null"))
                .hasType(VARCHAR)
                .isEqualTo("23:aaa::");

        assertThat(assertions.function("test_is_null", "23", "null", "'bbb'", "'ccc'"))
                .isNull(VARCHAR);

        assertThat(assertions.function("test_is_null_void", "null"))
                .isEqualTo(true);
    }
}
