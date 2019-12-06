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
package io.prestosql.sql.planner;

import io.airlift.slice.Slice;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.scalar.Re2JCastToRegexpFunction;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.tree.Expression;
import io.prestosql.type.Re2JRegexp;
import org.testng.annotations.Test;

import java.util.Base64;

import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.scalar.JoniRegexpCasts.castVarcharToJoniRegexp;
import static io.prestosql.operator.scalar.JsonFunctions.castVarcharToJsonPath;
import static io.prestosql.operator.scalar.StringFunctions.castVarcharToCodePoints;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.SqlFormatter.formatSql;
import static io.prestosql.type.CodePointsType.CODE_POINTS;
import static io.prestosql.type.JoniRegexpType.JONI_REGEXP;
import static io.prestosql.type.JsonPathType.JSON_PATH;
import static io.prestosql.type.LikePatternType.LIKE_PATTERN;
import static io.prestosql.type.Re2JRegexpType.RE2J_REGEXP;
import static io.prestosql.type.UnknownType.UNKNOWN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

public class TestLiteralEncoder
{
    private final Metadata metadata = createTestMetadataManager();
    private final LiteralEncoder encoder = new LiteralEncoder(metadata);

    @Test
    public void testEncode()
    {
        assertEncode(null, UNKNOWN, "null");
        assertEncode(null, BIGINT, "CAST(null AS bigint)");
        assertEncode(123L, BIGINT, "BIGINT '123'");
        assertEncode(utf8Slice("hello"), VARCHAR, "CAST('hello' AS varchar)");
        assertEncode(utf8Slice("hello"), VARBINARY, literalVarbinary("hello".getBytes(UTF_8)));
        assertEncode(castVarcharToJoniRegexp(utf8Slice("[a-z]")), LIKE_PATTERN, "LikePattern '[a-z]'");
        assertEncode(castVarcharToJoniRegexp(utf8Slice("[a-z]")), JONI_REGEXP, "JoniRegExp '[a-z]'");
        assertEncode(castVarcharToRe2JRegexp(utf8Slice("[a-z]")), RE2J_REGEXP, "Re2JRegExp '[a-z]'");
        assertEncode(castVarcharToJsonPath(utf8Slice("$.foo")), JSON_PATH, "JsonPath '$.foo'");
        assertEncode(castVarcharToCodePoints(utf8Slice("hello")), CODE_POINTS, "CodePoints 'hello'");
    }

    private void assertEncode(Object value, Type type, String expected)
    {
        Expression expression = encoder.toExpression(value, type);
        assertEquals(formatSql(expression), expected);
    }

    private static String literalVarbinary(byte[] value)
    {
        return "\"@$literal$|varbinary|varbinary@$literal$<f,t>(f):t\"" +
                "(\"@from_base64|varbinary|varchar@from_base64(varchar(x)):varbinary\"" +
                "('" + Base64.getEncoder().encodeToString(value) + "'))";
    }

    private static Re2JRegexp castVarcharToRe2JRegexp(Slice value)
    {
        return Re2JCastToRegexpFunction.castToRegexp(Integer.MAX_VALUE, 5, false, VarcharType.UNBOUNDED_LENGTH, value);
    }
}
