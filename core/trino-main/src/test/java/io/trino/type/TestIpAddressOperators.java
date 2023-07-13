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
package io.trino.type;

import io.trino.spi.function.OperatorType;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.HASH_CODE;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.IpAddressType.IPADDRESS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestIpAddressOperators
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testVarcharToIpAddressCast()
    {
        assertThat(assertions.expression("cast(a as IPADDRESS)")
                .binding("a", "'::ffff:1.2.3.4'"))
                .hasType(IPADDRESS)
                .isEqualTo("1.2.3.4");

        assertThat(assertions.expression("cast(a as IPADDRESS)")
                .binding("a", "'1.2.3.4'"))
                .hasType(IPADDRESS)
                .isEqualTo("1.2.3.4");

        assertThat(assertions.expression("cast(a as IPADDRESS)")
                .binding("a", "'192.168.0.0'"))
                .hasType(IPADDRESS)
                .isEqualTo("192.168.0.0");

        assertThat(assertions.expression("cast(a as IPADDRESS)")
                .binding("a", "'2001:0db8:0000:0000:0000:ff00:0042:8329'"))
                .hasType(IPADDRESS)
                .isEqualTo("2001:db8::ff00:42:8329");

        assertThat(assertions.expression("cast(a as IPADDRESS)")
                .binding("a", "'2001:db8::ff00:42:8329'"))
                .hasType(IPADDRESS)
                .isEqualTo("2001:db8::ff00:42:8329");

        assertThat(assertions.expression("cast(a as IPADDRESS)")
                .binding("a", "'2001:db8:0:0:1:0:0:1'"))
                .hasType(IPADDRESS)
                .isEqualTo("2001:db8::1:0:0:1");

        assertThat(assertions.expression("cast(a as IPADDRESS)")
                .binding("a", "'2001:db8:0:0:1::1'"))
                .hasType(IPADDRESS)
                .isEqualTo("2001:db8::1:0:0:1");

        assertThat(assertions.expression("cast(a as IPADDRESS)")
                .binding("a", "'2001:db8::1:0:0:1'"))
                .hasType(IPADDRESS)
                .isEqualTo("2001:db8::1:0:0:1");

        assertThat(assertions.expression("cast(a as IPADDRESS)")
                .binding("a", "'2001:DB8::FF00:ABCD:12EF'"))
                .hasType(IPADDRESS)
                .isEqualTo("2001:db8::ff00:abcd:12ef");

        assertThat(assertions.expression("IPADDRESS '10.0.0.0'"))
                .hasType(IPADDRESS)
                .isEqualTo("10.0.0.0");

        assertThat(assertions.expression("IPADDRESS '64:ff9b::10.0.0.0'"))
                .hasType(IPADDRESS)
                .isEqualTo("64:ff9b::a00:0");

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as IPADDRESS)")
                .binding("a", "'facebook.com'").evaluate())
                .hasMessage("Cannot cast value to IPADDRESS: facebook.com")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as IPADDRESS)")
                .binding("a", "'localhost'").evaluate())
                .hasMessage("Cannot cast value to IPADDRESS: localhost")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as IPADDRESS)")
                .binding("a", "'2001:db8::1::1'").evaluate())
                .hasMessage("Cannot cast value to IPADDRESS: 2001:db8::1::1")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as IPADDRESS)")
                .binding("a", "'2001:zxy::1::1'").evaluate())
                .hasMessage("Cannot cast value to IPADDRESS: 2001:zxy::1::1")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as IPADDRESS)")
                .binding("a", "'789.1.1.1'").evaluate())
                .hasMessage("Cannot cast value to IPADDRESS: 789.1.1.1")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testIpAddressToVarcharCast()
    {
        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "IPADDRESS '::ffff:1.2.3.4'"))
                .hasType(VARCHAR)
                .isEqualTo("1.2.3.4");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "IPADDRESS '::ffff:102:304'"))
                .hasType(VARCHAR)
                .isEqualTo("1.2.3.4");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "IPADDRESS '2001:0db8:0000:0000:0000:ff00:0042:8329'"))
                .hasType(VARCHAR)
                .isEqualTo("2001:db8::ff00:42:8329");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "IPADDRESS '2001:db8::ff00:42:8329'"))
                .hasType(VARCHAR)
                .isEqualTo("2001:db8::ff00:42:8329");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "IPADDRESS '2001:db8:0:0:1:0:0:1'"))
                .hasType(VARCHAR)
                .isEqualTo("2001:db8::1:0:0:1");

        assertThat(assertions.expression("cast(a AS VARCHAR)")
                .binding("a", "CAST('1.2.3.4' as IPADDRESS)"))
                .hasType(VARCHAR)
                .isEqualTo("1.2.3.4");

        assertThat(assertions.expression("cast(a AS VARCHAR)")
                .binding("a", "CAST('2001:db8:0:0:1::1' as IPADDRESS)"))
                .hasType(VARCHAR)
                .isEqualTo("2001:db8::1:0:0:1");

        assertThat(assertions.expression("cast(a AS VARCHAR)")
                .binding("a", "CAST('64:ff9b::10.0.0.0' as IPADDRESS)"))
                .hasType(VARCHAR)
                .isEqualTo("64:ff9b::a00:0");
    }

    @Test
    public void testVarbinaryToIpAddressCast()
    {
        assertThat(assertions.expression("cast(a as IPADDRESS)")
                .binding("a", "x'00000000000000000000ffff01020304'"))
                .hasType(IPADDRESS)
                .isEqualTo("1.2.3.4");

        assertThat(assertions.expression("cast(a as IPADDRESS)")
                .binding("a", "x'01020304'"))
                .hasType(IPADDRESS)
                .isEqualTo("1.2.3.4");

        assertThat(assertions.expression("cast(a as IPADDRESS)")
                .binding("a", "x'c0a80000'"))
                .hasType(IPADDRESS)
                .isEqualTo("192.168.0.0");

        assertThat(assertions.expression("cast(a as IPADDRESS)")
                .binding("a", "x'20010db8000000000000ff0000428329'"))
                .hasType(IPADDRESS)
                .isEqualTo("2001:db8::ff00:42:8329");

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as IPADDRESS)")
                .binding("a", "x'f000001100'")
                .evaluate())
                .hasMessage("Invalid IP address binary length: 5")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testIpAddressToVarbinaryCast()
    {
        assertThat(assertions.expression("cast(a as VARBINARY)")
                .binding("a", "IPADDRESS '::ffff:1.2.3.4'"))
                .hasType(VARBINARY)
                .matches("X'00000000000000000000FFFF01020304'");

        assertThat(assertions.expression("cast(a as VARBINARY)")
                .binding("a", "IPADDRESS '2001:0db8:0000:0000:0000:ff00:0042:8329'"))
                .hasType(VARBINARY)
                .matches("X'20010DB8000000000000FF0000428329'");

        assertThat(assertions.expression("cast(a as VARBINARY)")
                .binding("a", "IPADDRESS '2001:db8::ff00:42:8329'"))
                .hasType(VARBINARY)
                .matches("X'20010DB8000000000000FF0000428329'");
    }

    @Test
    public void testEquals()
    {
        assertThat(assertions.operator(EQUAL, "IPADDRESS '2001:0db8:0000:0000:0000:ff00:0042:8329'", "IPADDRESS '2001:db8::ff00:42:8329'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "cast('1.2.3.4' as IPADDRESS)", "CAST('::ffff:1.2.3.4' AS IPADDRESS)"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "IPADDRESS '192.168.0.0'", "IPADDRESS '::ffff:192.168.0.0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "IPADDRESS '10.0.0.0'", "IPADDRESS '::ffff:a00:0'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "IPADDRESS '2001:db8::ff00:42:8329'", "IPADDRESS '2001:db8::ff00:42:8300'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "CAST('1.2.3.4' AS IPADDRESS)", "IPADDRESS '1.2.3.5'"))
                .isEqualTo(false);
    }

    @Test
    public void testDistinctFrom()
    {
        assertThat(assertions.operator(OperatorType.IS_DISTINCT_FROM, "IPADDRESS '2001:0db8:0000:0000:0000:ff00:0042:8329'", "IPADDRESS '2001:db8::ff00:42:8329'"))
                .isEqualTo(false);

        assertThat(assertions.operator(OperatorType.IS_DISTINCT_FROM, "cast(NULL as IPADDRESS)", "CAST(NULL AS IPADDRESS)"))
                .isEqualTo(false);

        assertThat(assertions.operator(OperatorType.IS_DISTINCT_FROM, "IPADDRESS '2001:0db8:0000:0000:0000:ff00:0042:8329'", "IPADDRESS '2001:db8::ff00:42:8328'"))
                .isEqualTo(true);

        assertThat(assertions.operator(OperatorType.IS_DISTINCT_FROM, "IPADDRESS '2001:0db8:0000:0000:0000:ff00:0042:8329'", "CAST(NULL AS IPADDRESS)"))
                .isEqualTo(true);

        assertThat(assertions.operator(OperatorType.IS_DISTINCT_FROM, "CAST(NULL AS IPADDRESS)", "IPADDRESS '2001:db8::ff00:42:8328'"))
                .isEqualTo(true);
    }

    @Test
    public void testNotEquals()
    {
        assertThat(assertions.expression("a != b")
                .binding("a", "IPADDRESS '2001:0db8:0000:0000:0000:ff00:0042:8329'")
                .binding("b", "IPADDRESS '1.2.3.4'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "CAST('1.2.3.4' as IPADDRESS)")
                .binding("b", "CAST('1.2.3.5' AS IPADDRESS)"))
                .isEqualTo(true);

        assertThat(assertions.expression("a != b")
                .binding("a", "CAST('1.2.3.4' AS IPADDRESS)")
                .binding("b", "IPADDRESS '1.2.3.4'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "IPADDRESS '2001:0db8:0000:0000:0000:ff00:0042:8329'")
                .binding("b", "IPADDRESS '2001:db8::ff00:42:8329'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a <> b")
                .binding("a", "CAST('1.2.3.4' as IPADDRESS)")
                .binding("b", "CAST('::ffff:1.2.3.4' AS IPADDRESS)"))
                .isEqualTo(false);
    }

    @Test
    public void testOrderOperators()
    {
        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "IPADDRESS '::2222'")
                .binding("low", "IPADDRESS '::'")
                .binding("high", "IPADDRESS '::1234'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "IPADDRESS '2001:0db8:0000:0000:0000:ff00:0042:8329'")
                .binding("b", "IPADDRESS '1.2.3.4'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "IPADDRESS '1.2.3.4'")
                .binding("b", "IPADDRESS '2001:0db8:0000:0000:0000:ff00:0042:8329'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "cast('1.2.3.4' as IPADDRESS)", "CAST('1.2.3.5' AS IPADDRESS)"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "cast('1.2.3.5' as IPADDRESS)", "CAST('1.2.3.4' AS IPADDRESS)"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "IPADDRESS '::1'", "CAST('1.2.3.5' AS IPADDRESS)"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "IPADDRESS '1.2.3.5'", "CAST('1.2.3.5' AS IPADDRESS)"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "IPADDRESS '1.2.3.6'", "CAST('1.2.3.5' AS IPADDRESS)"))
                .isEqualTo(false);

        assertThat(assertions.expression("a >= b")
                .binding("a", "IPADDRESS '::1'")
                .binding("b", "IPADDRESS '::'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "IPADDRESS '::1'")
                .binding("b", "IPADDRESS '::1'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "IPADDRESS '::'")
                .binding("b", "IPADDRESS '::1'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "IPADDRESS '::1'")
                .binding("low", "IPADDRESS '::'")
                .binding("high", "IPADDRESS '::1234'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "IPADDRESS '::2222'")
                .binding("low", "IPADDRESS '::'")
                .binding("high", "IPADDRESS '::1234'"))
                .isEqualTo(false);
    }

    @Test
    public void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "CAST(null AS IPADDRESS)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "IPADDRESS '::2222'"))
                .isEqualTo(false);
    }

    @Test
    public void testHash()
    {
        assertThat(assertions.operator(HASH_CODE, "CAST(null AS IPADDRESS)"))
                .isNull(BIGINT);

        assertThat(assertions.operator(HASH_CODE, "IPADDRESS '::2222'"))
                .isEqualTo(6196302176720775325L);
    }
}
