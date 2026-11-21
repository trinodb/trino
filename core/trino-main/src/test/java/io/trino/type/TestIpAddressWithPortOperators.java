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

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.IpAddressType.IPADDRESS;
import static io.trino.type.IpAddressWithPortType.IPADDRESS_WITH_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestIpAddressWithPortOperators
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
    public void testVarcharToIpAddressWithPortCast()
    {
        assertThat(assertions.expression("cast(a as IPADDRESS_WITH_PORT)")
                .binding("a", "'1.2.3.4:80'"))
                .hasType(IPADDRESS_WITH_PORT)
                .isEqualTo(List.of("1.2.3.4", 80));

        assertThat(assertions.expression("cast(a as IPADDRESS_WITH_PORT)")
                .binding("a", "'::ffff:1.2.3.4:443'"))
                .hasType(IPADDRESS_WITH_PORT)
                .isEqualTo(List.of("1.2.3.4", 443));

        assertThat(assertions.expression("cast(a as IPADDRESS_WITH_PORT)")
                .binding("a", "'[2001:db8::1]:443'"))
                .hasType(IPADDRESS_WITH_PORT)
                .isEqualTo(List.of("2001:db8::1", 443));

        assertThat(assertions.expression("cast(a as IPADDRESS_WITH_PORT)")
                .binding("a", "'[2001:0db8:0000:0000:0000:0000:0000:0001]:443'"))
                .hasType(IPADDRESS_WITH_PORT)
                .isEqualTo(List.of("2001:db8::1", 443));

        assertThat(assertions.expression("IPADDRESS_WITH_PORT '1.2.3.4:80'"))
                .hasType(IPADDRESS_WITH_PORT)
                .isEqualTo(List.of("1.2.3.4", 80));

        assertThat(assertions.expression("IPADDRESS_WITH_PORT '[2001:db8::1]:8080'"))
                .hasType(IPADDRESS_WITH_PORT)
                .isEqualTo(List.of("2001:db8::1", 8080));

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as IPADDRESS_WITH_PORT)")
                .binding("a", "'1.2.3.4'").evaluate())
                .hasMessage("Cannot cast value to IPADDRESS_WITH_PORT: 1.2.3.4")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as IPADDRESS_WITH_PORT)")
                .binding("a", "'1.2.3.4:port'").evaluate())
                .hasMessage("Cannot cast value to IPADDRESS_WITH_PORT: 1.2.3.4:port")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as IPADDRESS_WITH_PORT)")
                .binding("a", "'1.2.3.4:65536'").evaluate())
                .hasMessage("Cannot cast value to IPADDRESS_WITH_PORT: 1.2.3.4:65536")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as IPADDRESS_WITH_PORT)")
                .binding("a", "'not-an-ip:80'").evaluate())
                .hasMessage("Cannot cast value to IPADDRESS_WITH_PORT: not-an-ip:80")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as IPADDRESS_WITH_PORT)")
                .binding("a", "'[2001:db8::1'").evaluate())
                .hasMessage("Cannot cast value to IPADDRESS_WITH_PORT: [2001:db8::1")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testIpAddressWithPortToVarcharCast()
    {
        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "IPADDRESS_WITH_PORT '1.2.3.4:80'"))
                .hasType(VARCHAR)
                .isEqualTo("1.2.3.4:80");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "IPADDRESS_WITH_PORT '[2001:db8::1]:443'"))
                .hasType(VARCHAR)
                .isEqualTo("[2001:db8::1]:443");
    }

    @Test
    public void testTupleFieldAccess()
    {
        assertThat(assertions.expression("(IPADDRESS_WITH_PORT '1.2.3.4:80')[1]"))
                .hasType(IPADDRESS)
                .isEqualTo("1.2.3.4");

        assertThat(assertions.expression("(IPADDRESS_WITH_PORT '1.2.3.4:80')[2]"))
                .hasType(INTEGER)
                .isEqualTo(80);
    }

    @Test
    public void testComparison()
    {
        assertThat(assertions.expression("IPADDRESS_WITH_PORT '1.2.3.4:80' = IPADDRESS_WITH_PORT '1.2.3.4:80'"))
                .isEqualTo(true);

        assertThat(assertions.expression("IPADDRESS_WITH_PORT '1.2.3.4:80' = IPADDRESS_WITH_PORT '1.2.3.4:81'"))
                .isEqualTo(false);

        assertThat(assertions.expression("IPADDRESS_WITH_PORT '1.2.3.4:80' < IPADDRESS_WITH_PORT '1.2.3.4:81'"))
                .isEqualTo(true);

        assertThat(assertions.expression("IPADDRESS_WITH_PORT '1.2.3.5:80' > IPADDRESS_WITH_PORT '1.2.3.4:81'"))
                .isEqualTo(true);
    }

    @Test
    public void testTypeRegistration()
    {
        assertThat(assertions.query("SELECT IPADDRESS_WITH_PORT '1.2.3.4:80'").getTypes())
                .containsExactly(IPADDRESS_WITH_PORT);
    }
}
