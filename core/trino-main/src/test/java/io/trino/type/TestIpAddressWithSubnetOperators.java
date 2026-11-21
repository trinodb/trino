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
import static io.trino.type.IpAddressWithSubnetType.IPADDRESS_WITH_SUBNET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestIpAddressWithSubnetOperators
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
    public void testVarcharToIpAddressWithSubnetCast()
    {
        assertThat(assertions.expression("cast(a as IPADDRESS_WITH_SUBNET)")
                .binding("a", "'192.168.1.10/16'"))
                .hasType(IPADDRESS_WITH_SUBNET)
                .isEqualTo(List.of("192.168.0.0", 16));

        assertThat(assertions.expression("cast(a as IPADDRESS_WITH_SUBNET)")
                .binding("a", "'192.168.1.10/32'"))
                .hasType(IPADDRESS_WITH_SUBNET)
                .isEqualTo(List.of("192.168.1.10", 32));

        assertThat(assertions.expression("cast(a as IPADDRESS_WITH_SUBNET)")
                .binding("a", "'0.0.0.0/0'"))
                .hasType(IPADDRESS_WITH_SUBNET)
                .isEqualTo(List.of("0.0.0.0", 0));

        assertThat(assertions.expression("cast(a as IPADDRESS_WITH_SUBNET)")
                .binding("a", "'2001:db8::1/32'"))
                .hasType(IPADDRESS_WITH_SUBNET)
                .isEqualTo(List.of("2001:db8::", 32));

        assertThat(assertions.expression("cast(a as IPADDRESS_WITH_SUBNET)")
                .binding("a", "'2001:db8::1/128'"))
                .hasType(IPADDRESS_WITH_SUBNET)
                .isEqualTo(List.of("2001:db8::1", 128));

        assertThat(assertions.expression("cast(a as IPADDRESS_WITH_SUBNET)")
                .binding("a", "'::/0'"))
                .hasType(IPADDRESS_WITH_SUBNET)
                .isEqualTo(List.of("::", 0));

        assertThat(assertions.expression("IPADDRESS_WITH_SUBNET '192.168.1.10/16'"))
                .hasType(IPADDRESS_WITH_SUBNET)
                .isEqualTo(List.of("192.168.0.0", 16));

        assertThat(assertions.expression("IPADDRESS_WITH_SUBNET '2001:db8::1/32'"))
                .hasType(IPADDRESS_WITH_SUBNET)
                .isEqualTo(List.of("2001:db8::", 32));

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as IPADDRESS_WITH_SUBNET)")
                .binding("a", "'192.168.1.10'").evaluate())
                .hasMessage("Cannot cast value to IPADDRESS_WITH_SUBNET: 192.168.1.10")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as IPADDRESS_WITH_SUBNET)")
                .binding("a", "'192.168.1.10/33'").evaluate())
                .hasMessage("Cannot cast value to IPADDRESS_WITH_SUBNET: 192.168.1.10/33")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as IPADDRESS_WITH_SUBNET)")
                .binding("a", "'2001:db8::1/129'").evaluate())
                .hasMessage("Cannot cast value to IPADDRESS_WITH_SUBNET: 2001:db8::1/129")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as IPADDRESS_WITH_SUBNET)")
                .binding("a", "'192.168.1.10/-1'").evaluate())
                .hasMessage("Cannot cast value to IPADDRESS_WITH_SUBNET: 192.168.1.10/-1")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as IPADDRESS_WITH_SUBNET)")
                .binding("a", "'192.168.1.10/x'").evaluate())
                .hasMessage("Cannot cast value to IPADDRESS_WITH_SUBNET: 192.168.1.10/x")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as IPADDRESS_WITH_SUBNET)")
                .binding("a", "'not-an-ip/16'").evaluate())
                .hasMessage("Cannot cast value to IPADDRESS_WITH_SUBNET: not-an-ip/16")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as IPADDRESS_WITH_SUBNET)")
                .binding("a", "'/16'").evaluate())
                .hasMessage("Cannot cast value to IPADDRESS_WITH_SUBNET: /16")
                .hasErrorCode(INVALID_CAST_ARGUMENT);

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as IPADDRESS_WITH_SUBNET)")
                .binding("a", "'192.168.1.10/'").evaluate())
                .hasMessage("Cannot cast value to IPADDRESS_WITH_SUBNET: 192.168.1.10/")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testIpAddressWithSubnetToVarcharCast()
    {
        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "IPADDRESS_WITH_SUBNET '192.168.1.10/16'"))
                .hasType(VARCHAR)
                .isEqualTo("192.168.0.0/16");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "IPADDRESS_WITH_SUBNET '2001:db8::1/32'"))
                .hasType(VARCHAR)
                .isEqualTo("2001:db8::/32");

        assertThat(assertions.expression("cast(a as VARCHAR)")
                .binding("a", "IPADDRESS_WITH_SUBNET '::ffff:192.168.1.10/128'"))
                .hasType(VARCHAR)
                .isEqualTo("::ffff:c0a8:10a/128");
    }

    @Test
    public void testTupleFieldAccess()
    {
        assertThat(assertions.expression("(IPADDRESS_WITH_SUBNET '192.168.1.10/16')[1]"))
                .hasType(IPADDRESS)
                .isEqualTo("192.168.0.0");

        assertThat(assertions.expression("(IPADDRESS_WITH_SUBNET '192.168.1.10/16')[2]"))
                .hasType(INTEGER)
                .isEqualTo(16);
    }

    @Test
    public void testComparison()
    {
        assertThat(assertions.expression("IPADDRESS_WITH_SUBNET '192.168.1.10/16' = IPADDRESS_WITH_SUBNET '192.168.4.4/16'"))
                .isEqualTo(true);

        assertThat(assertions.expression("IPADDRESS_WITH_SUBNET '192.168.1.0/16' = IPADDRESS_WITH_SUBNET '192.168.0.0/16'"))
                .isEqualTo(true);

        assertThat(assertions.expression("IPADDRESS_WITH_SUBNET '192.168.0.0/16' < IPADDRESS_WITH_SUBNET '192.168.0.0/24'"))
                .isEqualTo(true);

        assertThat(assertions.expression("IPADDRESS_WITH_SUBNET '2001:db8::/32' = IPADDRESS_WITH_SUBNET '2001:db8::1/32'"))
                .isEqualTo(true);
    }

    @Test
    public void testTypeRegistration()
    {
        assertThat(assertions.execute("SELECT IPADDRESS_WITH_SUBNET '192.168.1.10/16'").getTypes())
                .containsExactly(IPADDRESS_WITH_SUBNET);
    }
}
