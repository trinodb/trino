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

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.IpAddressType.IPADDRESS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestIpAddressFunctions
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
    public void testIpAddressContains()
    {
        // Class A (prefix length is between 1 and 8)
        assertThat(assertions.function("contains", "'0.0.0.0/0'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.0.0/0'", "IPADDRESS '255.255.255.255'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/1'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'128.0.0.0/1'", "IPADDRESS '128.0.0.0'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/2'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'64.0.0.0/2'", "IPADDRESS '64.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'128.0.0.0/2'", "IPADDRESS '128.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'192.0.0.0/2'", "IPADDRESS '192.0.0.0'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/3'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'32.0.0.0/3'", "IPADDRESS '32.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'224.0.0.0/3'", "IPADDRESS '224.0.0.0'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/4'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'16.0.0.0/4'", "IPADDRESS '16.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'240.0.0.0/4'", "IPADDRESS '240.0.0.0'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/5'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'8.0.0.0/5'", "IPADDRESS '8.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'248.0.0.0/5'", "IPADDRESS '248.0.0.0'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/6'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'4.0.0.0/6'", "IPADDRESS '4.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'252.0.0.0/6'", "IPADDRESS '252.0.0.0'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/7'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'2.0.0.0/7'", "IPADDRESS '2.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'254.0.0.0/7'", "IPADDRESS '254.0.0.0'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/8'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'255.0.0.0/8'", "IPADDRESS '255.0.0.0'"))
                .isEqualTo(true);

        // Class B (prefix length is between 9 and 16)
        assertThat(assertions.function("contains", "'0.0.0.0/9'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.128.0.0/9'", "IPADDRESS '0.128.0.0'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/10'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.64.0.0/10'", "IPADDRESS '0.64.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.128.0.0/10'", "IPADDRESS '0.128.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.192.0.0/10'", "IPADDRESS '0.192.0.0'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/11'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.32.0.0/11'", "IPADDRESS '0.32.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.224.0.0/11'", "IPADDRESS '0.224.0.0'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/12'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.16.0.0/12'", "IPADDRESS '0.16.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.240.0.0/12'", "IPADDRESS '0.240.0.0'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/13'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.8.0.0/13'", "IPADDRESS '0.8.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.248.0.0/13'", "IPADDRESS '0.248.0.0'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/14'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.4.0.0/14'", "IPADDRESS '0.4.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.252.0.0/14'", "IPADDRESS '0.252.0.0'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/15'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.2.0.0/15'", "IPADDRESS '0.2.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.254.0.0/15'", "IPADDRESS '0.254.0.0'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/16'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.255.0.0/16'", "IPADDRESS '0.255.0.0'"))
                .isEqualTo(true);

        // Class C (prefix length is between 17 and 24)
        assertThat(assertions.function("contains", "'0.0.0.0/17'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.128.0/17'", "IPADDRESS '0.0.128.0'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/18'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.64.0/18'", "IPADDRESS '0.0.64.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.128.0/18'", "IPADDRESS '0.0.128.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.192.0/18'", "IPADDRESS '0.0.192.0'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/19'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.32.0/19'", "IPADDRESS '0.0.32.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.224.0/19'", "IPADDRESS '0.0.224.0'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/20'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.16.0/20'", "IPADDRESS '0.0.16.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.240.0/20'", "IPADDRESS '0.0.240.0'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/21'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.8.0/21'", "IPADDRESS '0.0.8.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.248.0/21'", "IPADDRESS '0.0.248.0'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/22'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.4.0/22'", "IPADDRESS '0.0.4.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.252.0/22'", "IPADDRESS '0.0.252.0'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/23'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.2.0/23'", "IPADDRESS '0.0.2.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.254.0/23'", "IPADDRESS '0.0.254.0'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/24'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.255.0/24'", "IPADDRESS '0.0.255.0'"))
                .isEqualTo(true);

        // Class C (prefix length is between 25 and 32)
        assertThat(assertions.function("contains", "'0.0.0.0/25'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.0.128/25'", "IPADDRESS '0.0.0.128'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/26'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.0.64/26'", "IPADDRESS '0.0.0.64'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.0.128/26'", "IPADDRESS '0.0.0.128'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.0.192/26'", "IPADDRESS '0.0.0.192'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/27'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.0.32/27'", "IPADDRESS '0.0.0.32'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.0.224/27'", "IPADDRESS '0.0.0.224'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/28'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.0.16/28'", "IPADDRESS '0.0.0.16'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.0.240/28'", "IPADDRESS '0.0.0.240'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/29'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.0.8/29'", "IPADDRESS '0.0.0.8'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.0.248/29'", "IPADDRESS '0.0.0.248'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/30'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.0.4/30'", "IPADDRESS '0.0.0.4'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.0.252/30'", "IPADDRESS '0.0.0.252'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/31'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.0.2/31'", "IPADDRESS '0.0.0.2'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.0.254/31'", "IPADDRESS '0.0.0.254'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'0.0.0.0/32'", "IPADDRESS '0.0.0.0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.0.255/32'", "IPADDRESS '0.0.0.255'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0.0.0.255/32'", "IPADDRESS '255.0.0.0'"))
                .isEqualTo(false);

        // IPv6
        assertThat(assertions.function("contains", "'::ffff:0000:0000/0'", "IPADDRESS '::ffff:0000:0000'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'::ffff:0000:0000/0'", "IPADDRESS '::ffff:ffff:ffff'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'64:ff9b::10.0.0.0/64'", "IPADDRESS '64:ff9a:f:f:f:f:f:f'"))
                .isEqualTo(false);
        assertThat(assertions.function("contains", "'64:ff9b::10.0.0.0/64'", "IPADDRESS '64:ff9b:0:0:0:0:0:0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'64:ff9b::10.0.0.0/64'", "IPADDRESS '64:ff9b:0:0:f:f:f:f'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'64:ff9b::10.0.0.0/64'", "IPADDRESS '64:ff9b:0:1:0:0:0:0'"))
                .isEqualTo(false);

        assertThat(assertions.function("contains", "'2001:0DB8:0000:CD30:0000:0000:0000:0000/60'", "IPADDRESS '2001:0DB8::CD30:0:0:0:0'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'2620:109:c003:104::/64'", "IPADDRESS '2620:109:c003:104::C01'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'2001:0db8:0:0:0:ff00:0042:8329/128'", "IPADDRESS '2001:0db8:0:0:0:ff00:0042:8328'"))
                .isEqualTo(false);
        assertThat(assertions.function("contains", "'2001:0db8:0:0:0:ff00:0042:8329/128'", "IPADDRESS '2001:0db8:0:0:0:ff00:0042:8329'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'2001:0db8:0:0:0:ff00:0042:8329/128'", "IPADDRESS '2001:0db8:0:0:0:ff00:0042:8330'"))
                .isEqualTo(false);

        assertThat(assertions.function("contains", "'::/0'", "IPADDRESS '::'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'::/0'", "IPADDRESS '::1'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'::/0'", "IPADDRESS '2001::'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'::1/128'", "IPADDRESS '::1'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'::1/128'", "IPADDRESS '::'"))
                .isEqualTo(false);
        assertThat(assertions.function("contains", "'::1/128'", "IPADDRESS '2001::'"))
                .isEqualTo(false);

        assertThat(assertions.function("contains", "'2001:abcd:ef01:2345:6789:abcd:ef01:234/64'", "IPADDRESS '2001:abcd:ef01:2345::1'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'2001:abcd:ef01:2345:6789:abcd:ef01:234/64'", "IPADDRESS '2001:abcd:ef01:2345::'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'2001:abcd:ef01:2345:6789:abcd:ef01:234/64'", "IPADDRESS '2001::'"))
                .isEqualTo(false);
        assertThat(assertions.function("contains", "'2001:abcd:ef01:2345:6789:abcd:ef01:234/64'", "IPADDRESS '2001:abcd::'"))
                .isEqualTo(false);
        assertThat(assertions.function("contains", "'2001:abcd:ef01:2345:6789:abcd:ef01:234/64'", "IPADDRESS '2001:abcd:ef01:2340::'"))
                .isEqualTo(false);
        assertThat(assertions.function("contains", "'2001:abcd:ef01:2345:6789:abcd:ef01:234/64'", "IPADDRESS '2002::'"))
                .isEqualTo(false);

        assertThat(assertions.function("contains", "'2001:abcd:ef01:2345:6789:abcd:ef01:234/60'", "IPADDRESS '2001:abcd:ef01:2345::'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'2001:abcd:ef01:2345:6789:abcd:ef01:234/60'", "IPADDRESS '2001:abcd:ef01:2340::'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'2001:abcd:ef01:2345:6789:abcd:ef01:234/60'", "IPADDRESS '2001:abcd:ef01:2330::'"))
                .isEqualTo(false);
        assertThat(assertions.function("contains", "'2001:abcd:ef01:2345:6789:abcd:ef01:234/60'", "IPADDRESS '2001:abcd::'"))
                .isEqualTo(false);
        assertThat(assertions.function("contains", "'2001:abcd:ef01:2345:6789:abcd:ef01:234/60'", "IPADDRESS '2001:abcd:ef00::'"))
                .isEqualTo(false);
        assertThat(assertions.function("contains", "'2001:abcd:ef01:2345:6789:abcd:ef01:234/60'", "IPADDRESS '2001::'"))
                .isEqualTo(false);
        assertThat(assertions.function("contains", "'2001:abcd:ef01:2345:6789:abcd:ef01:234/60'", "IPADDRESS '2002::'"))
                .isEqualTo(false);

        // conflicting IP address versions
        assertThat(assertions.function("contains", "'127.0.0.1/32'", "IPADDRESS '64:ff9a:f:f:f:f:f:f'"))
                .isEqualTo(false);
        assertThat(assertions.function("contains", "'2001:abcd:ef01:2345:6789:abcd:ef01:234/60'", "IPADDRESS '127.0.0.0'"))
                .isEqualTo(false);

        // NULL argument
        assertThat(assertions.function("contains", "'10.0.0.1/0'", "cast(NULL as IPADDRESS)"))
                .isNull(BOOLEAN);
        assertThat(assertions.function("contains", "'::ffff:1.2.3.4/0'", "cast(NULL as IPADDRESS)"))
                .isNull(BOOLEAN);

        assertThat(assertions.function("contains", "NULL", "IPADDRESS '10.0.0.1'"))
                .isNull(BOOLEAN);
        assertThat(assertions.function("contains", "NULL", "IPADDRESS '::ffff:1.2.3.4'"))
                .isNull(BOOLEAN);

        assertThat(assertions.function("contains", "NULL", "cast(NULL as IPADDRESS)"))
                .isNull(BOOLEAN);

        // Invalid prefix length
        assertTrinoExceptionThrownBy(assertions.function("contains", "'0.0.0.0/-1'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid prefix length");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'64:ff9b::10.0.0.0/-1'", "IPADDRESS '64:ff9b::10.0.0.0'")::evaluate)
                .hasMessage("Invalid prefix length");

        // Invalid CIDR format
        assertTrinoExceptionThrownBy(assertions.function("contains", "'0.0.0.1/0'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'1.0.0.0/1'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'128.1.1.1/1'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'129.0.0.0/1'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'192.1.1.1/2'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'193.0.0.0/2'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'224.1.1.1/3'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'225.0.0.0/3'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'240.1.1.1/4'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'241.0.0.0/4'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'248.1.1.1/5'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'249.0.0.0/5'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'252.1.1.1/6'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'253.0.0.0/6'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'254.1.1.1/7'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.0.0.0/7'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.1.1.1/8'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.0.1.1/9'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.129.0.0/9'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.0.1.1/10'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.193.0.0/10'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.0.1.1/11'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.225.0.0/11'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.240.1.1/12'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.241.0.0/12'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.248.1.1/13'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.249.1.1/13'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.252.1.1/14'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.253.0.0/14'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.254.1.1/15'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.255.1.1/15'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.255.0.1/16'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.255.1.0/16'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.255.0.1/17'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.255.129.0/17'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.255.0.1/18'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.255.193.0/18'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.255.0.1/19'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.255.225.0/19'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.255.240.1/20'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.255.241.0/20'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.255.248.1/21'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.255.249.1/21'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.255.252.1/22'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.255.253.0/22'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.255.254.1/23'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.255.255.1/23'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'255.255.255.1/24'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");

        assertTrinoExceptionThrownBy(assertions.function("contains", "'10.0.0.1/33'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'64:ff9b::10.0.0.0/129'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'2620:109:c006:104::/250'", "IPADDRESS '2620:109:c006:104::'")::evaluate)
                .hasMessage("Invalid CIDR");

        assertTrinoExceptionThrownBy(assertions.function("contains", "'x.x.x.x'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'x:x:x:10.0.0.0'", "IPADDRESS '64:ff9b::10.0.0.0'")::evaluate)
                .hasMessage("Invalid CIDR");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'x.x.x.x/1'", "IPADDRESS '0.0.0.0'")::evaluate)
                .hasMessage("Invalid network IP address");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'x:x:x:10.0.0.0/1'", "IPADDRESS '64:ff9b::10.0.0.0'")::evaluate)
                .hasMessage("Invalid network IP address");
        assertTrinoExceptionThrownBy(assertions.function("contains", "'2001:0DB8:0:CD3/60'", "IPADDRESS '2001:0DB8::CD30:0:0:0:0'")::evaluate)
                .hasMessage("Invalid network IP address");
    }

    @Test
    public void testIPv4MappedAddresses()
    {
        assertThat(assertions.function("contains", "'0:0:0:0:0:ffff:aabb:ccdd/96'", "IPADDRESS '170.187.204.221'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'0:0:0:0:0:ffff::/96'", "IPADDRESS '170.187.204.221'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'::aabb:ccdd/96'", "IPADDRESS '170.187.204.221'"))
                .isEqualTo(false);
        assertThat(assertions.function("contains", "'1:2:3:4:5:6:aabb:ccdd/96'", "IPADDRESS '170.187.204.221'"))
                .isEqualTo(false);

        assertThat(assertions.function("contains", "'170.0.0.0/8'", "IPADDRESS '0:0:0:0:0:ffff:aa01:0203'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'170.187.0.0/16'", "IPADDRESS '0:0:0:0:0:ffff:aabb:0203'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'170.187.204.0/24'", "IPADDRESS '0:0:0:0:0:ffff:aabb:cc03'"))
                .isEqualTo(true);

        assertThat(assertions.function("contains", "'170.187.204.0/24'", "IPADDRESS '0:0:0:0:0:0:aabb:cc03'"))
                .isEqualTo(false);

        // 127.0.0.1 equals ::ffff:7f00:0001 in IPv6
        assertThat(assertions.function("contains", "'127.0.0.1/32'", "IPADDRESS '127.0.0.0'"))
                .isEqualTo(false);
        assertThat(assertions.function("contains", "'127.0.0.1/32'", "IPADDRESS '127.0.0.1'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'127.0.0.1/32'", "IPADDRESS '127.0.0.2'"))
                .isEqualTo(false);

        assertThat(assertions.function("contains", "'::ffff:7f00:0001/128'", "IPADDRESS '127.0.0.0'"))
                .isEqualTo(false);
        assertThat(assertions.function("contains", "'::ffff:7f00:0001/128'", "IPADDRESS '127.0.0.1'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'::ffff:7f00:0001/128'", "IPADDRESS '127.0.0.2'"))
                .isEqualTo(false);

        assertThat(assertions.function("contains", "'127.0.0.1/32'", "IPADDRESS '::ffff:7f00:0000'"))
                .isEqualTo(false);
        assertThat(assertions.function("contains", "'127.0.0.1/32'", "IPADDRESS '::ffff:7f00:0001'"))
                .isEqualTo(true);
        assertThat(assertions.function("contains", "'127.0.0.1/32'", "IPADDRESS '::ffff:7f00:0002'"))
                .isEqualTo(false);
    }

    @Test
    public void testIpAddressToBits()
    {
        assertThat(assertions.expression("ip_to_bits(a)")
                .binding("a", "IPADDRESS '192.172.1.20'"))
                .hasType(VARCHAR)
                .isEqualTo("11000000101011000000000100010100");

        assertThat(assertions.expression("ip_to_bits(a)")
                .binding("a", "IPADDRESS '4ffe:2900:5545:3210:2000:f8ff:fe21:67cf'"))
                .hasType(VARCHAR)
                .isEqualTo("01001111111111100010100100000000010101010100010100110010000100000010000000000000111110001111111111111110001000010110011111001111");
    }

    @Test
    public void testIpAddressToBitsWithSubnet()
    {
        assertThat(assertions.expression("ip_to_bits(a, 96)")
                .binding("a", "IPADDRESS '64:ff9b::'"))
                .hasType(VARCHAR)
                .isEqualTo("00000000011001001111111110011011000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");

        assertThat(assertions.expression("ip_to_bits(a, 1)")
                .binding("a", "IPADDRESS 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'"))
                .hasType(VARCHAR)
                .isEqualTo("10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");

        assertThat(assertions.expression("ip_to_bits(a, 120)")
                .binding("a", "IPADDRESS 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:00ff'"))
                .hasType(VARCHAR)
                .isEqualTo("11111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111111110000000000000000");

        assertThat(assertions.expression("ip_to_bits(a, 8)")
                .binding("a", "IPADDRESS 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:00ff'"))
                .hasType(VARCHAR)
                .isEqualTo("11111111000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");

        assertThat(assertions.expression("ip_to_bits(a, 9)")
                .binding("a", "IPADDRESS 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff'"))
                .hasType(VARCHAR)
                .isEqualTo("11111111100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");

        assertThat(assertions.expression("ip_to_bits(a, 32)")
                .binding("a", "IPADDRESS '255.255.255.255'"))
                .hasType(VARCHAR)
                .isEqualTo("11111111111111111111111111111111");

        assertThat(assertions.expression("ip_to_bits(a, 16)")
                .binding("a", "IPADDRESS '255.255.255.255'"))
                .hasType(VARCHAR)
                .isEqualTo("11111111111111110000000000000000");

        assertThat(assertions.expression("ip_to_bits(a, 0)")
                .binding("a", "IPADDRESS '255.255.255.255'"))
                .hasType(VARCHAR)
                .isEqualTo("00000000000000000000000000000000");

        // Roundtrips
        assertThat(assertions.expression("ip_from_bits(ip_to_bits(a))")
                .binding("a", "IPADDRESS '192.172.1.20'"))
                .hasType(IPADDRESS)
                .isEqualTo("192.172.1.20");

        assertThat(assertions.expression("ip_from_bits(ip_to_bits(a, 24))")
                .binding("a", "IPADDRESS '192.172.1.20'"))
                .hasType(IPADDRESS)
                .isEqualTo("192.172.1.0");

        assertThat(assertions.expression("ip_from_bits(ip_to_bits(a, 112))")
                .binding("a", "IPADDRESS '4ffe:2900:5545:3210:2000:f8ff:fe21:67cf'"))
                .hasType(IPADDRESS)
                .isEqualTo("4ffe:2900:5545:3210:2000:f8ff:fe21:0");
    }

    @Test
    public void testIpAddressFromBits()
    {
        assertThat(assertions.expression("ip_from_bits(a)")
                .binding("a", "'00000000000000000000000000000000000000000000000000000000000000000000000000000000111111111111111111000000101011000000000100010100'"))
                .hasType(IPADDRESS)
                .isEqualTo("192.172.1.20");

        assertThat(assertions.expression("ip_from_bits(a)")
                .binding("a", "'01001111111111100010100100000000010101010100010100110010000100000010000000000000111110001111111111111110001000010110011111001111'"))
                .hasType(IPADDRESS)
                .isEqualTo("4ffe:2900:5545:3210:2000:f8ff:fe21:67cf");

        assertThat(assertions.expression("ip_from_bits(a)")
                .binding("a", "'00000000000000000000000000000000000000000000000000000000000000000000000000000000111111111111111100000000000000000000000000000000'"))
                .hasType(IPADDRESS)
                .isEqualTo("0.0.0.0");
    }

    @Test
    public void testIpAddressVersion()
    {
        assertThat(assertions.expression("ip_version(a)")
                .binding("a", "IPADDRESS '192.172.1.20'"))
                .hasType(INTEGER)
                .isEqualTo(4);

        assertThat(assertions.expression("ip_version(a)")
                .binding("a", "IPADDRESS '64:ff9b::'"))
                .hasType(INTEGER)
                .isEqualTo(6);
    }

    @Test
    public void testIpAddressBitsInvalidArguments()
    {
        assertTrinoExceptionThrownBy(assertions.function("ip_from_bits", "'0'")::evaluate)
                .hasMessage("Invalid IP address bits length, expected 32 or 128 but was 1");
        assertTrinoExceptionThrownBy(assertions.function("ip_from_bits", "'%s'".formatted("2".repeat(128)))::evaluate)
                .hasMessage("Invalid IP address bits, expected only '0' and '1'");
        assertTrinoExceptionThrownBy(assertions.function("ip_to_bits", "IPADDRESS '192.172.1.20'", "33")::evaluate)
                .hasMessage("Subnet address must be between 0 and 32 for IPv4, but was 33");
        assertTrinoExceptionThrownBy(assertions.function("ip_to_bits", "IPADDRESS '64:ff9b::'", "129")::evaluate)
                .hasMessage("Subnet address must be between 0 and 128 for IPv6, but was 129");
    }
}
