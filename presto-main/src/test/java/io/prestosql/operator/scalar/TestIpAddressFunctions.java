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
package io.prestosql.operator.scalar;

import org.testng.annotations.Test;

import static io.prestosql.spi.type.BooleanType.BOOLEAN;

public class TestIpAddressFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testIpAddressContains()
    {
        // Prefix length is 0
        assertFunction("contains('10.0.0.1/0', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('10.0.0.1/0', IPADDRESS '255.255.255.255')", BOOLEAN, true);

        assertFunction("contains('::ffff:0a00:0001/0', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('::ffff:0a00:0001/0', IPADDRESS '255.255.255.255')", BOOLEAN, true);

        assertFunction("contains('10.0.0.1/0', IPADDRESS '::ffff:0000:0000')", BOOLEAN, true);
        assertFunction("contains('10.0.0.1/0', IPADDRESS '::ffff:ffff:ffff')", BOOLEAN, true);

        // 10.0.0.1 equals ::ffff:0a00:0001 in IPv6
        assertFunction("contains('10.0.0.1/8', IPADDRESS '9.255.255.255')", BOOLEAN, false);
        assertFunction("contains('10.0.0.1/8', IPADDRESS '10.0.0.0')", BOOLEAN, true);
        assertFunction("contains('10.0.0.1/8', IPADDRESS '10.255.255.255')", BOOLEAN, true);
        assertFunction("contains('10.0.0.1/8', IPADDRESS '11.255.255.255')", BOOLEAN, false);

        assertFunction("contains('::ffff:0a00:0001/8', IPADDRESS '9.255.255.255')", BOOLEAN, false);
        assertFunction("contains('::ffff:0a00:0001/8', IPADDRESS '10.0.0.0')", BOOLEAN, true);
        assertFunction("contains('::ffff:0a00:0001/8', IPADDRESS '10.255.255.255')", BOOLEAN, true);
        assertFunction("contains('::ffff:0a00:0001/8', IPADDRESS '11.255.255.255')", BOOLEAN, false);

        assertFunction("contains('10.0.0.1/8', IPADDRESS '::ffff:09ff:ffff')", BOOLEAN, false);
        assertFunction("contains('10.0.0.1/8', IPADDRESS '::ffff:0a00:0000')", BOOLEAN, true);
        assertFunction("contains('10.0.0.1/8', IPADDRESS '::ffff:0aff:ffff')", BOOLEAN, true);
        assertFunction("contains('10.0.0.1/8', IPADDRESS '::ffff:0bff:ffff')", BOOLEAN, false);

        // 127.0.0.1 equals ::ffff:7f00:0001 in IPv6
        assertFunction("contains('127.0.0.1/32', IPADDRESS '127.0.0.0')", BOOLEAN, false);
        assertFunction("contains('127.0.0.1/32', IPADDRESS '127.0.0.1')", BOOLEAN, true);
        assertFunction("contains('127.0.0.1/32', IPADDRESS '127.0.0.2')", BOOLEAN, false);

        assertFunction("contains('::ffff:7f00:0001/32', IPADDRESS '127.0.0.0')", BOOLEAN, false);
        assertFunction("contains('::ffff:7f00:0001/32', IPADDRESS '127.0.0.1')", BOOLEAN, true);
        assertFunction("contains('::ffff:7f00:0001/32', IPADDRESS '127.0.0.2')", BOOLEAN, false);

        assertFunction("contains('127.0.0.1/32', IPADDRESS '::ffff:7f00:0000')", BOOLEAN, false);
        assertFunction("contains('127.0.0.1/32', IPADDRESS '::ffff:7f00:0001')", BOOLEAN, true);
        assertFunction("contains('127.0.0.1/32', IPADDRESS '::ffff:7f00:0002')", BOOLEAN, false);

        // IPv4 out of range
        assertFunction("contains('64:ff9b::10.0.0.0/64', IPADDRESS '64:ff9a:f:f:f:f:f:f')", BOOLEAN, false);
        assertFunction("contains('64:ff9b::10.0.0.0/64', IPADDRESS '64:ff9b:0:0:0:0:0:0')", BOOLEAN, true);
        assertFunction("contains('64:ff9b::10.0.0.0/64', IPADDRESS '64:ff9b:0:0:f:f:f:f')", BOOLEAN, true);
        assertFunction("contains('64:ff9b::10.0.0.0/64', IPADDRESS '64:ff9b:0:1:0:0:0:0')", BOOLEAN, false);

        assertFunction("contains('2001:0db8:0:0:0:ff00:0042:8329/128', IPADDRESS '2001:0db8:0:0:0:ff00:0042:8328')", BOOLEAN, false);
        assertFunction("contains('2001:0db8:0:0:0:ff00:0042:8329/128', IPADDRESS '2001:0db8:0:0:0:ff00:0042:8329')", BOOLEAN, true);
        assertFunction("contains('2001:0db8:0:0:0:ff00:0042:8329/128', IPADDRESS '2001:0db8:0:0:0:ff00:0042:8330')", BOOLEAN, false);

        assertFunction("contains('0.0.0.0/0', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('0.0.0.0/0', IPADDRESS '255.255.255.255')", BOOLEAN, true);
        assertFunction("contains('0.0.0.0/0', IPADDRESS '::ffff:ffff:ffff')", BOOLEAN, true);

        // NULL argument
        assertFunction("contains('10.0.0.1/0', cast(NULL as IPADDRESS))", BOOLEAN, null);
        assertFunction("contains('::ffff:1.2.3.4/0', cast(NULL as IPADDRESS))", BOOLEAN, null);

        assertFunction("contains(NULL, IPADDRESS '10.0.0.1')", BOOLEAN, null);
        assertFunction("contains(NULL, IPADDRESS '::ffff:1.2.3.4')", BOOLEAN, null);

        assertFunction("contains(NULL, cast(NULL as IPADDRESS))", BOOLEAN, null);

        // Invalid argument
        assertInvalidFunction("contains('64:ff9b::10.0.0.0/64', IPADDRESS '10.0.0.0')", "IP address version should be the same");
        assertInvalidFunction("contains('10.0.0.0/0', IPADDRESS '64:ff9b::10.0.0.0')", "IP address version should be the same");

        assertInvalidFunction("contains('10.0.0.1/-1', IPADDRESS '0.0.0.0')", "Invalid prefix length");
        assertInvalidFunction("contains('64:ff9b::10.0.0.0/-1', IPADDRESS '0.0.0.0')", "Invalid prefix length");
        assertInvalidFunction("contains('10.0.0.1/33', IPADDRESS '0.0.0.0')", "Prefix length exceeds address length");
        assertInvalidFunction("contains('64:ff9b::10.0.0.0/129', IPADDRESS '0.0.0.0')", "Prefix length exceeds address length");
        assertInvalidFunction("contains('x.x.x.x', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('x:x:x:10.0.0.0', IPADDRESS '64:ff9b::10.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('x.x.x.x/1', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('x:x:x:10.0.0.0/1', IPADDRESS '64:ff9b::10.0.0.0')", "Invalid CIDR");
    }
}
