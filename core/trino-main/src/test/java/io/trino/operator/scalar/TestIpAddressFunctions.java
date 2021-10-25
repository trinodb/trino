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

import org.testng.annotations.Test;

import static io.trino.spi.type.BooleanType.BOOLEAN;

public class TestIpAddressFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testIpAddressContains()
    {
        // Class A (prefix length is between 1 and 8)
        assertFunction("contains('0.0.0.0/0', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('0.0.0.0/0', IPADDRESS '255.255.255.255')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/1', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('128.0.0.0/1', IPADDRESS '128.0.0.0')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/2', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('64.0.0.0/2', IPADDRESS '64.0.0.0')", BOOLEAN, true);
        assertFunction("contains('128.0.0.0/2', IPADDRESS '128.0.0.0')", BOOLEAN, true);
        assertFunction("contains('192.0.0.0/2', IPADDRESS '192.0.0.0')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/3', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('32.0.0.0/3', IPADDRESS '32.0.0.0')", BOOLEAN, true);
        assertFunction("contains('224.0.0.0/3', IPADDRESS '224.0.0.0')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/4', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('16.0.0.0/4', IPADDRESS '16.0.0.0')", BOOLEAN, true);
        assertFunction("contains('240.0.0.0/4', IPADDRESS '240.0.0.0')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/5', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('8.0.0.0/5', IPADDRESS '8.0.0.0')", BOOLEAN, true);
        assertFunction("contains('248.0.0.0/5', IPADDRESS '248.0.0.0')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/6', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('4.0.0.0/6', IPADDRESS '4.0.0.0')", BOOLEAN, true);
        assertFunction("contains('252.0.0.0/6', IPADDRESS '252.0.0.0')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/7', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('2.0.0.0/7', IPADDRESS '2.0.0.0')", BOOLEAN, true);
        assertFunction("contains('254.0.0.0/7', IPADDRESS '254.0.0.0')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/8', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('255.0.0.0/8', IPADDRESS '255.0.0.0')", BOOLEAN, true);

        // Class B (prefix length is between 9 and 16)
        assertFunction("contains('0.0.0.0/9', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('0.128.0.0/9', IPADDRESS '0.128.0.0')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/10', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('0.64.0.0/10', IPADDRESS '0.64.0.0')", BOOLEAN, true);
        assertFunction("contains('0.128.0.0/10', IPADDRESS '0.128.0.0')", BOOLEAN, true);
        assertFunction("contains('0.192.0.0/10', IPADDRESS '0.192.0.0')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/11', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('0.32.0.0/11', IPADDRESS '0.32.0.0')", BOOLEAN, true);
        assertFunction("contains('0.224.0.0/11', IPADDRESS '0.224.0.0')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/12', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('0.16.0.0/12', IPADDRESS '0.16.0.0')", BOOLEAN, true);
        assertFunction("contains('0.240.0.0/12', IPADDRESS '0.240.0.0')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/13', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('0.8.0.0/13', IPADDRESS '0.8.0.0')", BOOLEAN, true);
        assertFunction("contains('0.248.0.0/13', IPADDRESS '0.248.0.0')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/14', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('0.4.0.0/14', IPADDRESS '0.4.0.0')", BOOLEAN, true);
        assertFunction("contains('0.252.0.0/14', IPADDRESS '0.252.0.0')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/15', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('0.2.0.0/15', IPADDRESS '0.2.0.0')", BOOLEAN, true);
        assertFunction("contains('0.254.0.0/15', IPADDRESS '0.254.0.0')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/16', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('0.255.0.0/16', IPADDRESS '0.255.0.0')", BOOLEAN, true);

        // Class C (prefix length is between 17 and 24)
        assertFunction("contains('0.0.0.0/17', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('0.0.128.0/17', IPADDRESS '0.0.128.0')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/18', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('0.0.64.0/18', IPADDRESS '0.0.64.0')", BOOLEAN, true);
        assertFunction("contains('0.0.128.0/18', IPADDRESS '0.0.128.0')", BOOLEAN, true);
        assertFunction("contains('0.0.192.0/18', IPADDRESS '0.0.192.0')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/19', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('0.0.32.0/19', IPADDRESS '0.0.32.0')", BOOLEAN, true);
        assertFunction("contains('0.0.224.0/19', IPADDRESS '0.0.224.0')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/20', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('0.0.16.0/20', IPADDRESS '0.0.16.0')", BOOLEAN, true);
        assertFunction("contains('0.0.240.0/20', IPADDRESS '0.0.240.0')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/21', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('0.0.8.0/21', IPADDRESS '0.0.8.0')", BOOLEAN, true);
        assertFunction("contains('0.0.248.0/21', IPADDRESS '0.0.248.0')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/22', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('0.0.4.0/22', IPADDRESS '0.0.4.0')", BOOLEAN, true);
        assertFunction("contains('0.0.252.0/22', IPADDRESS '0.0.252.0')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/23', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('0.0.2.0/23', IPADDRESS '0.0.2.0')", BOOLEAN, true);
        assertFunction("contains('0.0.254.0/23', IPADDRESS '0.0.254.0')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/24', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('0.0.255.0/24', IPADDRESS '0.0.255.0')", BOOLEAN, true);

        // Class C (prefix length is between 25 and 32)
        assertFunction("contains('0.0.0.0/25', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('0.0.0.128/25', IPADDRESS '0.0.0.128')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/26', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('0.0.0.64/26', IPADDRESS '0.0.0.64')", BOOLEAN, true);
        assertFunction("contains('0.0.0.128/26', IPADDRESS '0.0.0.128')", BOOLEAN, true);
        assertFunction("contains('0.0.0.192/26', IPADDRESS '0.0.0.192')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/27', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('0.0.0.32/27', IPADDRESS '0.0.0.32')", BOOLEAN, true);
        assertFunction("contains('0.0.0.224/27', IPADDRESS '0.0.0.224')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/28', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('0.0.0.16/28', IPADDRESS '0.0.0.16')", BOOLEAN, true);
        assertFunction("contains('0.0.0.240/28', IPADDRESS '0.0.0.240')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/29', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('0.0.0.8/29', IPADDRESS '0.0.0.8')", BOOLEAN, true);
        assertFunction("contains('0.0.0.248/29', IPADDRESS '0.0.0.248')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/30', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('0.0.0.4/30', IPADDRESS '0.0.0.4')", BOOLEAN, true);
        assertFunction("contains('0.0.0.252/30', IPADDRESS '0.0.0.252')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/31', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('0.0.0.2/31', IPADDRESS '0.0.0.2')", BOOLEAN, true);
        assertFunction("contains('0.0.0.254/31', IPADDRESS '0.0.0.254')", BOOLEAN, true);

        assertFunction("contains('0.0.0.0/32', IPADDRESS '0.0.0.0')", BOOLEAN, true);
        assertFunction("contains('0.0.0.255/32', IPADDRESS '0.0.0.255')", BOOLEAN, true);
        assertFunction("contains('0.0.0.255/32', IPADDRESS '255.0.0.0')", BOOLEAN, false);

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

        // IPv6
        assertFunction("contains('::ffff:0000:0000/0', IPADDRESS '::ffff:0000:0000')", BOOLEAN, true);
        assertFunction("contains('::ffff:0000:0000/0', IPADDRESS '::ffff:ffff:ffff')", BOOLEAN, true);

        assertFunction("contains('64:ff9b::10.0.0.0/64', IPADDRESS '64:ff9a:f:f:f:f:f:f')", BOOLEAN, false);
        assertFunction("contains('64:ff9b::10.0.0.0/64', IPADDRESS '64:ff9b:0:0:0:0:0:0')", BOOLEAN, true);
        assertFunction("contains('64:ff9b::10.0.0.0/64', IPADDRESS '64:ff9b:0:0:f:f:f:f')", BOOLEAN, true);
        assertFunction("contains('64:ff9b::10.0.0.0/64', IPADDRESS '64:ff9b:0:1:0:0:0:0')", BOOLEAN, false);

        assertFunction("contains('2001:0DB8:0000:CD30:0000:0000:0000:0000/60', IPADDRESS '2001:0DB8::CD30:0:0:0:0')", BOOLEAN, true);
        assertFunction("contains('2620:109:c003:104::/64', IPADDRESS '2620:109:c003:104::C01')", BOOLEAN, true);

        assertFunction("contains('2001:0db8:0:0:0:ff00:0042:8329/128', IPADDRESS '2001:0db8:0:0:0:ff00:0042:8328')", BOOLEAN, false);
        assertFunction("contains('2001:0db8:0:0:0:ff00:0042:8329/128', IPADDRESS '2001:0db8:0:0:0:ff00:0042:8329')", BOOLEAN, true);
        assertFunction("contains('2001:0db8:0:0:0:ff00:0042:8329/128', IPADDRESS '2001:0db8:0:0:0:ff00:0042:8330')", BOOLEAN, false);

        assertFunction("contains('::/0', IPADDRESS '::')", BOOLEAN, true);
        assertFunction("contains('::/0', IPADDRESS '::1')", BOOLEAN, true);
        assertFunction("contains('::/0', IPADDRESS '2001::')", BOOLEAN, true);

        assertFunction("contains('::1/128', IPADDRESS '::1')", BOOLEAN, true);
        assertFunction("contains('::1/128', IPADDRESS '::')", BOOLEAN, false);
        assertFunction("contains('::1/128', IPADDRESS '2001::')", BOOLEAN, false);

        assertFunction("contains('2001:abcd:ef01:2345:6789:abcd:ef01:234/64', IPADDRESS '2001:abcd:ef01:2345::1')", BOOLEAN, true);
        assertFunction("contains('2001:abcd:ef01:2345:6789:abcd:ef01:234/64', IPADDRESS '2001:abcd:ef01:2345::')", BOOLEAN, true);
        assertFunction("contains('2001:abcd:ef01:2345:6789:abcd:ef01:234/64', IPADDRESS '2001::')", BOOLEAN, false);
        assertFunction("contains('2001:abcd:ef01:2345:6789:abcd:ef01:234/64', IPADDRESS '2001:abcd::')", BOOLEAN, false);
        assertFunction("contains('2001:abcd:ef01:2345:6789:abcd:ef01:234/64', IPADDRESS '2001:abcd:ef01:2340::')", BOOLEAN, false);
        assertFunction("contains('2001:abcd:ef01:2345:6789:abcd:ef01:234/64', IPADDRESS '2002::')", BOOLEAN, false);

        assertFunction("contains('2001:abcd:ef01:2345:6789:abcd:ef01:234/60', IPADDRESS '2001:abcd:ef01:2345::')", BOOLEAN, true);
        assertFunction("contains('2001:abcd:ef01:2345:6789:abcd:ef01:234/60', IPADDRESS '2001:abcd:ef01:2340::')", BOOLEAN, true);

        assertFunction("contains('2001:abcd:ef01:2345:6789:abcd:ef01:234/60', IPADDRESS '2001:abcd:ef01:2330::')", BOOLEAN, false);
        assertFunction("contains('2001:abcd:ef01:2345:6789:abcd:ef01:234/60', IPADDRESS '2001:abcd::')", BOOLEAN, false);
        assertFunction("contains('2001:abcd:ef01:2345:6789:abcd:ef01:234/60', IPADDRESS '2001:abcd:ef00::')", BOOLEAN, false);
        assertFunction("contains('2001:abcd:ef01:2345:6789:abcd:ef01:234/60', IPADDRESS '2001::')", BOOLEAN, false);
        assertFunction("contains('2001:abcd:ef01:2345:6789:abcd:ef01:234/60', IPADDRESS '2002::')", BOOLEAN, false);

        // NULL argument
        assertFunction("contains('10.0.0.1/0', cast(NULL as IPADDRESS))", BOOLEAN, null);
        assertFunction("contains('::ffff:1.2.3.4/0', cast(NULL as IPADDRESS))", BOOLEAN, null);

        assertFunction("contains(NULL, IPADDRESS '10.0.0.1')", BOOLEAN, null);
        assertFunction("contains(NULL, IPADDRESS '::ffff:1.2.3.4')", BOOLEAN, null);

        assertFunction("contains(NULL, cast(NULL as IPADDRESS))", BOOLEAN, null);

        // Invalid argument
        assertInvalidFunction("contains('64:ff9b::10.0.0.0/64', IPADDRESS '0.0.0.0')", "IP address version should be the same");
        assertInvalidFunction("contains('0.0.0.0/0', IPADDRESS '64:ff9b::10.0.0.0')", "IP address version should be the same");

        // Invalid prefix length
        assertInvalidFunction("contains('0.0.0.0/-1', IPADDRESS '0.0.0.0')", "Invalid prefix length");
        assertInvalidFunction("contains('64:ff9b::10.0.0.0/-1', IPADDRESS '64:ff9b::10.0.0.0')", "Invalid prefix length");

        // Invalid CIDR format
        assertInvalidFunction("contains('0.0.0.1/0', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('1.0.0.0/1', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('128.1.1.1/1', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('129.0.0.0/1', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('192.1.1.1/2', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('193.0.0.0/2', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('224.1.1.1/3', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('225.0.0.0/3', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('240.1.1.1/4', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('241.0.0.0/4', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('248.1.1.1/5', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('249.0.0.0/5', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('252.1.1.1/6', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('253.0.0.0/6', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('254.1.1.1/7', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.0.0.0/7', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.1.1.1/8', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.0.1.1/9', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.129.0.0/9', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.0.1.1/10', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.193.0.0/10', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.0.1.1/11', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.225.0.0/11', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.240.1.1/12', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.241.0.0/12', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.248.1.1/13', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.249.1.1/13', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.252.1.1/14', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.253.0.0/14', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.254.1.1/15', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.255.1.1/15', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.255.0.1/16', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.255.1.0/16', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.255.0.1/17', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.255.129.0/17', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.255.0.1/18', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.255.193.0/18', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.255.0.1/19', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.255.225.0/19', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.255.240.1/20', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.255.241.0/20', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.255.248.1/21', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.255.249.1/21', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.255.252.1/22', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.255.253.0/22', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.255.254.1/23', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.255.255.1/23', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('255.255.255.1/24', IPADDRESS '0.0.0.0')", "Invalid CIDR");

        assertInvalidFunction("contains('10.0.0.1/33', IPADDRESS '0.0.0.0')", "Prefix length exceeds address length");
        assertInvalidFunction("contains('64:ff9b::10.0.0.0/129', IPADDRESS '0.0.0.0')", "Prefix length exceeds address length");
        assertInvalidFunction("contains('2620:109:c006:104::/250', IPADDRESS '2620:109:c006:104::')", "Prefix length exceeds address length");

        assertInvalidFunction("contains('x.x.x.x', IPADDRESS '0.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('x:x:x:10.0.0.0', IPADDRESS '64:ff9b::10.0.0.0')", "Invalid CIDR");
        assertInvalidFunction("contains('x.x.x.x/1', IPADDRESS '0.0.0.0')", "Invalid network IP address");
        assertInvalidFunction("contains('x:x:x:10.0.0.0/1', IPADDRESS '64:ff9b::10.0.0.0')", "Invalid network IP address");
        assertInvalidFunction("contains('2001:0DB8:0:CD3/60', IPADDRESS '2001:0DB8::CD30:0:0:0:0')", "Invalid network IP address");
    }
}
