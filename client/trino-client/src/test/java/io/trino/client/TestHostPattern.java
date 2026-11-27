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
package io.trino.client;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestHostPattern
{
    @Test
    void testExactHost()
    {
        HostPattern pattern = HostPattern.parse("trino.example.com");
        assertThat(pattern.matches("trino.example.com")).isTrue();
        assertThat(pattern.matches("Trino.EXAMPLE.com")).isTrue();
        assertThat(pattern.matches("example.com")).isFalse();
        assertThat(pattern.matches("other.trino.example.com")).isFalse();
        assertThat(HostPattern.parse("TRINO.Example.com").matches("trino.example.com")).isTrue();
    }

    @Test
    void testTrailingDot()
    {
        assertThat(HostPattern.parse("trino.example.com.").matches("trino.example.com")).isTrue();
        assertThat(HostPattern.parse("trino.example.com").matches("trino.example.com.")).isTrue();
        assertThat(HostPattern.parse("*.example.com").matches("trino.example.com.")).isTrue();
    }

    @Test
    void testSingleLabel()
    {
        HostPattern pattern = HostPattern.parse("localhost");
        assertThat(pattern.matches("localhost")).isTrue();
        assertThat(pattern.matches("localhost.example.com")).isFalse();
        assertThat(pattern.matches("")).isFalse();
    }

    @Test
    void testLeadingWildcard()
    {
        HostPattern pattern = HostPattern.parse("*.example.com");
        assertThat(pattern.matches("trino.example.com")).isTrue();
        assertThat(pattern.matches("a.b.example.com")).isTrue();
        assertThat(pattern.matches("example.com")).isFalse();
        assertThat(pattern.matches("notexample.com")).isFalse();
        assertThat(pattern.matches("example.com.attacker.net")).isFalse();
    }

    @Test
    void testInnerWildcard()
    {
        HostPattern pattern = HostPattern.parse("*.s3.*.amazonaws.com");
        assertThat(pattern.matches("bucket.s3.us-east-1.amazonaws.com")).isTrue();
        assertThat(pattern.matches("dotted.bucket.s3.us-east-1.amazonaws.com")).isTrue();
        assertThat(pattern.matches("bucket.s3.amazonaws.com")).isFalse();
        assertThat(pattern.matches("bucket.s3.dualstack.us-east-1.amazonaws.com")).isFalse();
        assertThat(pattern.matches("ec2-1-2-3-4.compute-1.amazonaws.com")).isFalse();
    }

    @Test
    void testMultipleWildcards()
    {
        HostPattern pattern = HostPattern.parse("*.*.example.com");
        assertThat(pattern.matches("a.b.example.com")).isTrue();
        assertThat(pattern.matches("a.b.c.example.com")).isTrue();
        assertThat(pattern.matches("b.example.com")).isFalse();
    }

    @Test
    void testIpAddress()
    {
        assertThat(HostPattern.parse("10.0.0.1").matches("10.0.0.1")).isTrue();
        assertThat(HostPattern.parse("*.example.com").matches("169.254.169.254")).isFalse();
    }

    @Test
    void testIpv6Address()
    {
        assertThat(HostPattern.parse("::1").matches("::1")).isTrue();
        assertThat(HostPattern.parse("[::1]").matches("::1")).isTrue();
        assertThat(HostPattern.parse("0:0:0:0:0:0:0:1").matches("::1")).isTrue();
        assertThat(HostPattern.parse("2001:DB8::1").matches("2001:db8::1")).isTrue();
        assertThat(HostPattern.parse("::1").matches("::2")).isFalse();
    }

    @Test
    void testInvalidPattern()
    {
        assertThatThrownBy(() -> HostPattern.parse("*"))
                .hasMessage("Host pattern '*' matches every host");
        assertThatThrownBy(() -> HostPattern.parse("*.*"))
                .hasMessage("Host pattern '*.*' matches every host");
        assertThatThrownBy(() -> HostPattern.parse("trino*.example.com"))
                .hasMessage("Host pattern 'trino*.example.com' can only use * as a whole label");
        assertThatThrownBy(() -> HostPattern.parse("example..com"))
                .hasMessage("Host pattern 'example..com' contains an empty label");
        assertThatThrownBy(() -> HostPattern.parse(""))
                .hasMessage("Host pattern '' contains an empty label");
        assertThatThrownBy(() -> HostPattern.parse("trino.example.com:8443"))
                .hasMessage("Host pattern 'trino.example.com:8443' cannot contain a port");
        assertThatThrownBy(() -> HostPattern.parse("[::1]:8443"))
                .hasMessage("Host pattern '[::1]:8443' cannot contain a port");
    }
}
