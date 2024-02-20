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
package io.trino.plugin.cassandra;

import com.datastax.oss.driver.internal.core.metadata.token.Murmur3Token;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

import static java.math.BigInteger.ONE;
import static java.math.BigInteger.ZERO;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;

public class TestMurmur3PartitionerTokenRing
{
    private static final Murmur3PartitionerTokenRing tokenRing = Murmur3PartitionerTokenRing.INSTANCE;

    @Test
    public void testGetTokenCountInRange()
    {
        assertThat(tokenRing.getTokenCountInRange(new Murmur3Token(0), new Murmur3Token(1))).isEqualTo(ONE);
        assertThat(tokenRing.getTokenCountInRange(new Murmur3Token(-1), new Murmur3Token(1))).isEqualTo(new BigInteger("2"));
        assertThat(tokenRing.getTokenCountInRange(new Murmur3Token(-100), new Murmur3Token(100))).isEqualTo(new BigInteger("200"));
        assertThat(tokenRing.getTokenCountInRange(new Murmur3Token(0), new Murmur3Token(10))).isEqualTo(new BigInteger("10"));
        assertThat(tokenRing.getTokenCountInRange(new Murmur3Token(1), new Murmur3Token(11))).isEqualTo(new BigInteger("10"));
        assertThat(tokenRing.getTokenCountInRange(new Murmur3Token(0), new Murmur3Token(0))).isEqualTo(ZERO);
        assertThat(tokenRing.getTokenCountInRange(new Murmur3Token(1), new Murmur3Token(1))).isEqualTo(ZERO);
        assertThat(tokenRing.getTokenCountInRange(new Murmur3Token(Long.MIN_VALUE), new Murmur3Token(Long.MIN_VALUE))).isEqualTo(BigInteger.valueOf(2).pow(64).subtract(ONE));
        assertThat(tokenRing.getTokenCountInRange(new Murmur3Token(1), new Murmur3Token(0))).isEqualTo(BigInteger.valueOf(2).pow(64).subtract(BigInteger.valueOf(2)));
    }

    @Test
    public void testGetRingFraction()
    {
        assertThat(tokenRing.getRingFraction(new Murmur3Token(1), new Murmur3Token(1))).isCloseTo(0.0, offset(0.001));
        assertThat(tokenRing.getRingFraction(new Murmur3Token(1), new Murmur3Token(0))).isCloseTo(1.0, offset(0.001));
        assertThat(tokenRing.getRingFraction(new Murmur3Token(0), new Murmur3Token(Long.MAX_VALUE))).isCloseTo(0.5, offset(0.001));
        assertThat(tokenRing.getRingFraction(new Murmur3Token(Long.MIN_VALUE), new Murmur3Token(Long.MAX_VALUE))).isCloseTo(1.0, offset(0.001));
        assertThat(tokenRing.getRingFraction(new Murmur3Token(Long.MIN_VALUE), new Murmur3Token(Long.MIN_VALUE))).isCloseTo(1.0, offset(0.001));
    }
}
