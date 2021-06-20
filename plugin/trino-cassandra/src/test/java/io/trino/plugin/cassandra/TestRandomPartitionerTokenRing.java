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

import com.datastax.oss.driver.internal.core.metadata.token.RandomToken;
import org.testng.annotations.Test;

import java.math.BigInteger;

import static java.math.BigInteger.ONE;
import static java.math.BigInteger.ZERO;
import static org.testng.Assert.assertEquals;

public class TestRandomPartitionerTokenRing
{
    private static final RandomPartitionerTokenRing tokenRing = RandomPartitionerTokenRing.INSTANCE;

    @Test
    public void testGetRingFraction()
    {
        assertEquals(tokenRing.getTokenCountInRange(randomToken(0), randomToken(1)), ONE);
        assertEquals(tokenRing.getTokenCountInRange(randomToken(0), randomToken(200)), new BigInteger("200"));
        assertEquals(tokenRing.getTokenCountInRange(randomToken(0), randomToken(10)), new BigInteger("10"));
        assertEquals(tokenRing.getTokenCountInRange(randomToken(1), randomToken(11)), new BigInteger("10"));
        assertEquals(tokenRing.getTokenCountInRange(randomToken(0), randomToken(0)), ZERO);
        assertEquals(tokenRing.getTokenCountInRange(randomToken(-1), randomToken(-1)), BigInteger.valueOf(2).pow(127).add(ONE));
        assertEquals(tokenRing.getTokenCountInRange(randomToken(1), randomToken(0)), BigInteger.valueOf(2).pow(127));
    }

    @Test
    public void testGetTokenCountInRange()
    {
        assertEquals(tokenRing.getRingFraction(randomToken(0), randomToken(0)), 0.0, 0.001);
        assertEquals(tokenRing.getRingFraction(randomToken(1), randomToken(0)), 1.0, 0.001);
        assertEquals(tokenRing.getRingFraction(randomToken(-1), randomToken(-1)), 1.0, 0.001);
        assertEquals(tokenRing.getRingFraction(randomToken(0), randomToken(BigInteger.valueOf(2).pow(126))), 0.5, 0.001);
        assertEquals(tokenRing.getRingFraction(randomToken(BigInteger.valueOf(2).pow(126)), randomToken(BigInteger.valueOf(2).pow(127))), 0.5, 0.001);
        assertEquals(tokenRing.getRingFraction(randomToken(0), randomToken(BigInteger.valueOf(2).pow(126))), 0.5, 0.001);
        assertEquals(tokenRing.getRingFraction(randomToken(0), randomToken(BigInteger.valueOf(2).pow(127))), 1.0, 0.001);
    }

    private static RandomToken randomToken(long value)
    {
        return randomToken(BigInteger.valueOf(value));
    }

    private static RandomToken randomToken(BigInteger value)
    {
        return new RandomToken(value);
    }
}
