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
package io.prestosql.operator.scalar.timestamp;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestTimestamp
        extends BaseTestTimestamp
{
    protected TestTimestamp()
    {
        super(false);
    }

    @Test
    public void testToUnixTime()
    {
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56')")).matches("1589114096e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.1')")).matches("1589114096.1e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.12')")).matches("1589114096.12e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.123')")).matches("1589114096.123e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.1234')")).matches("1589114096.1234e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.12345')")).matches("1589114096.12345e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.123456')")).matches("1589114096.123456e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.1234567')")).matches("1589114096.1234567e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.12345678')")).matches("1589114096.1234567e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.123456789')")).matches("1589114096.1234567e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.1234567890')")).matches("1589114096.1234567e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.12345678901')")).matches("1589114096.1234567e0");
        assertThat(assertions.expression("to_unixtime(TIMESTAMP '2020-05-10 12:34:56.123456789012')")).matches("1589114096.1234567e0");
    }
}
