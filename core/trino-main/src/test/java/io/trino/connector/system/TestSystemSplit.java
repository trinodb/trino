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
package io.trino.connector.system;

import io.airlift.json.JsonCodec;
import io.trino.spi.HostAddress;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSystemSplit
{
    @Test
    public void testSerialization()
    {
        SystemSplit expected = new SystemSplit(HostAddress.fromParts("127.0.0.1", 0), TupleDomain.all());

        JsonCodec<SystemSplit> codec = jsonCodec(SystemSplit.class);
        SystemSplit actual = codec.fromJson(codec.toJson(expected));

        assertThat(actual.getAddresses()).isEqualTo(expected.getAddresses());
        assertThat(actual.getConstraint()).isEqualTo(expected.getConstraint());
    }
}
