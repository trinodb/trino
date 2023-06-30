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
package io.trino.plugin.jdbc;

import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJdbcSplit
{
    private final JdbcSplit split = new JdbcSplit(Optional.of("additional predicate"));

    @Test
    public void testAddresses()
    {
        // split uses "example" scheme so no addresses are available and is not remotely accessible
        assertThat(split.getAddresses()).isEmpty();
        assertThat(split.isRemotelyAccessible()).isTrue();

        JdbcSplit jdbcSplit = new JdbcSplit(Optional.empty());
        assertThat(jdbcSplit.getAddresses()).isEmpty();
    }

    @Test
    public void testJsonRoundTrip()
    {
        JsonCodec<JdbcSplit> codec = jsonCodec(JdbcSplit.class);
        String json = codec.toJson(split);
        JdbcSplit copy = codec.fromJson(json);
        assertThat(copy.getAdditionalPredicate()).isEqualTo(split.getAdditionalPredicate());

        assertThat(copy.getAddresses()).isEmpty();
        assertThat(copy.isRemotelyAccessible()).isTrue();
    }
}
