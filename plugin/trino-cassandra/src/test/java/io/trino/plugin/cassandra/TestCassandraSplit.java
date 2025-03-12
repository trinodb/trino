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

import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.trino.spi.HostAddress;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestCassandraSplit
{
    private final JsonCodec<CassandraSplit> codec = JsonCodec.jsonCodec(CassandraSplit.class);

    private final ImmutableList<HostAddress> addresses = ImmutableList.of(
            HostAddress.fromParts("127.0.0.1", 44),
            HostAddress.fromParts("127.0.0.1", 45));

    @Test
    public void testJsonRoundTrip()
    {
        CassandraSplit expected = new CassandraSplit("partitionId", "condition", addresses);

        String json = codec.toJson(expected);
        CassandraSplit actual = codec.fromJson(json);

        assertThat(actual.splitCondition()).isEqualTo(expected.splitCondition());
        assertThat(actual.addresses()).isEqualTo(expected.addresses());
    }

    @Test
    public void testWhereClause()
    {
        CassandraSplit split;
        split = new CassandraSplit(
                CassandraPartition.UNPARTITIONED_ID,
                "token(k) >= 0 AND token(k) <= 2",
                addresses);
        assertThat(split.getWhereClause()).isEqualTo(" WHERE token(k) >= 0 AND token(k) <= 2");

        split = new CassandraSplit(
                "key = 123",
                null,
                addresses);
        assertThat(split.getWhereClause()).isEqualTo(" WHERE key = 123");
    }
}
