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
package io.trino.server;

import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.trino.client.NodeVersion;
import io.trino.node.NodeState;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.assertj.core.api.Assertions.assertThat;

public class TestServerInfoSerialization
{
    private static final JsonCodec<io.trino.server.ServerInfo> SERVER_SERVER_INFO_CODEC = jsonCodec(io.trino.server.ServerInfo.class);
    private static final JsonCodec<io.trino.client.ServerInfo> CLIENT_SERVER_INFO_CODEC = jsonCodec(io.trino.client.ServerInfo.class);

    @Test
    void testServerInfoSerialization()
    {
        io.trino.server.ServerInfo serverServerInfo = new io.trino.server.ServerInfo("some-node-id", NodeState.ACTIVE, new NodeVersion("some-version"), "some-env", true, Optional.of("some-coordinator-id"), true, Duration.valueOf("1h"));
        io.trino.client.ServerInfo clientServerInfo = new io.trino.client.ServerInfo(new NodeVersion("some-version"), "some-env", true, true, Optional.of(Duration.valueOf("1h")), Optional.of("some-coordinator-id"), Optional.of("some-node-id"));
        assertThat(CLIENT_SERVER_INFO_CODEC.fromJson(SERVER_SERVER_INFO_CODEC.toJson(serverServerInfo))).isEqualTo(clientServerInfo);
    }
}
