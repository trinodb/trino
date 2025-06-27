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

import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.client.NodeVersion.UNKNOWN;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

public class TestServerInfo
{
    private static final JsonCodec<ServerInfo> SERVER_INFO_CODEC = jsonCodec(ServerInfo.class);

    @Test
    public void testJsonRoundTrip()
    {
        assertJsonRoundTrip(new ServerInfo(UNKNOWN, "test", true, false, Optional.of(new Duration(2, MINUTES)), Optional.of("3sruz")));
        assertJsonRoundTrip(new ServerInfo(UNKNOWN, "test", true, false, Optional.of(new Duration(2, MINUTES)), Optional.empty()));
        assertJsonRoundTrip(new ServerInfo(UNKNOWN, "test", true, false, Optional.empty(), Optional.empty()));
    }

    @Test
    public void testBackwardsCompatible()
    {
        ServerInfo newServerInfo = new ServerInfo(UNKNOWN, "test", true, false, Optional.empty(), Optional.empty());
        ServerInfo legacyServerInfo = SERVER_INFO_CODEC.fromJson("{\"nodeVersion\":{\"version\":\"<unknown>\"},\"environment\":\"test\",\"coordinator\":true}");
        assertThat(newServerInfo).isEqualTo(legacyServerInfo);
    }

    private static void assertJsonRoundTrip(ServerInfo serverInfo)
    {
        String json = SERVER_INFO_CODEC.toJson(serverInfo);
        ServerInfo copy = SERVER_INFO_CODEC.fromJson(json);
        assertThat(copy).isEqualTo(serverInfo);
    }
}
