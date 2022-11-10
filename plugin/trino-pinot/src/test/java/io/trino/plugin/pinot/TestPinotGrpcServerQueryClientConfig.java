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
package io.trino.plugin.pinot;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.DataSize;
import io.trino.plugin.pinot.client.PinotGrpcServerQueryClientConfig;
import org.testng.annotations.Test;

import java.util.Map;

import static org.apache.pinot.common.config.GrpcConfig.DEFAULT_MAX_INBOUND_MESSAGE_BYTES_SIZE;

public class TestPinotGrpcServerQueryClientConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(
                ConfigAssertions.recordDefaults(PinotGrpcServerQueryClientConfig.class)
                        .setMaxRowsPerSplitForSegmentQueries(Integer.MAX_VALUE - 1)
                        .setGrpcPort(8090)
                        .setUsePlainText(true)
                        .setMaxInboundMessageSize(DataSize.ofBytes(DEFAULT_MAX_INBOUND_MESSAGE_BYTES_SIZE))
                        .setProxyUri(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("pinot.max-rows-per-split-for-segment-queries", "10")
                .put("pinot.grpc.port", "8091")
                .put("pinot.grpc.use-plain-text", "false")
                .put("pinot.grpc.max-inbound-message-size", String.valueOf(DataSize.ofBytes(1)))
                .put("pinot.grpc.proxy-uri", "my-pinot-proxy:8094")
                .buildOrThrow();
        PinotGrpcServerQueryClientConfig expected = new PinotGrpcServerQueryClientConfig()
                .setMaxRowsPerSplitForSegmentQueries(10)
                .setGrpcPort(8091)
                .setUsePlainText(false)
                .setMaxInboundMessageSize(DataSize.ofBytes(1))
                .setProxyUri("my-pinot-proxy:8094");
        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
