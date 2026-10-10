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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.pinot.client.InstanceInfo;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.pinot.MetadataUtil.SPLIT_JSON_CODEC;
import static io.trino.plugin.pinot.PinotSplit.createBrokerSplit;
import static io.trino.plugin.pinot.PinotSplit.createSegmentSplit;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPinotSplit
{
    @Test
    public void testSegmentSplitCarriesInstanceInfoAcrossSerialization()
    {
        InstanceInfo instanceInfo = new InstanceInfo("Server_pinot-server-0-0_8098", "pinot-server-0-0-0.pinot-server-headless.svc.cluster.local", 8098, 8096);
        PinotSplit split = createSegmentSplit("_OFFLINE", ImmutableList.of("segment1", "segment2"), "Server_pinot-server-0-0_8098", instanceInfo, Optional.of("ts > 0"));

        PinotSplit decoded = SPLIT_JSON_CODEC.fromJson(SPLIT_JSON_CODEC.toJson(split));

        assertThat(decoded.getSplitType()).isEqualTo(PinotSplit.SplitType.SEGMENT);
        assertThat(decoded.getSegmentHost()).contains("Server_pinot-server-0-0_8098");
        assertThat(decoded.getInstanceInfo()).contains(instanceInfo);
        assertThat(decoded.getSegments()).containsExactly("segment1", "segment2");
        assertThat(decoded.getTimePredicate()).contains("ts > 0");
    }

    @Test
    public void testBrokerSplitHasNoInstanceInfo()
    {
        PinotSplit decoded = SPLIT_JSON_CODEC.fromJson(SPLIT_JSON_CODEC.toJson(createBrokerSplit()));
        assertThat(decoded.getSplitType()).isEqualTo(PinotSplit.SplitType.BROKER);
        assertThat(decoded.getInstanceInfo()).isEmpty();
    }
}
