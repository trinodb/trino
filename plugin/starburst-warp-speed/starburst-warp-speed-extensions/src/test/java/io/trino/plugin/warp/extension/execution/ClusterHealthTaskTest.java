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
package io.trino.plugin.warp.extension.execution;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.airlift.json.ObjectMapperProvider;
import io.trino.plugin.varada.api.health.HealthNode;
import io.trino.plugin.varada.api.health.HealthResult;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

public class ClusterHealthTaskTest
{
    @Test
    public void testHealthResult()
            throws JsonProcessingException
    {
        HealthNode healthNode = new HealthNode(System.currentTimeMillis(), "nodeIdentifier", "http://test.com", "UP");
        HealthResult result = new HealthResult(true,
                URI.create("http://test.com"),
                Instant.now().toEpochMilli(),
                ImmutableList.of(healthNode),
                1L);

        ObjectMapper objectMapper = new ObjectMapperProvider().get();
        String resultStr = objectMapper.writeValueAsString(result);

        HealthResult expected = objectMapper.readerFor(HealthResult.class).readValue(resultStr);
        assertThat(result).isEqualTo(expected);
    }
}
