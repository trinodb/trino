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
package io.trino.plugin.warp.extension.execution.initworker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.json.ObjectMapperProvider;
import io.trino.plugin.varada.configuration.NativeConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CollectNodeInfoTaskTest
{
    private NativeConfiguration nativeConfiguration;

    @BeforeEach
    @SuppressWarnings("MockNotUsedInProduction")
    public void before()
    {
        nativeConfiguration = new NativeConfiguration();
    }

    @Test
    public void testCollectInfo()
    {
        CollectNodeInfoTask task = new CollectNodeInfoTask(nativeConfiguration);

        CollectNodeInfoResult collectNodeInfoResult = task.collectNodeInfo();
        assertThat(collectNodeInfoResult).isNotNull();
    }

    @Test
    public void testCollectInfoApi()
            throws JsonProcessingException
    {
        ObjectMapper objectMapper = new ObjectMapperProvider().get();

        CollectNodeInfoResult collectNodeInfoResult = new CollectNodeInfoResult(0);
        assertThat(collectNodeInfoResult).isEqualTo(objectMapper.readerFor(CollectNodeInfoResult.class)
                .readValue(objectMapper.writeValueAsString(collectNodeInfoResult)));
    }
}
