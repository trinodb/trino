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
package io.trino.plugin.varada.node;

import com.google.common.eventbus.EventBus;
import io.trino.plugin.varada.configuration.GlobalConfiguration;
import io.trino.spi.Node;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class CoordinatorInitializedEventHandlerTest
{
    @Test
    public void testCoordinatorInitializedEvent()
    {
        CoordinatorInitializedEvent event = new CoordinatorInitializedEvent(mock(Node.class));

        GlobalConfiguration globalConfiguration = new GlobalConfiguration();
        CoordinatorInitializedEventHandler handler = new CoordinatorInitializedEventHandler(mock(EventBus.class),
                globalConfiguration);

        handler.handleEvent(event);

        assertThat(globalConfiguration.getClusterUpTime()).isNotEqualTo(0);

        globalConfiguration.setClusterUpTime(2022L);
        handler.handleEvent(event);
        assertThat(globalConfiguration.getClusterUpTime()).isEqualTo(2022);
    }
}
