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
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.configuration.GlobalConfiguration;

import java.time.Instant;

import static java.util.Objects.requireNonNull;

@SuppressWarnings("UnstableApiUsage")
@Singleton
public class CoordinatorInitializedEventHandler
{
    private static final Logger logger = Logger.get(CoordinatorInitializedEventHandler.class);

    private final GlobalConfiguration globalConfiguration;

    @Inject
    public CoordinatorInitializedEventHandler(EventBus eventBus,
            GlobalConfiguration globalConfiguration)
    {
        requireNonNull(eventBus);
        this.globalConfiguration = requireNonNull(globalConfiguration);
        eventBus.register(this);
    }

    @Subscribe
    public void handleEvent(CoordinatorInitializedEvent event)
    {
        if (globalConfiguration.getClusterUpTime() > 0) {
            logger.debug("skipping CoordinatorInitializedEvent.ClusterUpTime since ClusterUpTime=%d",
                    globalConfiguration.getClusterUpTime());
            return; // ignore
        }
        logger.debug("setting CoordinatorInitializedEvent.ClusterUpTime new ClusterUpTime=%d",
                     Instant.now().toEpochMilli());
        globalConfiguration.setClusterUpTime(Instant.now().toEpochMilli());
    }
}
