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
package io.trino.plugin.varada.storage.engine.nativeimpl;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.varada.node.CoordinatorInitializedEvent;
import io.trino.plugin.varada.storage.engine.ConnectorSync;
import io.trino.plugin.varada.storage.engine.ConnectorSyncInitializedEvent;
import io.varada.tools.CatalogNameProvider;

import static java.util.Objects.requireNonNull;

@Singleton
public class CoordinatorNativeConnectorSync
        implements ConnectorSync
{
    private static final Logger logger = Logger.get(CoordinatorNativeConnectorSync.class);
    private final CatalogNameProvider catalogNameProvider;
    private final EventBus eventBus;
    private Integer catalogSequence;

    @Inject
    public CoordinatorNativeConnectorSync(CatalogNameProvider catalogNameProvider,
            EventBus eventBus)
    {
        this.catalogNameProvider = catalogNameProvider;
        this.eventBus = requireNonNull(eventBus);
        eventBus.register(this);
    }

    @Subscribe
    public void init(CoordinatorInitializedEvent coordinatorInitializedEvent)
    {
        try {
            logger.debug("nativeConnectorSync from init, %d", System.identityHashCode(this));
            String catalogName = catalogNameProvider.get();
            catalogSequence = register(catalogName, 1800);
            logger.info("catalog name %s sequence %d", catalogName, catalogSequence);
            eventBus.post(new ConnectorSyncInitializedEvent(catalogSequence));
        }
        catch (Throwable e) {
            logger.error("failed to register");
            logger.error(e);
            throw new RuntimeException(e);
        }
        finally {
            logger.debug("register finally");
        }
    }

    @Override
    public int getCatalogSequence()
    {
        return catalogSequence;
    }

    public native int register(String catalogName, int timeoutSec);
}
