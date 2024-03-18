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
package io.trino.plugin.warp.extension.execution.debugtools;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.varada.storage.engine.ConnectorSync;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;

@TaskResourceMarker
@Singleton
@Path(CatalogTask.TASK_NAME)
public class CatalogTask
        implements TaskResource
{
    public static final String TASK_NAME = "catalog";
    public static final String IS_DEFAULT = "is-default";

    private final ConnectorSync connectorSync;

    @Inject
    public CatalogTask(ConnectorSync connectorSync)
    {
        this.connectorSync = connectorSync;
    }

    @GET
    @Path(IS_DEFAULT)
    public boolean isDefault()
    {
        return connectorSync.getCatalogSequence() == ConnectorSync.DEFAULT_CATALOG;
    }
}
