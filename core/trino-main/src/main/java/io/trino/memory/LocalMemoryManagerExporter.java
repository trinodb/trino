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
package io.trino.memory;

import org.weakref.jmx.JmxException;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.ObjectNames;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public final class LocalMemoryManagerExporter
{
    public static final String EXPORTED_POOL_NAME = "general";
    private final MBeanExporter exporter;
    private final boolean poolExported;

    @Inject
    public LocalMemoryManagerExporter(LocalMemoryManager memoryManager, MBeanExporter exporter)
    {
        this.exporter = requireNonNull(exporter, "exporter is null");
        boolean poolExportedLocal = false;
        try {
            this.exporter.exportWithGeneratedName(memoryManager.getMemoryPool(), MemoryPool.class, EXPORTED_POOL_NAME);
            poolExportedLocal = true;
        }
        catch (JmxException e) {
            // ignored
        }
        this.poolExported = poolExportedLocal;
    }

    @PreDestroy
    public void destroy()
    {
        if (!poolExported) {
            return;
        }

        String objectName = ObjectNames.builder(MemoryPool.class, EXPORTED_POOL_NAME).build();
        try {
            exporter.unexport(objectName);
        }
        catch (JmxException e) {
            // ignored
        }
    }
}
