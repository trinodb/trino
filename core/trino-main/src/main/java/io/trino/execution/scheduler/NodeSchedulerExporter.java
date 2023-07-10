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
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.airlift.stats.CounterStat;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.JmxException;
import org.weakref.jmx.MBeanExport;
import org.weakref.jmx.MBeanExporter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public final class NodeSchedulerExporter
{
    @GuardedBy("this")
    private final List<MBeanExport> mbeanExports = new ArrayList<>();

    @Inject
    public NodeSchedulerExporter(TopologyAwareNodeSelectorFactory nodeSelectorFactory, MBeanExporter exporter)
    {
        requireNonNull(nodeSelectorFactory, "nodeSelectorFactory is null");
        requireNonNull(exporter, "exporter is null");
        for (Map.Entry<String, CounterStat> entry : nodeSelectorFactory.getPlacementCountersByName().entrySet()) {
            try {
                mbeanExports.add(exporter.exportWithGeneratedName(entry.getValue(), NodeScheduler.class, ImmutableMap.of("segment", entry.getKey())));
            }
            catch (JmxException e) {
                // ignored
            }
        }
    }

    @PreDestroy
    public synchronized void destroy()
    {
        for (MBeanExport mbeanExport : mbeanExports) {
            try {
                mbeanExport.unexport();
            }
            catch (JmxException e) {
                // ignored
            }
        }
        mbeanExports.clear();
    }
}
