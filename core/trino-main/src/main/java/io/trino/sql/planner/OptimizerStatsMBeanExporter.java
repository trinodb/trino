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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import io.trino.sql.planner.iterative.IterativeOptimizer;
import io.trino.sql.planner.iterative.RuleStats;
import io.trino.sql.planner.optimizations.OptimizerStats;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.MBeanExport;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.ObjectNames;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OptimizerStatsMBeanExporter
{
    @GuardedBy("this")
    private final List<MBeanExport> mbeanExports = new ArrayList<>();

    private final MBeanExporter exporter;
    private final Map<Class<?>, OptimizerStats> optimizerStats;
    private final Map<Class<?>, RuleStats> ruleStats;

    @Inject
    public OptimizerStatsMBeanExporter(MBeanExporter exporter, PlanOptimizersFactory optimizers)
    {
        requireNonNull(optimizers, "optimizers is null");
        optimizerStats = optimizers.getOptimizerStats();
        ruleStats = optimizers.getRuleStats();

        this.exporter = requireNonNull(exporter, "exporter is null");
    }

    @PostConstruct
    public synchronized void export()
    {
        checkState(mbeanExports.isEmpty(), "MBeans already exported");

        for (Map.Entry<Class<?>, OptimizerStats> entry : optimizerStats.entrySet()) {
            verify(!entry.getKey().getSimpleName().isEmpty());
            try {
                mbeanExports.add(exporter.exportWithGeneratedName(entry.getValue(), PlanOptimizer.class, ImmutableMap.<String, String>builder()
                        .put("name", PlanOptimizer.class.getSimpleName())
                        .put("optimizer", entry.getKey().getSimpleName())
                        .buildOrThrow()));
            }
            catch (RuntimeException e) {
                throw new RuntimeException(format("Failed to export MBean with name '%s'", getName(entry.getKey())), e);
            }
        }

        for (Map.Entry<Class<?>, RuleStats> entry : ruleStats.entrySet()) {
            verify(!entry.getKey().getSimpleName().isEmpty());
            try {
                mbeanExports.add(exporter.exportWithGeneratedName(entry.getValue(), IterativeOptimizer.class, ImmutableMap.<String, String>builder()
                        .put("name", IterativeOptimizer.class.getSimpleName())
                        .put("rule", entry.getKey().getSimpleName())
                        .buildOrThrow()));
            }
            catch (RuntimeException e) {
                throw new RuntimeException(format("Failed to export MBean with for rule '%s'", entry.getKey().getSimpleName()), e);
            }
        }
    }

    @PreDestroy
    public synchronized void unexport()
    {
        for (MBeanExport mbeanExport : mbeanExports) {
            mbeanExport.unexport();
        }
        mbeanExports.clear();
    }

    private String getName(Class<?> key)
    {
        return ObjectNames.builder(PlanOptimizer.class)
                .withProperty("optimizer", key.getSimpleName())
                .build();
    }
}
