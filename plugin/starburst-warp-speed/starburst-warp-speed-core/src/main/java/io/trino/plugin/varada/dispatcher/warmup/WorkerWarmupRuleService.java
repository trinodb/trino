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
package io.trino.plugin.varada.dispatcher.warmup;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.varada.dispatcher.warmup.fetcher.WarmupRuleFetcher;
import io.trino.plugin.varada.warmup.model.WarmupRule;
import io.trino.spi.connector.SchemaTableName;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@Singleton
public class WorkerWarmupRuleService
{
    private final WarmupRuleFetcher warmupRuleFetcher;
    private final ReadWriteLock readWriteLock;
    private volatile Map<SchemaTableName, List<WarmupRule>> warmupRuleByTable;

    @Inject
    public WorkerWarmupRuleService(WarmupRuleFetcher warmupRuleFetcher)
    {
        this.warmupRuleFetcher = requireNonNull(warmupRuleFetcher);
        this.readWriteLock = new ReentrantReadWriteLock();
    }

    public List<WarmupRule> fetchRulesFromCoordinator()
    {
        List<WarmupRule> warmupRules = warmupRuleFetcher.getWarmupRules();
        Map<SchemaTableName, List<WarmupRule>> newWarmupRuleByTable = warmupRules
                .stream().collect(Collectors.groupingBy(warmupRule -> new SchemaTableName(warmupRule.getSchema(), warmupRule.getTable())));
        updateWarmupRuleByTable(newWarmupRuleByTable);
        return warmupRules;
    }

    private void updateWarmupRuleByTable(Map<SchemaTableName, List<WarmupRule>> newWarmupRuleByTable)
    {
        Lock lock = readWriteLock.writeLock();
        lock.lock();
        try {
            this.warmupRuleByTable = newWarmupRuleByTable;
        }
        finally {
            lock.unlock();
        }
    }

    List<WarmupRule> getWarmupRules(SchemaTableName schemaTableName)
    {
        if (warmupRuleByTable == null) {
            synchronized (this) {
                if (warmupRuleByTable == null) {
                    fetchRulesFromCoordinator();
                }
            }
        }

        Lock lock = readWriteLock.readLock();
        lock.lock();
        try {
            return warmupRuleByTable.getOrDefault(schemaTableName, Collections.emptyList());
        }
        finally {
            lock.unlock();
        }
    }
}
