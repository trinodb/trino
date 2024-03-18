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
package io.trino.plugin.varada.warmup.dal;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.varada.warmup.model.WarmupRule;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Singleton
public class WarmupRuleDao
{
    private final Map<Integer, WarmupRule> cache;

    private static final AtomicInteger idGen = new AtomicInteger(1);

    @Inject
    public WarmupRuleDao()
    {
        cache = new ConcurrentHashMap<>();
    }

    public List<WarmupRule> getAll()
    {
        return ImmutableList.copyOf(cache.values());
    }

    public List<WarmupRule> replaceAll(List<WarmupRule> newWarmupRules)
    {
        cache.clear();
        save(newWarmupRules);
        return getAll();
    }

    public void delete(List<Integer> ids)
    {
        ids.forEach(cache::remove);
    }

    public Collection<WarmupRule> save(Collection<WarmupRule> entities)
    {
        entities.forEach(warmupRule -> {
            if (warmupRule.getId() == 0) {
                warmupRule = WarmupRule.builder(warmupRule).id(idGen.getAndIncrement()).build();
            }
            cache.put(warmupRule.getId(), warmupRule);
        });
        return getAll();
    }
}
