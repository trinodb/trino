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

package io.trino.sql.planner.iterative;

import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import com.google.common.reflect.TypeToken;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.matching.Pattern;
import io.trino.matching.pattern.TypeOfPattern;

import java.util.Set;
import java.util.concurrent.ExecutionException;

public class RuleIndex
{
    private final ListMultimap<Class<?>, Rule<?>> rulesByRootType;
    private final Cache<Class<?>, Set<Rule<?>>> rulesByClass;

    private RuleIndex(ListMultimap<Class<?>, Rule<?>> rulesByRootType)
    {
        this.rulesByRootType = ImmutableListMultimap.copyOf(rulesByRootType);
        this.rulesByClass = EvictableCacheBuilder.newBuilder()
                .maximumSize(128) // we have a limited number of node types, so this is more than enough
                .build();
    }

    public Set<Rule<?>> getCandidates(Object object)
    {
        try {
            return rulesByClass.get(object.getClass(), () -> computeCandidates(object.getClass()));
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public Set<Rule<?>> computeCandidates(Class<?> key)
    {
        ImmutableSet.Builder<Rule<?>> builder = ImmutableSet.builder();
        TypeToken.of(key).getTypes().forEach(clazz -> {
            Class<?> rawType = clazz.getRawType();
            if (rulesByRootType.containsKey(rawType)) {
                builder.addAll(rulesByRootType.get(rawType));
            }
        });
        return builder.build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private final ImmutableListMultimap.Builder<Class<?>, Rule<?>> rulesByRootType = ImmutableListMultimap.builder();

        public Builder register(Set<Rule<?>> rules)
        {
            rules.forEach(this::register);
            return this;
        }

        public Builder register(Rule<?> rule)
        {
            Pattern<?> pattern = getFirstPattern(rule.getPattern());
            if (!(pattern instanceof TypeOfPattern<?> typeOfPattern)) {
                throw new IllegalArgumentException("Unexpected Pattern: " + pattern);
            }
            rulesByRootType.put(typeOfPattern.expectedClass(), rule);
            return this;
        }

        private Pattern<?> getFirstPattern(Pattern<?> pattern)
        {
            while (pattern.previous().isPresent()) {
                pattern = pattern.previous().get();
            }
            return pattern;
        }

        public RuleIndex build()
        {
            return new RuleIndex(rulesByRootType.build());
        }
    }
}
