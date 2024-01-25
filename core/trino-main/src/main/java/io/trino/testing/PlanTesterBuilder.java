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
package io.trino.testing;

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.trino.Session;
import io.trino.cache.CacheConfig;
import io.trino.metadata.Metadata;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

// Note: the class name is off, but the built class is going to be renamed soon
public class PlanTesterBuilder
{
    public static PlanTesterBuilder planTesterBuilder(Session defaultSession)
    {
        return new PlanTesterBuilder(defaultSession);
    }

    private final Session defaultSession;
    private CacheConfig cacheConfig = new CacheConfig();
    private Function<Metadata, Metadata> metadataDecorator = identity();
    private int nodeCountForStats = 1;

    private PlanTesterBuilder(Session defaultSession)
    {
        this.defaultSession = requireNonNull(defaultSession, "defaultSession is null");
    }

    @CanIgnoreReturnValue
    public PlanTesterBuilder withCacheConfig(CacheConfig cacheConfig)
    {
        this.cacheConfig = requireNonNull(cacheConfig, "cacheConfig is null");
        return this;
    }

    @CanIgnoreReturnValue
    public PlanTesterBuilder withMetadataDecorator(Function<Metadata, Metadata> metadataDecorator)
    {
        this.metadataDecorator = requireNonNull(metadataDecorator, "metadataDecorator is null");
        return this;
    }

    @CanIgnoreReturnValue
    public PlanTesterBuilder withNodeCountForStats(int nodeCountForStats)
    {
        this.nodeCountForStats = nodeCountForStats;
        return this;
    }

    public LocalQueryRunner build()
    {
        return LocalQueryRunner.create(defaultSession, cacheConfig, metadataDecorator, nodeCountForStats);
    }
}
