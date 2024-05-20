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
package io.trino.plugin.hive.metastore.glue.v1;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.google.common.base.Suppliers;

import java.util.function.Supplier;

/**
 * Caches the result of calling {@link Regions#getCurrentRegion()} since accessing EC2 instance
 * metadata repeatedly can result in being throttled and prevent other metadata accessing operations
 * such as refreshing instance credentials from working normally
 */
final class AwsCurrentRegionHolder
{
    private static final Supplier<Region> SUPPLIER = Suppliers.memoize(AwsCurrentRegionHolder::loadCurrentRegion);

    private AwsCurrentRegionHolder() {}

    /**
     * Attempts to resolve the current region from EC2's instance metadata through {@link Regions#getCurrentRegion()}.
     * An exception is thrown if the region cannot be resolved.
     */
    public static Region getCurrentRegionFromEc2Metadata()
    {
        return SUPPLIER.get();
    }

    private static Region loadCurrentRegion()
    {
        Region result = Regions.getCurrentRegion();
        if (result == null) {
            throw new IllegalStateException("Failed to resolve current AWS region from EC2 metadata");
        }
        return result;
    }
}
