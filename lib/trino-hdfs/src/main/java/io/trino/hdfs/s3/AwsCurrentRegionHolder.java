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
package io.trino.hdfs.s3;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.google.common.base.Suppliers;

import java.util.function.Supplier;

/**
 * Caches the result of calling {@link Regions#getCurrentRegion()} since accessing EC2 instance
 * metadata repeatedly can result in being throttled and prevent other metadata accessing operations
 * such as refreshing instance credentials from working normally
 */
public final class AwsCurrentRegionHolder
{
    private static final Supplier<Region> SUPPLIER = Suppliers.memoize(AwsCurrentRegionHolder::loadCurrentRegionOrThrowOnNull);

    private AwsCurrentRegionHolder() {}

    /**
     * Attempts to resolve the current region from EC2's instance metadata through {@link Regions#getCurrentRegion()}. If
     * no region is able to be resolved an exception is thrown
     */
    public static Region getCurrentRegionFromEC2Metadata()
            throws IllegalStateException
    {
        return SUPPLIER.get();
    }

    /**
     * @throws IllegalStateException when no region is resolved to avoid memoizing a transient failure
     */
    private static Region loadCurrentRegionOrThrowOnNull()
            throws IllegalStateException
    {
        Region result = Regions.getCurrentRegion();
        if (result == null) {
            throw new IllegalStateException("Failed to resolve current AWS region from EC2 metadata");
        }
        return result;
    }
}
