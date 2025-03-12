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
package io.trino.plugin.faker;

import io.trino.spi.connector.ConnectorSplit;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;

public record FakerSplit(long splitNumber, long rowsOffset, long rowsCount)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(FakerSplit.class);

    public FakerSplit
    {
        checkArgument(splitNumber >= 0, "splitNumber is negative");
        checkArgument(rowsOffset >= 0, "rowsOffset is negative");
        checkArgument(rowsCount >= 0, "rowsCount is negative");
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }
}
