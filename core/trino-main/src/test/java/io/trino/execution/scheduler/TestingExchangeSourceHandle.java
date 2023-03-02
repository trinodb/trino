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

import io.trino.spi.exchange.ExchangeSourceHandle;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.instanceSize;

public class TestingExchangeSourceHandle
        implements ExchangeSourceHandle
{
    private static final long INSTANCE_SIZE = instanceSize(TestingExchangeSourceHandle.class);

    private final int id;
    private final int partitionId;
    private final long sizeInBytes;

    public TestingExchangeSourceHandle(int id, int partitionId, long sizeInBytes)
    {
        this.id = id;
        this.partitionId = partitionId;
        this.sizeInBytes = sizeInBytes;
    }

    public int getId()
    {
        return id;
    }

    @Override
    public int getPartitionId()
    {
        return partitionId;
    }

    @Override
    public long getDataSizeInBytes()
    {
        return sizeInBytes;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TestingExchangeSourceHandle that = (TestingExchangeSourceHandle) o;
        return id == that.id && partitionId == that.partitionId && sizeInBytes == that.sizeInBytes;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, partitionId, sizeInBytes);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .add("partitionId", partitionId)
                .add("sizeInBytes", sizeInBytes)
                .toString();
    }
}
