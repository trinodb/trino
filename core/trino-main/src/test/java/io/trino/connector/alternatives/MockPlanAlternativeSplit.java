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
package io.trino.connector.alternatives;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.StringJoiner;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class MockPlanAlternativeSplit
        implements ConnectorSplit
{
    private final ConnectorSplit delegate;
    private final int splitNumber;

    @JsonCreator
    public MockPlanAlternativeSplit(@JsonProperty("delegate") ConnectorSplit delegate, @JsonProperty("splitNumber") int splitNumber)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        checkArgument(splitNumber >= 0 && splitNumber < Integer.MAX_VALUE);
        this.splitNumber = splitNumber;
    }

    @JsonProperty
    public ConnectorSplit getDelegate()
    {
        return delegate;
    }

    @JsonProperty
    public int getSplitNumber()
    {
        return splitNumber;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return delegate.isRemotelyAccessible();
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return delegate.getAddresses();
    }

    @Override
    public Object getInfo()
    {
        return delegate.getInfo();
    }

    @Override
    public SplitWeight getSplitWeight()
    {
        return delegate.getSplitWeight();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return delegate.getRetainedSizeInBytes();
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", MockPlanAlternativeSplit.class.getSimpleName() + "[", "]")
                .add("splitNumber=" + splitNumber)
                .add("delegate=" + delegate)
                .toString();
    }
}
