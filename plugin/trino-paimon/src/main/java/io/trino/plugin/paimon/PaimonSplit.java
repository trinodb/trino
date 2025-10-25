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
package io.trino.plugin.paimon;

import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;
import org.apache.paimon.table.source.Split;

import java.util.Collections;
import java.util.List;

import static io.trino.plugin.paimon.EncodingUtils.decodeStringToObject;
import static io.trino.plugin.paimon.EncodingUtils.encodeObjectToString;
import static java.util.Objects.requireNonNull;

public record PaimonSplit(String splitSerialized, Double weight)
        implements ConnectorSplit
{
    public PaimonSplit
    {
        requireNonNull(splitSerialized, "splitSerialized is null");
        requireNonNull(weight, "weight is null");
    }

    public static PaimonSplit fromSplit(Split split, Double weight)
    {
        return new PaimonSplit(encodeObjectToString(split), weight);
    }

    public Split decodeSplit()
    {
        return decodeStringToObject(splitSerialized);
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return Collections.emptyList();
    }

    @Override
    public SplitWeight getSplitWeight()
    {
        return SplitWeight.fromProportion(weight);
    }
}
