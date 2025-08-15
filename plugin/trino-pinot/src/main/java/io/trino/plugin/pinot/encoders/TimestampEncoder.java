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
package io.trino.plugin.pinot.encoders;

import io.trino.spi.block.Block;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.TimestampType;

import static java.util.Objects.requireNonNull;

public class TimestampEncoder
        extends AbstractEncoder
{
    private final TimestampType timestampType;

    public TimestampEncoder(TimestampType timestampType)
    {
        this.timestampType = requireNonNull(timestampType, "timestampType is null");
    }

    @Override
    protected Object encodeNonNull(Block block, int position)
    {
        SqlTimestamp sqlTimestamp = (SqlTimestamp) timestampType.getObjectValue(null, block, position);
        return sqlTimestamp.getMillis();
    }
}
