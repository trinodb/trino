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
package io.trino.plugin.pinot.query.ptf.context;

import org.apache.pinot.common.request.context.LiteralContext;
import org.apache.pinot.spi.data.FieldSpec.DataType;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record SerializableLiteralContext(DataType dataType, Optional<Object> value)
{
    public SerializableLiteralContext
    {
        requireNonNull(dataType, "dataType is null");
        requireNonNull(value, "value is null");
    }

    public SerializableLiteralContext(LiteralContext literalContext)
    {
        this(literalContext.getType(), Optional.ofNullable(literalContext.getValue()));
    }

    public LiteralContext toLiteralContext()
    {
        return new LiteralContext(dataType, value.get());
    }
}
