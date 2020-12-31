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
package io.trino.spi.block;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;

import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.String.format;

public class DuplicateMapKeyException
        extends TrinoException
{
    private final Block block;
    private final int position;
    private final boolean hasDetailedMessage;

    public DuplicateMapKeyException(Block block, int position)
    {
        this(block, position, Optional.empty());
    }

    private DuplicateMapKeyException(Block block, int position, Optional<String> detailedMessage)
    {
        super(INVALID_FUNCTION_ARGUMENT, detailedMessage.orElse("Duplicate map keys are not allowed"));
        this.block = block;
        this.position = position;
        this.hasDetailedMessage = detailedMessage.isPresent();
    }

    public DuplicateMapKeyException withDetailedMessage(Type keyType, ConnectorSession session)
    {
        if (hasDetailedMessage) {
            return this;
        }
        String detailedMessage = format("Duplicate map keys (%s) are not allowed", keyType.getObjectValue(session, block, position));
        return new DuplicateMapKeyException(block, position, Optional.of(detailedMessage));
    }
}
