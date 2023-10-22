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
package io.trino.tests;

import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.AbstractLongType;
import io.trino.spi.type.TypeSignature;

public final class BogusType
        extends AbstractLongType
{
    public static final BogusType BOGUS = new BogusType();
    public static final String NAME = "Bogus";

    private BogusType()
    {
        super(new TypeSignature(NAME));
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        LongArrayBlock valueBlock = (LongArrayBlock) block.getUnderlyingValueBlock();
        int valueBlockPosition = block.getUnderlyingValuePosition(position);
        if (valueBlock.getLong(valueBlockPosition) != 0) {
            throw new RuntimeException("This is bogus exception");
        }

        return 0;
    }

    @Override
    public boolean isOrderable()
    {
        return false;
    }

    @Override
    public boolean isComparable()
    {
        return false;
    }
}
