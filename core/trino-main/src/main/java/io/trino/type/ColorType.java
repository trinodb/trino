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
package io.trino.type;

import io.trino.operator.scalar.ColorFunctions;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.AbstractIntType;
import io.trino.spi.type.TypeSignature;

import java.util.HexFormat;

public class ColorType
        extends AbstractIntType
{
    private static final HexFormat HEX_FORMAT = HexFormat.of();
    public static final ColorType COLOR = new ColorType();
    public static final String NAME = "color";

    private ColorType()
    {
        super(new TypeSignature(NAME));
    }

    @Override
    public boolean isOrderable()
    {
        return false;
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        int color = block.getInt(position, 0);
        if (color < 0) {
            return ColorFunctions.SystemColor.valueOf(-(color + 1)).getName();
        }

        StringBuilder builder = new StringBuilder(7).append('#');
        HEX_FORMAT.toHexDigits(builder, (byte) ((color >> 16) & 0xFF));
        HEX_FORMAT.toHexDigits(builder, (byte) ((color >> 8) & 0xFF));
        HEX_FORMAT.toHexDigits(builder, (byte) (color & 0xFF));
        return builder.toString();
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object other)
    {
        return other == COLOR;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }
}
