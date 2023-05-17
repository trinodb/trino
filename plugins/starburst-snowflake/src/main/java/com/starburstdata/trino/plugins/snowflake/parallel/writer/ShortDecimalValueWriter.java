/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.snowflake.parallel.writer;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.arrow.ArrowVectorConverter;

import static java.math.RoundingMode.UNNECESSARY;

public class ShortDecimalValueWriter
        implements BlockWriter
{
    private final ArrowVectorConverter converter;
    private final int rowCount;
    private final DecimalType decimalType;
    private final Type type;

    public ShortDecimalValueWriter(ArrowVectorConverter converter, int rowCount, DecimalType decimalType, Type type)
    {
        this.converter = converter;
        this.rowCount = rowCount;
        this.decimalType = decimalType;
        this.type = type;
    }

    @Override
    public void write(BlockBuilder output)
            throws SFException
    {
        for (int row = 0; row < rowCount; row++) {
            if (converter.isNull(row)) {
                output.appendNull();
            }
            else {
                type.writeLong(output, Decimals.encodeShortScaledValue(converter.toBigDecimal(row), decimalType.getScale(), UNNECESSARY));
            }
        }
    }
}
