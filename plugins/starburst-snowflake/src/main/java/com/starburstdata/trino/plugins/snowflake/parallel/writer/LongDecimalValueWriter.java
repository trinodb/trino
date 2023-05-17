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
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.arrow.ArrowVectorConverter;

import static java.lang.Math.max;
import static java.math.RoundingMode.UNNECESSARY;

public class LongDecimalValueWriter
        implements BlockWriter
{
    private final ArrowVectorConverter converter;
    private final int rowCount;
    private final int decimalDigits;
    private final Type type;

    public LongDecimalValueWriter(ArrowVectorConverter converter, int rowCount, int decimalDigits, Type type)
    {
        this.converter = converter;
        this.rowCount = rowCount;
        this.decimalDigits = decimalDigits;
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
                type.writeObject(output, Decimals.valueOf(converter.toBigDecimal(row).setScale(max(decimalDigits, 0), UNNECESSARY)));
            }
        }
    }
}
