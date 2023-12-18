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
import io.trino.spi.type.Type;
import net.snowflake.client.core.SFException;
import net.snowflake.client.core.arrow.ArrowVectorConverter;

import java.time.LocalDateTime;

import static com.starburstdata.trino.plugins.snowflake.jdbc.SnowflakeClient.SNOWFLAKE_TIMESTAMP_READ_FORMATTER;
import static io.trino.plugin.jdbc.StandardColumnMappings.toTrinoTimestamp;
import static io.trino.spi.type.TimestampType.createTimestampType;

public class ShortTimestampValueWriter
        implements BlockWriter
{
    private final ArrowVectorConverter converter;
    private final int rowCount;
    private final Type type;
    private final int precision;

    public ShortTimestampValueWriter(ArrowVectorConverter converter, int rowCount, Type type, int precision)
    {
        this.converter = converter;
        this.rowCount = rowCount;
        this.type = type;
        this.precision = precision;
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
                LocalDateTime localDateTime = LocalDateTime.parse(converter.toString(row), SNOWFLAKE_TIMESTAMP_READ_FORMATTER);
                long value = toTrinoTimestamp(createTimestampType(precision), localDateTime);
                type.writeLong(output, value);
            }
        }
    }
}
