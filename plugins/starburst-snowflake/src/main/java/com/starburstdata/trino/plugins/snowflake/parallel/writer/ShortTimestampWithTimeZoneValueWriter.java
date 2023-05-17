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

import java.time.ZonedDateTime;

import static com.starburstdata.trino.plugins.snowflake.jdbc.SnowflakeClient.SNOWFLAKE_DATE_TIME_FORMATTER;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;

public class ShortTimestampWithTimeZoneValueWriter
        implements BlockWriter
{
    private final ArrowVectorConverter converter;
    private final int rowCount;
    private final Type type;

    public ShortTimestampWithTimeZoneValueWriter(ArrowVectorConverter converter, int rowCount, Type type)
    {
        this.converter = converter;
        this.rowCount = rowCount;
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
                ZonedDateTime timestamp = SNOWFLAKE_DATE_TIME_FORMATTER.parse(converter.toString(row), ZonedDateTime::from);
                long value = packDateTimeWithZone(
                        timestamp.toInstant().toEpochMilli(),
                        timestamp.getZone().getId());
                type.writeLong(output, value);
            }
        }
    }
}
