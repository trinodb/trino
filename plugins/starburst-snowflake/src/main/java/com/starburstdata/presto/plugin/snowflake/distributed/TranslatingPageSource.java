/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake.distributed;

import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.LazyBlockLoader;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkState;
import static com.starburstdata.presto.plugin.snowflake.distributed.SnowflakeQueryBuilder.TIMESTAMP_WITH_TIME_ZONE_MILLIS_SHIFT;
import static com.starburstdata.presto.plugin.snowflake.distributed.SnowflakeQueryBuilder.TIMESTAMP_WITH_TIME_ZONE_ZONE_MASK;
import static com.starburstdata.presto.plugin.snowflake.distributed.SnowflakeQueryBuilder.ZONE_OFFSET_MINUTES_BIAS;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static java.util.Objects.requireNonNull;

/**
 * A page source used to convert various data types between their representation in
 * Snowflake's export and the corresponding Presto types.
 */
public class TranslatingPageSource
        implements ConnectorPageSource
{
    private static final TimeType TIME_TYPE = createTimeType(TimestampType.DEFAULT_PRECISION);

    private final ConnectorPageSource delegate;
    private final List<HiveColumnHandle> columns;
    private final TranslateToLong timestampWithTimeZoneTranslation;

    public TranslatingPageSource(ConnectorPageSource delegate, List<HiveColumnHandle> columns)
    {
        this.delegate = requireNonNull(delegate, "delegate was null");
        this.columns = requireNonNull(columns, "columns was null");
        timestampWithTimeZoneTranslation = new TimestampWithTimeZoneTranslation();
    }

    @Override
    public Page getNextPage()
    {
        Page originalPage = delegate.getNextPage();
        if (originalPage == null) {
            return null;
        }
        checkState(
                originalPage.getChannelCount() == columns.size(),
                "Number of blocks in page (%s) doesn't match number of columns (%s)",
                originalPage.getChannelCount(),
                columns.size());
        Block[] translatedBlocks = new Block[originalPage.getChannelCount()];
        for (int index = 0; index < translatedBlocks.length; index++) {
            Block originalBlock = originalPage.getBlock(index);
            Type type = columns.get(index).getType();
            if (type instanceof TimestampWithTimeZoneType) {
                translatedBlocks[index] = new LazyBlock(
                        originalBlock.getPositionCount(),
                        new TranslateToLongBlockLoader(
                                originalBlock,
                                timestampWithTimeZoneTranslation));
            }
            else if (type instanceof TimeType) {
                translatedBlocks[index] = new LazyBlock(
                        originalBlock.getPositionCount(),
                        new TranslateToLongBlockLoader(
                                originalBlock,
                                TranslatingPageSource::translateTime));
            }
            else {
                translatedBlocks[index] = originalBlock;
            }
        }
        return new Page(originalPage.getPositionCount(), translatedBlocks);
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public long getMemoryUsage()
    {
        return delegate.getMemoryUsage();
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return delegate.isBlocked();
    }

    private static final class TranslateToLongBlockLoader
            implements LazyBlockLoader
    {
        private Block block;
        private final TranslateToLong translation;

        public TranslateToLongBlockLoader(Block block, TranslateToLong translation)
        {
            this.block = requireNonNull(block, "block was null");
            this.translation = requireNonNull(translation, "translation was null");
        }

        @Override
        public Block load()
        {
            checkState(block != null, "Already loaded");

            long[] values = new long[block.getPositionCount()];
            boolean[] valueIsNull = new boolean[block.getPositionCount()];
            boolean checkNulls = block.mayHaveNull();
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (checkNulls) {
                    if (block.isNull(position)) {
                        valueIsNull[position] = true;
                    }
                    else {
                        valueIsNull[position] = false;
                        values[position] = translation.mapLong(translation.extractLong(block, position));
                    }
                }
                else {
                    values[position] = translation.mapLong(translation.extractLong(block, position));
                }
            }
            Block mapped;
            if (checkNulls) {
                mapped = new LongArrayBlock(block.getPositionCount(), Optional.of(valueIsNull), values);
            }
            else {
                mapped = new LongArrayBlock(block.getPositionCount(), Optional.empty(), values);
            }
            // clear reference to loader to free resources, since load was successful
            block = null;

            return mapped;
        }
    }

    // TODO: Remove mapLong entirely and make TimestampWithTimeZoneTranslation into a static method instead of a class
    private interface TranslateToLong
    {
        long extractLong(Block block, int position);

        default long mapLong(long value)
        {
            return value;
        }
    }

    private static long translateTime(Block block, int position)
    {
        return TIME_TYPE.getLong(block, position) * PICOSECONDS_PER_MILLISECOND;
    }

    private static class TimestampWithTimeZoneTranslation
            implements TranslateToLong
    {
        private static final DecimalType LONG_DECIMAL_TYPE = DecimalType.createDecimalType(19);

        @Override
        public long extractLong(Block block, int position)
        {
            BigInteger value = ((Int128) LONG_DECIMAL_TYPE.getObject(block, position)).toBigInteger();
            return value.longValue();
        }

        @Override
        public long mapLong(long value)
        {
            int offsetMinutes = (int) (value & TIMESTAMP_WITH_TIME_ZONE_ZONE_MASK) - ZONE_OFFSET_MINUTES_BIAS;
            return packDateTimeWithZone(value >> TIMESTAMP_WITH_TIME_ZONE_MILLIS_SHIFT, offsetMinutes);
        }
    }
}
