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

import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.LazyBlock;
import io.prestosql.spi.block.LazyBlockLoader;
import io.prestosql.spi.block.LongArrayBlock;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;

import java.io.IOException;
import java.math.BigInteger;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.starburstdata.presto.plugin.snowflake.distributed.LegacyDateTimeConversionUtils.toPrestoLegacyTime;
import static com.starburstdata.presto.plugin.snowflake.distributed.LegacyDateTimeConversionUtils.toPrestoLegacyTimestamp;
import static com.starburstdata.presto.plugin.snowflake.distributed.SnowflakeQueryBuilder.TIMESTAMP_WITH_TIME_ZONE_MILLIS_SHIFT;
import static com.starburstdata.presto.plugin.snowflake.distributed.SnowflakeQueryBuilder.TIMESTAMP_WITH_TIME_ZONE_ZONE_MASK;
import static com.starburstdata.presto.plugin.snowflake.distributed.SnowflakeQueryBuilder.ZONE_OFFSET_MINUTES_BIAS;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.TimeType.TIME;
import static java.util.Objects.requireNonNull;

/**
 * A page source used to convert various data types between their representation in
 * Snowflake's export and the corresponding Presto types.
 */
public class TranslatingPageSource
        implements ConnectorPageSource
{
    private final ConnectorPageSource delegate;
    private final List<HiveColumnHandle> columns;
    private final ConnectorSession session;
    private final TranslateToLong legacyTimestampTranslation;
    private final TranslateToLong legacyTimeTranslation;
    private final TranslateToLong timestampWithTimeZoneTranslation;

    public TranslatingPageSource(ConnectorPageSource delegate, List<HiveColumnHandle> columns, ConnectorSession session)
    {
        this.delegate = requireNonNull(delegate, "delegate was null");
        this.columns = requireNonNull(columns, "columns was null");
        this.session = requireNonNull(session, "session was null");
        legacyTimestampTranslation = new LegacyTimestampTranslation();
        legacyTimeTranslation = new LegacyTimeTranslation();
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
            if (TIME.equals(type) && session.isLegacyTimestamp()) {
                translatedBlocks[index] = new LazyBlock(
                        originalBlock.getPositionCount(),
                        new TranslateToLongBlockLoader(
                                originalBlock,
                                legacyTimeTranslation));
            }
            else if (type instanceof TimestampType && session.isLegacyTimestamp()) {
                verify(((TimestampType) type).getPrecision() == 3, "Unsupported TimestampType: %s", type);
                translatedBlocks[index] = new LazyBlock(
                        originalBlock.getPositionCount(),
                        new TranslateToLongBlockLoader(
                                originalBlock,
                                legacyTimestampTranslation));
            }
            else if (type instanceof TimestampWithTimeZoneType) {
                translatedBlocks[index] = new LazyBlock(
                        originalBlock.getPositionCount(),
                        new TranslateToLongBlockLoader(
                                originalBlock,
                                timestampWithTimeZoneTranslation));
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
    public long getSystemMemoryUsage()
    {
        return delegate.getSystemMemoryUsage();
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

    interface TranslateToLong
    {
        long extractLong(Block block, int position);

        long mapLong(long value);
    }

    private class LegacyTimeTranslation
            implements TranslateToLong
    {
        @Override
        public long extractLong(Block block, int position)
        {
            return block.getLong(position, 0);
        }

        @Override
        public long mapLong(long value)
        {
            return toPrestoLegacyTime(value, ZoneId.of(session.getTimeZoneKey().getId()));
        }
    }

    private class LegacyTimestampTranslation
            implements TranslateToLong
    {
        @Override
        public long extractLong(Block block, int position)
        {
            return block.getLong(position, 0);
        }

        @Override
        public long mapLong(long value)
        {
            return toPrestoLegacyTimestamp(value, ZoneId.of(session.getTimeZoneKey().getId()));
        }
    }

    private static class TimestampWithTimeZoneTranslation
            implements TranslateToLong
    {
        private static final DecimalType LONG_DECIMAL_TYPE = DecimalType.createDecimalType(19);

        @Override
        public long extractLong(Block block, int position)
        {
            BigInteger value = Decimals.decodeUnscaledValue(LONG_DECIMAL_TYPE.getSlice(block, position));
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
