package io.trino.arrow.writer;

import io.trino.spi.block.Block;
import io.trino.spi.type.TimestampType;
import org.apache.arrow.vector.TimeStampMilliVector;

import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;

public class TimeStampMilliColumnWriter extends FixedWidthColumnWriter<TimeStampMilliVector>
{
    private final TimestampType type;

    public TimeStampMilliColumnWriter(TimeStampMilliVector vector, TimestampType type)
    {
        super(vector);
        this.type = type;
    }

    @Override
    protected void writeNull(int position)
    {
        vector.setNull(position);
    }

    @Override
    protected void writeValue(Block block, int position)
    {
        vector.set(position, type.getLong(block, position) / PICOSECONDS_PER_MILLISECOND);
    }
}
