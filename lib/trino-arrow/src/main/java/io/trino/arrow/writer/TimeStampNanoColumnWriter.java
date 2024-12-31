package io.trino.arrow.writer;

import io.trino.spi.block.Block;
import io.trino.spi.type.TimestampType;
import org.apache.arrow.vector.TimeStampNanoVector;

import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;

public class TimeStampNanoColumnWriter extends FixedWidthColumnWriter<TimeStampNanoVector>
{
    private final TimestampType type;

    public TimeStampNanoColumnWriter(TimeStampNanoVector vector, TimestampType type)
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
        vector.set(position, type.getLong(block, position) / PICOSECONDS_PER_NANOSECOND);
    }
}
