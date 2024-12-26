package io.trino.arrow.writer;

import io.trino.spi.block.Block;
import io.trino.spi.type.TimestampType;
import org.apache.arrow.vector.TimeStampSecVector;

import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_SECOND;

public class TimeStampSecColumnWriter extends PrimitiveColumnWriter<TimeStampSecVector>
{
    private final TimestampType type;

    public TimeStampSecColumnWriter(TimeStampSecVector vector, TimestampType type)
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
        vector.set(position, type.getLong(block, position) / PICOSECONDS_PER_SECOND);
    }
}
