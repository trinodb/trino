package io.trino.arrow.writer;

import io.trino.spi.block.Block;
import io.trino.spi.type.TimeType;
import org.apache.arrow.vector.TimeMicroVector;

import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;

public class TimeMicroColumnWriter extends FixedWidthColumnWriter<TimeMicroVector>
{
    private final TimeType type;

    public TimeMicroColumnWriter(TimeMicroVector vector, TimeType type)
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
        vector.set(position, type.getLong(block, position) / PICOSECONDS_PER_MICROSECOND);
    }
}
