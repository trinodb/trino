package io.trino.arrow.writer;

import io.trino.spi.block.Block;
import io.trino.spi.type.TimeType;
import org.apache.arrow.vector.TimeSecVector;

import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_SECOND;

public class TimeSecColumnWriter extends PrimitiveColumnWriter<TimeSecVector>
{
    private final TimeType type;

    public TimeSecColumnWriter(TimeSecVector vector, TimeType type)
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
        vector.set(position, (int) (type.getLong(block, position) / PICOSECONDS_PER_SECOND));
    }
}
