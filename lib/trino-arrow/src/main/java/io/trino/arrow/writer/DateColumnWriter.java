package io.trino.arrow.writer;

import io.trino.spi.block.Block;
import io.trino.spi.type.DateType;
import org.apache.arrow.vector.DateDayVector;

public class DateColumnWriter extends FixedWidthColumnWriter<DateDayVector>
{
    private final DateType type = DateType.DATE;

    public DateColumnWriter(DateDayVector vector) {
        super(vector);
    }
    @Override
    protected void writeNull(int position)
    {
        vector.setNull(position);
    }

    @Override
    protected void writeValue(Block block, int position)
    {
        vector.set(position, type.getInt(block, position));
    }
}
