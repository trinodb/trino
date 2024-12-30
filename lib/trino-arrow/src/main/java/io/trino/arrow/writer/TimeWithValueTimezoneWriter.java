package io.trino.arrow.writer;

import io.trino.arrow.ArrowColumnWriter;
import io.trino.arrow.type.TimeWithValueTimezoneVector;
import io.trino.spi.block.Block;
import io.trino.spi.type.*;
import org.apache.arrow.vector.complex.writer.*;

import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static java.util.Objects.requireNonNull;

public class TimeWithValueTimezoneWriter implements ArrowColumnWriter {

    private final TimeWithValueTimezoneVector vector;
    private final Type type;

    public TimeWithValueTimezoneWriter(TimeWithValueTimezoneVector vector, Type type) {
        this.vector = vector;
        this.type = type;

    }
    @Override
    public void write(Block block) {
        BaseWriter.StructWriter structWriter = vector.getUnderlyingVector().getWriter();
        TimeMilliWriter timeMilliWriter = structWriter.timeMilli("time");
        IntWriter adjustmentWriter = structWriter.integer("pico_adjustment"); //TODO confirm this is null if not present
        IntWriter offsetWriter = structWriter.integer("offset_minutes");
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                structWriter.writeNull();
            }else {
                structWriter.start();
                int millis;
                int offsetMinutes;
                Object value = type.getObject(block, i);
                switch(value){
                    case LongTimeWithTimeZone time -> {
                        millis = Math.toIntExact(time.getPicoseconds() / PICOSECONDS_PER_MILLISECOND);
                        offsetMinutes = time.getOffsetMinutes();
                        requireNonNull(adjustmentWriter, "adjustmentWriter is null");
                        adjustmentWriter.writeInt(Math.toIntExact(time.getPicoseconds() - (millis * PICOSECONDS_PER_MILLISECOND)));
                    }
                    case SqlTimeWithTimeZone time -> {
                        millis = Math.toIntExact(time.getPicos() / PICOSECONDS_PER_MILLISECOND);
                        offsetMinutes = time.getOffsetMinutes();
                    }
                    default -> throw new IllegalArgumentException("Unexpected value type: " + value.getClass());
                }
                timeMilliWriter.writeTimeMilli(millis);
                offsetWriter.writeInt(offsetMinutes);
            }
            structWriter.end();
        }

    }
}
