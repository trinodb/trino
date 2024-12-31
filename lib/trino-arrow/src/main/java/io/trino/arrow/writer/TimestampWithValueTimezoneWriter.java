package io.trino.arrow.writer;

import io.trino.arrow.ArrowColumnWriter;
import io.trino.arrow.type.PicosecondTimestampVector;
import io.trino.arrow.type.TimestampWithValueTimezoneVector;
import io.trino.spi.block.Block;
import io.trino.spi.type.*;
import org.apache.arrow.vector.complex.writer.*;

import static java.util.Objects.requireNonNull;

public class TimestampWithValueTimezoneWriter implements ArrowColumnWriter {

    private final TimestampWithValueTimezoneVector vector;
    private final Type type;

    public TimestampWithValueTimezoneWriter(TimestampWithValueTimezoneVector vector, Type type) {
        this.vector = vector;
        this.type = type;

    }
    @Override
    public void write(Block block) {
        BaseWriter.StructWriter structWriter = vector.getUnderlyingVector().getWriter();
        TimeStampMilliWriter timeStampMilliWriter = structWriter.timeStampMilli("timestamp");
        IntWriter adjustmentWriter = structWriter.integer("pico_adjustment"); //TODO confirm this is null if not present
        SmallIntWriter zoneWriter = structWriter.smallInt("zone_id");
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                structWriter.writeNull();
            }else {
                structWriter.start();
                long epochMillis;
                short zoneId;
                Object value = type.getObject(block, i);
                switch(value){
                    case LongTimestampWithTimeZone timestamp -> {
                        epochMillis = timestamp.getEpochMillis();
                        zoneId = timestamp.getTimeZoneKey();
                        requireNonNull(adjustmentWriter, "adjustmentWriter is null");
                        adjustmentWriter.writeInt(timestamp.getPicosOfMilli());
                    }
                    case SqlTimestampWithTimeZone timestamp -> {
                        epochMillis = timestamp.getEpochMillis();
                        zoneId = timestamp.getTimeZoneKey().getKey();
                    }
                    default -> throw new IllegalArgumentException("Unexpected value type: " + value.getClass());
                }
                timeStampMilliWriter.writeTimeStampMilli(epochMillis);
                zoneWriter.writeSmallInt(zoneId);
            }
            structWriter.end();
        }

    }
}
