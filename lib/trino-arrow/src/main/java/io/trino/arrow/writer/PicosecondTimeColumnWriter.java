package io.trino.arrow.writer;

import io.trino.arrow.ArrowColumnWriter;
import io.trino.arrow.type.PicosecondTimeVector;
import io.trino.arrow.type.PicosecondTimestampVector;
import io.trino.spi.block.Block;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.SqlTime;
import io.trino.spi.type.TimestampType;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.complex.writer.TimeMicroWriter;

import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;

public class PicosecondTimeColumnWriter implements ArrowColumnWriter {

    private final PicosecondTimeVector vector;
    private static final TimestampType TYPE = TimestampType.TIMESTAMP_PICOS;

    public PicosecondTimeColumnWriter(PicosecondTimeVector vector) {
        this.vector = vector;

    }
    @Override
    public void write(Block block) {
        BaseWriter.StructWriter structWriter = vector.getUnderlyingVector().getWriter();
        TimeMicroWriter timeMicroWriter = structWriter.timeMicro("time");
        IntWriter intWriter = structWriter.integer("pico_adjustment");
        for (int i = 0; i < block.getPositionCount(); i++) {
            SqlTime timestamp = (SqlTime) TYPE.getObject(block, i);
            if (block.isNull(i)) {
                structWriter.writeNull();
            }else {
                structWriter.start();
                long micros = timestamp.getPicos() / PICOSECONDS_PER_MICROSECOND;
                int picoAdjustment = Math.toIntExact(timestamp.getPicos() - micros);
                timeMicroWriter.writeTimeMicro(micros);
                intWriter.writeInt(picoAdjustment);
            }
            structWriter.end();
        }

    }
}
