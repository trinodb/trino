package io.trino.arrow.writer;

import io.trino.arrow.ArrowColumnWriter;
import io.trino.arrow.type.PicosecondTimestampVector;
import io.trino.spi.block.Block;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import org.apache.arrow.vector.complex.writer.BaseWriter;
import org.apache.arrow.vector.complex.writer.IntWriter;
import org.apache.arrow.vector.complex.writer.TimeMicroWriter;

public class PicosecondTimestampColumnWriter implements ArrowColumnWriter {

    private final PicosecondTimestampVector vector;
    private static final TimestampType TYPE = TimestampType.TIMESTAMP_PICOS;

    public PicosecondTimestampColumnWriter(PicosecondTimestampVector vector) {
        this.vector = vector;

    }
    @Override
    public void write(Block block) {
        BaseWriter.StructWriter structWriter = vector.getUnderlyingVector().getWriter();
        TimeMicroWriter timeMicroWriter = structWriter.timeMicro("timestamp");
        IntWriter intWriter = structWriter.integer("pico_adjustment");
        for (int i = 0; i < block.getPositionCount(); i++) {
            LongTimestamp timestamp = (LongTimestamp) TYPE.getObject(block, i);
            if (block.isNull(i)) {
                structWriter.writeNull();
            }else {
                structWriter.start();
                timeMicroWriter.writeTimeMicro(timestamp.getEpochMicros());
                intWriter.writeInt(timestamp.getPicosOfMicro());
            }
            structWriter.end();
        }

    }
}
