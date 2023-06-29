package io.trino.plugin.truera.metrics;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.AccumulatorStateSerializer;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import java.util.ArrayList;
import java.util.List;

public class AUCAccumulatorStateSerializer implements AccumulatorStateSerializer<AUCAccumulatorState> {
    private static final ArrayType DOUBLE_ARRAY_TYPE = new ArrayType(DoubleType.DOUBLE);
    private static final DoubleType DOUBLE_TYPE = DoubleType.DOUBLE;

    @Override
    public Type getSerializedType() {
        // Return the type that represents the serialized state
        return DOUBLE_ARRAY_TYPE;
    }

    @Override
    public void serialize(AUCAccumulatorState state, BlockBuilder out)
    {
        if (state.getActuals().size() == 0 || state.getPredictions().size() == 0) {
            out.appendNull();
            return;
        }

        BlockBuilder arrayBuilder = DOUBLE_TYPE.createBlockBuilder(null, state.getActuals().size() * 2);
        for (int i = 0; i < state.getActuals().size(); i++) {
            DoubleType.DOUBLE.writeDouble(arrayBuilder, state.getActuals().get(i));
            DoubleType.DOUBLE.writeDouble(arrayBuilder, state.getPredictions().get(i));
        }
        DOUBLE_ARRAY_TYPE.writeObject(out, arrayBuilder.build());
    }

    @Override
    public void deserialize(Block block, int index, AUCAccumulatorState state)
    {
        if (block.isNull(index)) {
            state.setActuals(null);
            state.setPredictions(null);
            return;
        }

        ArrayType arrayType = new ArrayType(new ArrayType(DOUBLE_TYPE));
        Block arrayBlock = arrayType.getObject(block, index);
        List<Double> actuals = new ArrayList<>();
        List<Double> predictions = new ArrayList<>();
        for (int i = 0; i < arrayBlock.getPositionCount(); i=i+2) {
            actuals.add(DOUBLE_TYPE.getDouble(arrayBlock, i));
            predictions.add(DOUBLE_TYPE.getDouble(arrayBlock, i+1));
        }
        state.setActuals(actuals);
        state.setPredictions(predictions);
    }
}

