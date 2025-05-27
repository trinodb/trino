package io.trino.plugin.datasketches.hll;

import io.trino.spi.Plugin;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.memory.Memory;

import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Arrays.stream;

public class DataSketchesHllPlugin
        implements Plugin
{
    @Override
    public Set<Class<?>> getFunctions()
    {
        return stream(DataSketchesHllFunctions.class.getDeclaredClasses())
                .filter(clazz -> clazz.isAnnotationPresent(ScalarFunction.class))
                .collect(toImmutableSet());
    }

    public static class DataSketchesHllFunctions
    {
        @ScalarFunction("hll_estimate")
        @SqlType(StandardTypes.DOUBLE)
        public static double hllEstimate(@SqlType(StandardTypes.VARBINARY) byte[] sketch)
        {
            HllSketch hllSketch = HllSketch.heapify(Memory.wrap(sketch));
            return hllSketch.getEstimate();
        }

        @ScalarFunction("hll_union")
        @SqlType(StandardTypes.VARBINARY)
        public static byte[] hllUnion(@SqlType(StandardTypes.VARBINARY) byte[] sketch1, @SqlType(StandardTypes.VARBINARY) byte[] sketch2)
        {
            HllSketch union = HllSketch.heapify(Memory.wrap(sketch1));
            union.union(HllSketch.heapify(Memory.wrap(sketch2)));
            return union.toCompactByteArray();
        }
    }
} 