package io.trino.plugin.datasketches.hll;

import io.trino.spi.Plugin;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.hll.HllSketchFactory;

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
        private static final int DEFAULT_LOG_K = 12; // Default precision parameter

        @ScalarFunction("hll_create")
        @SqlType(StandardTypes.VARBINARY)
        public static byte[] hllCreate()
        {
            return hllCreate(DEFAULT_LOG_K);
        }

        @ScalarFunction("hll_create")
        @SqlType(StandardTypes.VARBINARY)
        public static byte[] hllCreate(@SqlType(StandardTypes.INTEGER) long logK)
        {
            HllSketch sketch = new HllSketch((int) logK, TgtHllType.HLL_4);
            return sketch.toCompactByteArray();
        }

        @ScalarFunction("hll_add")
        @SqlType(StandardTypes.VARBINARY)
        public static byte[] hllAdd(@SqlType(StandardTypes.VARBINARY) byte[] sketch, @SqlType(StandardTypes.VARCHAR) String value)
        {
            HllSketch hllSketch = HllSketch.heapify(Memory.wrap(sketch));
            hllSketch.update(value);
            return hllSketch.toCompactByteArray();
        }

        @ScalarFunction("hll_add")
        @SqlType(StandardTypes.VARBINARY)
        public static byte[] hllAdd(@SqlType(StandardTypes.VARBINARY) byte[] sketch, @SqlType(StandardTypes.BIGINT) long value)
        {
            HllSketch hllSketch = HllSketch.heapify(Memory.wrap(sketch));
            hllSketch.update(value);
            return hllSketch.toCompactByteArray();
        }

        @ScalarFunction("hll_add")
        @SqlType(StandardTypes.VARBINARY)
        public static byte[] hllAdd(@SqlType(StandardTypes.VARBINARY) byte[] sketch, @SqlType(StandardTypes.DOUBLE) double value)
        {
            HllSketch hllSketch = HllSketch.heapify(Memory.wrap(sketch));
            hllSketch.update(value);
            return hllSketch.toCompactByteArray();
        }

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

        @ScalarFunction("hll_union_agg")
        @SqlType(StandardTypes.VARBINARY)
        public static byte[] hllUnionAgg(@SqlType(StandardTypes.VARBINARY) byte[] sketch)
        {
            return sketch;
        }

        @ScalarFunction("hll_std_error")
        @SqlType(StandardTypes.DOUBLE)
        public static double hllStdError(@SqlType(StandardTypes.VARBINARY) byte[] sketch)
        {
            HllSketch hllSketch = HllSketch.heapify(Memory.wrap(sketch));
            return hllSketch.getRelErr(true, true, false, false);
        }

        @ScalarFunction("hll_upper_bound")
        @SqlType(StandardTypes.DOUBLE)
        public static double hllUpperBound(@SqlType(StandardTypes.VARBINARY) byte[] sketch, @SqlType(StandardTypes.DOUBLE) double numStdDev)
        {
            HllSketch hllSketch = HllSketch.heapify(Memory.wrap(sketch));
            return hllSketch.getUpperBound(numStdDev);
        }

        @ScalarFunction("hll_lower_bound")
        @SqlType(StandardTypes.DOUBLE)
        public static double hllLowerBound(@SqlType(StandardTypes.VARBINARY) byte[] sketch, @SqlType(StandardTypes.DOUBLE) double numStdDev)
        {
            HllSketch hllSketch = HllSketch.heapify(Memory.wrap(sketch));
            return hllSketch.getLowerBound(numStdDev);
        }

        @ScalarFunction("hll_is_empty")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean hllIsEmpty(@SqlType(StandardTypes.VARBINARY) byte[] sketch)
        {
            HllSketch hllSketch = HllSketch.heapify(Memory.wrap(sketch));
            return hllSketch.isEmpty();
        }

        @ScalarFunction("hll_get_log_k")
        @SqlType(StandardTypes.INTEGER)
        public static long hllGetLogK(@SqlType(StandardTypes.VARBINARY) byte[] sketch)
        {
            HllSketch hllSketch = HllSketch.heapify(Memory.wrap(sketch));
            return hllSketch.getLgConfigK();
        }

        // New functions for working with Apache DataSketches format

        @ScalarFunction("hll_from_string")
        @SqlType(StandardTypes.VARBINARY)
        public static byte[] hllFromString(@SqlType(StandardTypes.VARCHAR) String base64String)
        {
            try {
                byte[] bytes = java.util.Base64.getDecoder().decode(base64String);
                HllSketch sketch = HllSketch.heapify(Memory.wrap(bytes));
                return sketch.toCompactByteArray();
            }
            catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid HLL sketch format: " + e.getMessage());
            }
        }

        @ScalarFunction("hll_to_string")
        @SqlType(StandardTypes.VARCHAR)
        public static String hllToString(@SqlType(StandardTypes.VARBINARY) byte[] sketch)
        {
            return java.util.Base64.getEncoder().encodeToString(sketch);
        }

        @ScalarFunction("hll_validate")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean hllValidate(@SqlType(StandardTypes.VARBINARY) byte[] sketch)
        {
            try {
                HllSketch.heapify(Memory.wrap(sketch));
                return true;
            }
            catch (Exception e) {
                return false;
            }
        }

        @ScalarFunction("hll_get_serialization_bytes")
        @SqlType(StandardTypes.INTEGER)
        public static long hllGetSerializationBytes(@SqlType(StandardTypes.VARBINARY) byte[] sketch)
        {
            HllSketch hllSketch = HllSketch.heapify(Memory.wrap(sketch));
            return hllSketch.getSerializationBytes();
        }

        @ScalarFunction("hll_get_compact_bytes")
        @SqlType(StandardTypes.INTEGER)
        public static long hllGetCompactBytes(@SqlType(StandardTypes.VARBINARY) byte[] sketch)
        {
            HllSketch hllSketch = HllSketch.heapify(Memory.wrap(sketch));
            return hllSketch.getCompactSerializationBytes();
        }

        @ScalarFunction("hll_get_updatable_bytes")
        @SqlType(StandardTypes.INTEGER)
        public static long hllGetUpdatableBytes(@SqlType(StandardTypes.VARBINARY) byte[] sketch)
        {
            HllSketch hllSketch = HllSketch.heapify(Memory.wrap(sketch));
            return hllSketch.getUpdatableSerializationBytes();
        }

        @ScalarFunction("hll_get_serialization_version")
        @SqlType(StandardTypes.INTEGER)
        public static long hllGetSerializationVersion(@SqlType(StandardTypes.VARBINARY) byte[] sketch)
        {
            HllSketch hllSketch = HllSketch.heapify(Memory.wrap(sketch));
            return hllSketch.getSerializationVersion();
        }
    }
} 