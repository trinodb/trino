/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.ml;

import com.google.common.collect.ImmutableList;
import io.trino.spi.block.Block;
import io.trino.spi.block.BufferedMapValueBuilder;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;

import java.util.List;

public final class MLFeaturesFunctions
{
    public static final List<Class<?>> ML_FEATURE_FUNCTIONS = ImmutableList.of(Features1.class, Features2.class, Features3.class, Features4.class, Features5.class, Features6.class, Features7.class, Features8.class, Features9.class, Features10.class);

    private static final String MAP_BIGINT_DOUBLE = "map(bigint,double)";

    private MLFeaturesFunctions() {}

    @ScalarFunction("features")
    public static class Features1
    {
        private final BufferedMapValueBuilder mapValueBuilder;

        public Features1(@TypeParameter(MAP_BIGINT_DOUBLE) Type mapType)
        {
            mapValueBuilder = BufferedMapValueBuilder.createBuffered((MapType) mapType);
        }

        @SqlType(MAP_BIGINT_DOUBLE)
        public Block features(@SqlType(StandardTypes.DOUBLE) double f1)
        {
            return featuresHelper(mapValueBuilder, f1);
        }
    }

    @ScalarFunction("features")
    public static class Features2
    {
        private final BufferedMapValueBuilder mapValueBuilder;

        public Features2(@TypeParameter(MAP_BIGINT_DOUBLE) Type mapType)
        {
            mapValueBuilder = BufferedMapValueBuilder.createBuffered((MapType) mapType);
        }

        @SqlType(MAP_BIGINT_DOUBLE)
        public Block features(@SqlType(StandardTypes.DOUBLE) double f1, @SqlType(StandardTypes.DOUBLE) double f2)
        {
            return featuresHelper(mapValueBuilder, f1, f2);
        }
    }

    @ScalarFunction("features")
    public static class Features3
    {
        private final BufferedMapValueBuilder mapValueBuilder;

        public Features3(@TypeParameter(MAP_BIGINT_DOUBLE) Type mapType)
        {
            mapValueBuilder = BufferedMapValueBuilder.createBuffered((MapType) mapType);
        }

        @SqlType(MAP_BIGINT_DOUBLE)
        public Block features(@SqlType(StandardTypes.DOUBLE) double f1, @SqlType(StandardTypes.DOUBLE) double f2, @SqlType(StandardTypes.DOUBLE) double f3)
        {
            return featuresHelper(mapValueBuilder, f1, f2, f3);
        }
    }

    @ScalarFunction("features")
    public static class Features4
    {
        private final BufferedMapValueBuilder mapValueBuilder;

        public Features4(@TypeParameter(MAP_BIGINT_DOUBLE) Type mapType)
        {
            mapValueBuilder = BufferedMapValueBuilder.createBuffered((MapType) mapType);
        }

        @SqlType(MAP_BIGINT_DOUBLE)
        public Block features(@SqlType(StandardTypes.DOUBLE) double f1, @SqlType(StandardTypes.DOUBLE) double f2, @SqlType(StandardTypes.DOUBLE) double f3, @SqlType(StandardTypes.DOUBLE) double f4)
        {
            return featuresHelper(mapValueBuilder, f1, f2, f3, f4);
        }
    }

    @ScalarFunction("features")
    public static class Features5
    {
        private final BufferedMapValueBuilder mapValueBuilder;

        public Features5(@TypeParameter(MAP_BIGINT_DOUBLE) Type mapType)
        {
            mapValueBuilder = BufferedMapValueBuilder.createBuffered((MapType) mapType);
        }

        @SqlType(MAP_BIGINT_DOUBLE)
        public Block features(@SqlType(StandardTypes.DOUBLE) double f1, @SqlType(StandardTypes.DOUBLE) double f2, @SqlType(StandardTypes.DOUBLE) double f3, @SqlType(StandardTypes.DOUBLE) double f4, @SqlType(StandardTypes.DOUBLE) double f5)
        {
            return featuresHelper(mapValueBuilder, f1, f2, f3, f4, f5);
        }
    }

    @ScalarFunction("features")
    public static class Features6
    {
        private final BufferedMapValueBuilder mapValueBuilder;

        public Features6(@TypeParameter(MAP_BIGINT_DOUBLE) Type mapType)
        {
            mapValueBuilder = BufferedMapValueBuilder.createBuffered((MapType) mapType);
        }

        @SqlType(MAP_BIGINT_DOUBLE)
        public Block features(@SqlType(StandardTypes.DOUBLE) double f1, @SqlType(StandardTypes.DOUBLE) double f2, @SqlType(StandardTypes.DOUBLE) double f3, @SqlType(StandardTypes.DOUBLE) double f4, @SqlType(StandardTypes.DOUBLE) double f5, @SqlType(StandardTypes.DOUBLE) double f6)
        {
            return featuresHelper(mapValueBuilder, f1, f2, f3, f4, f5, f6);
        }
    }

    @ScalarFunction("features")
    public static class Features7
    {
        private final BufferedMapValueBuilder mapValueBuilder;

        public Features7(@TypeParameter(MAP_BIGINT_DOUBLE) Type mapType)
        {
            mapValueBuilder = BufferedMapValueBuilder.createBuffered((MapType) mapType);
        }

        @SqlType(MAP_BIGINT_DOUBLE)
        public Block features(@SqlType(StandardTypes.DOUBLE) double f1, @SqlType(StandardTypes.DOUBLE) double f2, @SqlType(StandardTypes.DOUBLE) double f3, @SqlType(StandardTypes.DOUBLE) double f4, @SqlType(StandardTypes.DOUBLE) double f5, @SqlType(StandardTypes.DOUBLE) double f6, @SqlType(StandardTypes.DOUBLE) double f7)
        {
            return featuresHelper(mapValueBuilder, f1, f2, f3, f4, f5, f6, f7);
        }
    }

    @ScalarFunction("features")
    public static class Features8
    {
        private final BufferedMapValueBuilder mapValueBuilder;

        public Features8(@TypeParameter(MAP_BIGINT_DOUBLE) Type mapType)
        {
            mapValueBuilder = BufferedMapValueBuilder.createBuffered((MapType) mapType);
        }

        @SqlType(MAP_BIGINT_DOUBLE)
        public Block features(@SqlType(StandardTypes.DOUBLE) double f1, @SqlType(StandardTypes.DOUBLE) double f2, @SqlType(StandardTypes.DOUBLE) double f3, @SqlType(StandardTypes.DOUBLE) double f4, @SqlType(StandardTypes.DOUBLE) double f5, @SqlType(StandardTypes.DOUBLE) double f6, @SqlType(StandardTypes.DOUBLE) double f7, @SqlType(StandardTypes.DOUBLE) double f8)
        {
            return featuresHelper(mapValueBuilder, f1, f2, f3, f4, f5, f6, f7, f8);
        }
    }

    @ScalarFunction("features")
    public static class Features9
    {
        private final BufferedMapValueBuilder mapValueBuilder;

        public Features9(@TypeParameter(MAP_BIGINT_DOUBLE) Type mapType)
        {
            mapValueBuilder = BufferedMapValueBuilder.createBuffered((MapType) mapType);
        }

        @SqlType(MAP_BIGINT_DOUBLE)
        public Block features(@SqlType(StandardTypes.DOUBLE) double f1, @SqlType(StandardTypes.DOUBLE) double f2, @SqlType(StandardTypes.DOUBLE) double f3, @SqlType(StandardTypes.DOUBLE) double f4, @SqlType(StandardTypes.DOUBLE) double f5, @SqlType(StandardTypes.DOUBLE) double f6, @SqlType(StandardTypes.DOUBLE) double f7, @SqlType(StandardTypes.DOUBLE) double f8, @SqlType(StandardTypes.DOUBLE) double f9)
        {
            return featuresHelper(mapValueBuilder, f1, f2, f3, f4, f5, f6, f7, f8, f9);
        }
    }

    @ScalarFunction("features")
    public static class Features10
    {
        private final BufferedMapValueBuilder mapValueBuilder;

        public Features10(@TypeParameter(MAP_BIGINT_DOUBLE) Type mapType)
        {
            mapValueBuilder = BufferedMapValueBuilder.createBuffered((MapType) mapType);
        }

        @SqlType(MAP_BIGINT_DOUBLE)
        public Block features(@SqlType(StandardTypes.DOUBLE) double f1, @SqlType(StandardTypes.DOUBLE) double f2, @SqlType(StandardTypes.DOUBLE) double f3, @SqlType(StandardTypes.DOUBLE) double f4, @SqlType(StandardTypes.DOUBLE) double f5, @SqlType(StandardTypes.DOUBLE) double f6, @SqlType(StandardTypes.DOUBLE) double f7, @SqlType(StandardTypes.DOUBLE) double f8, @SqlType(StandardTypes.DOUBLE) double f9, @SqlType(StandardTypes.DOUBLE) double f10)
        {
            return featuresHelper(mapValueBuilder, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10);
        }
    }

    private static Block featuresHelper(BufferedMapValueBuilder mapValueBuilder, double... features)
    {
        return mapValueBuilder.build(features.length, (keyBuilder, valueBuilder) -> {
            for (int i = 0; i < features.length; i++) {
                BigintType.BIGINT.writeLong(keyBuilder, i);
                DoubleType.DOUBLE.writeDouble(valueBuilder, features[i]);
            }
        });
    }
}
