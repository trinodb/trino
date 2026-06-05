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
package io.trino.plugin.iceberg;

import io.trino.metadata.TestingFunctionResolution;
import io.trino.plugin.geospatial.GeoPlugin;
import io.trino.spi.TrinoException;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.geospatial.GeometryType.GEOMETRY;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTypeConverter
{
    private static final TypeManager TESTING_TYPE_MANAGER = new TestingFunctionResolution(new GeoPlugin())
            .getPlannerContext()
            .getTypeManager();

    @Test
    void testGeometryWithCustomSrid()
    {
        assertThat(toTrinoType(Types.GeometryType.of("EPSG:3857"), TESTING_TYPE_MANAGER))
                .isEqualTo(GEOMETRY);
    }

    @Test
    void testGeometryWithInvalidCrs()
    {
        assertThatThrownBy(() -> toTrinoType(Types.GeometryType.of("EPSG:0"), TESTING_TYPE_MANAGER))
                .isInstanceOfSatisfying(TrinoException.class, exception -> assertThat(exception.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessageContaining("Unsupported geometry CRS 'EPSG:0'");
    }
}
