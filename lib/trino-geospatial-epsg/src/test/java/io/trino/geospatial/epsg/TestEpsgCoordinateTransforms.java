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
package io.trino.geospatial.epsg;

import org.junit.jupiter.api.Test;
import org.opengis.referencing.operation.MathTransform;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.data.Offset.offset;

public class TestEpsgCoordinateTransforms
{
    @Test
    public void testTransform()
            throws Exception
    {
        MathTransform transform = EpsgCoordinateTransforms.transform(4326, 3857);
        double[] source = {-71.0882, 42.3607};
        double[] target = new double[2];

        transform.transform(source, 0, target, 0, 1);

        assertThat(target[0]).isCloseTo(-7913502.22541039, offset(1e-6));
        assertThat(target[1]).isCloseTo(5215164.63048419, offset(1e-6));
    }

    @Test
    public void testTransformFailure()
    {
        assertThatThrownBy(() -> EpsgCoordinateTransforms.transform(-1, 3857))
                .isInstanceOf(EpsgException.class)
                .hasMessageContaining("No EPSG CoordinateReferenceSystem for code -1");
    }
}
