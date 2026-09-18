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
import org.opengis.referencing.IdentifiedObject;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.CoordinateOperation;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class TestEpsgAuthorityFactory
{
    @Test
    public void testAuthorityCodesForSupertype()
    {
        EpsgAuthorityFactory factory = new EpsgAuthorityFactory(EpsgCatalog.loadDefault());

        Set<String> crsCodes = factory.getAuthorityCodes(CoordinateReferenceSystem.class);
        Set<String> operationCodes = factory.getAuthorityCodes(CoordinateOperation.class);
        Set<String> identifiedObjectCodes = factory.getAuthorityCodes(IdentifiedObject.class);

        assertThat(crsCodes).isNotEmpty();
        assertThat(operationCodes).isNotEmpty();
        assertThat(identifiedObjectCodes)
                .containsAll(crsCodes)
                .containsAll(operationCodes);
    }
}
