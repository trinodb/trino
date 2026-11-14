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

import io.trino.epsg.EpsgCatalog;
import org.junit.jupiter.api.Test;
import org.opengis.referencing.IdentifiedObject;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.CoordinateOperation;
import org.opengis.util.FactoryException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestEpsgAuthorityFactory
{
    @Test
    public void testAuthorityCodesForSupertype()
            throws IOException
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

    @Test
    public void testCoordinateOperationsFromStandaloneCatalog()
            throws Exception
    {
        EpsgAuthorityFactory factory = new EpsgAuthorityFactory(EpsgCatalog.loadDefault());
        assertThat(factory.createFromCoordinateReferenceSystemCodes("4269", "4326"))
                .containsExactly(
                        factory.createCoordinateOperation("1188"),
                        factory.createCoordinateOperation("1251"),
                        factory.createCoordinateOperation("1252"));
    }

    @Test
    public void testCorruptDefinitions()
            throws IOException
    {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(0x54524745);
            output.writeInt(1);
            for (int code : List.of(4326, 1188)) {
                output.writeInt(1); // entry count
                output.writeInt(code);
                output.writeInt(0); // block index
                output.writeInt(0); // offset
                output.writeInt(1); // definition length
                output.writeInt(1); // block count
                output.writeInt(1); // uncompressed length
                output.writeInt(1); // compressed length
                output.writeByte(0); // invalid Zstd data
            }
            output.writeInt(0); // pair count
        }
        EpsgAuthorityFactory factory = new EpsgAuthorityFactory(EpsgCatalog.read(new ByteArrayInputStream(bytes.toByteArray())));
        assertThatThrownBy(() -> factory.createCoordinateReferenceSystem("4326"))
                .isInstanceOf(FactoryException.class)
                .hasCauseInstanceOf(EpsgException.class)
                .cause()
                .hasCauseInstanceOf(IOException.class);
        assertThatThrownBy(() -> factory.createCoordinateOperation("1188"))
                .isInstanceOf(FactoryException.class)
                .hasCauseInstanceOf(EpsgException.class)
                .cause()
                .hasCauseInstanceOf(IOException.class);
    }
}
