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
import org.apache.sis.io.wkt.WKTFormat;
import org.apache.sis.io.wkt.Warnings;
import org.junit.jupiter.api.Test;
import org.opengis.referencing.IdentifiedObject;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.CoordinateOperation;
import org.opengis.util.FactoryException;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.base.Throwables.getRootCause;
import static io.trino.geospatial.epsg.EpsgAuthorityFactory.wktFormat;
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

        assertThat(crsCodes).contains("4326");
        assertThat(operationCodes).contains("1188");
        assertThat(identifiedObjectCodes)
                .containsAll(crsCodes)
                .containsAll(operationCodes);
    }

    @Test
    public void testCreateFromCoordinateReferenceSystemCodes()
            throws Exception
    {
        EpsgAuthorityFactory factory = new EpsgAuthorityFactory(EpsgCatalog.loadDefault());
        assertThat(factory.createFromCoordinateReferenceSystemCodes("4269", "4326"))
                .containsExactly(
                        factory.createCoordinateOperation("1188"),
                        factory.createCoordinateOperation("1252"),
                        factory.createCoordinateOperation("1251"));
    }

    @Test
    public void testCreateAllDefinitions()
            throws ParseException
    {
        // The library writes the catalog with its own version of Apache SIS, and Trino parses it with the
        // version in its own dependencies. Parse every definition, so that a WKT incompatibility between the
        // two versions fails here instead of at query time for the affected codes. SIS reports elements it
        // cannot apply as warnings and still returns the object, so any warning other than the known ones fails.
        EpsgCatalog catalog = EpsgCatalog.loadDefault();
        EpsgAuthorityFactory factory = new EpsgAuthorityFactory(catalog);
        List<String> failures = new ArrayList<>();
        List<String> warnings = new ArrayList<>();
        for (String code : factory.getAuthorityCodes(CoordinateReferenceSystem.class)) {
            try {
                factory.createCoordinateReferenceSystem(code);
            }
            catch (FactoryException | RuntimeException e) {
                failures.add("CRS %s: %s".formatted(code, getRootCause(e)));
            }
            addWarnings(catalog.crsWkt(Integer.parseInt(code)).orElseThrow(), code, warnings);
        }
        for (String code : factory.getAuthorityCodes(CoordinateOperation.class)) {
            try {
                factory.createCoordinateOperation(code);
            }
            catch (FactoryException | RuntimeException e) {
                failures.add("Operation %s: %s".formatted(code, getRootCause(e)));
            }
            addWarnings(catalog.operationWkt(Integer.parseInt(code)).orElseThrow(), code, warnings);
        }
        assertThat(failures).isEmpty();
        // The NCRS Las Vegas CRSs have a vertical extent in metres, which SIS cannot apply
        assertThat(warnings).containsExactlyElementsOf(IntStream.of(8379, 8380, 8381, 8382, 8383, 8384, 8385, 8387)
                .mapToObj(code -> "EPSG %s: Cannot assign units “m” to dimension “VerticalExtent”.".formatted(code))
                .toList());
    }

    private static void addWarnings(String wkt, String code, List<String> warnings)
            throws ParseException
    {
        WKTFormat format = wktFormat();
        format.parseObject(wkt);
        Warnings parseWarnings = format.getWarnings();
        if (parseWarnings != null) {
            for (int i = 0; i < parseWarnings.getNumMessages(); i++) {
                warnings.add("EPSG %s: %s".formatted(code, parseWarnings.getMessage(i)));
            }
        }
    }
}
