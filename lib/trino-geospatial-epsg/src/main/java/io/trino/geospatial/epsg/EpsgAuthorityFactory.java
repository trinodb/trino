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

import org.apache.sis.io.wkt.Convention;
import org.apache.sis.io.wkt.WKTFormat;
import org.apache.sis.metadata.iso.citation.Citations;
import org.apache.sis.referencing.factory.GeodeticAuthorityFactory;
import org.opengis.metadata.citation.Citation;
import org.opengis.referencing.IdentifiedObject;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.CoordinateOperation;
import org.opengis.referencing.operation.CoordinateOperationAuthorityFactory;
import org.opengis.referencing.operation.OperationMethod;
import org.opengis.util.FactoryException;

import java.text.ParseException;
import java.time.ZoneId;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

final class EpsgAuthorityFactory
        extends GeodeticAuthorityFactory
        implements CRSAuthorityFactory, CoordinateOperationAuthorityFactory
{
    private final EpsgCatalog catalog;
    private final Map<Integer, CoordinateReferenceSystem> crsCache = new ConcurrentHashMap<>();
    private final Map<Integer, CoordinateOperation> operationCache = new ConcurrentHashMap<>();

    EpsgAuthorityFactory(EpsgCatalog catalog)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
    }

    @Override
    public Citation getAuthority()
    {
        return Citations.EPSG;
    }

    @Override
    public Set<String> getAuthorityCodes(Class<? extends IdentifiedObject> type)
    {
        boolean includeCrs = CoordinateReferenceSystem.class.isAssignableFrom(type) || type.isAssignableFrom(CoordinateReferenceSystem.class);
        boolean includeOperations = CoordinateOperation.class.isAssignableFrom(type) || type.isAssignableFrom(CoordinateOperation.class);
        if (includeCrs && includeOperations) {
            Set<String> codes = new LinkedHashSet<>(catalog.crsCodes());
            codes.addAll(catalog.operationCodes());
            return codes;
        }
        if (includeCrs) {
            return catalog.crsCodes();
        }
        if (includeOperations) {
            return catalog.operationCodes();
        }
        return Set.of();
    }

    @Override
    public IdentifiedObject createObject(String code)
            throws NoSuchAuthorityCodeException, FactoryException
    {
        int epsgCode = parseCode(code);
        if (catalog.hasCrsCode(epsgCode)) {
            return createCoordinateReferenceSystem(code);
        }
        if (catalog.hasOperationCode(epsgCode)) {
            return createCoordinateOperation(code);
        }
        throw noSuchAuthorityCode(IdentifiedObject.class, code);
    }

    @Override
    public CoordinateReferenceSystem createCoordinateReferenceSystem(String code)
            throws NoSuchAuthorityCodeException, FactoryException
    {
        int epsgCode = parseCode(code);
        if (!catalog.hasCrsCode(epsgCode)) {
            throw noSuchAuthorityCode(CoordinateReferenceSystem.class, code);
        }
        try {
            return crsCache.computeIfAbsent(epsgCode, this::parseCoordinateReferenceSystem);
        }
        catch (EpsgException e) {
            throw new FactoryException(e.getMessage(), e);
        }
    }

    @Override
    public CoordinateOperation createCoordinateOperation(String code)
            throws NoSuchAuthorityCodeException, FactoryException
    {
        int epsgCode = parseCode(code);
        if (!catalog.hasOperationCode(epsgCode)) {
            throw noSuchAuthorityCode(CoordinateOperation.class, code);
        }
        try {
            return operationCache.computeIfAbsent(epsgCode, this::parseCoordinateOperation);
        }
        catch (EpsgException e) {
            throw new FactoryException(e.getMessage(), e);
        }
    }

    @Override
    public OperationMethod createOperationMethod(String code)
            throws NoSuchAuthorityCodeException
    {
        throw noSuchAuthorityCode(OperationMethod.class, code);
    }

    @Override
    public Set<CoordinateOperation> createFromCoordinateReferenceSystemCodes(String sourceCRS, String targetCRS)
            throws FactoryException
    {
        int[] operationCodes = catalog.getOperationCodes(parseCode(sourceCRS), parseCode(targetCRS));
        Set<CoordinateOperation> operations = new LinkedHashSet<>();
        for (int operationCode : operationCodes) {
            operations.add(createCoordinateOperation(Integer.toString(operationCode)));
        }
        return operations;
    }

    private CoordinateReferenceSystem parseCoordinateReferenceSystem(int code)
    {
        try {
            return (CoordinateReferenceSystem) wktFormat().parseObject(catalog.getCrsWkt(code));
        }
        catch (ParseException e) {
            throw new EpsgException("Failed to parse EPSG CRS " + code, e);
        }
    }

    private CoordinateOperation parseCoordinateOperation(int code)
    {
        try {
            return (CoordinateOperation) wktFormat().parseObject(catalog.getOperationWkt(code));
        }
        catch (ParseException e) {
            throw new EpsgException("Failed to parse EPSG coordinate operation " + code, e);
        }
    }

    private static WKTFormat wktFormat()
    {
        WKTFormat format = new WKTFormat(Locale.ROOT, (ZoneId) null);
        format.setConvention(Convention.WKT2_2019);
        return format;
    }

    private static int parseCode(String code)
            throws NoSuchAuthorityCodeException
    {
        String original = requireNonNull(code, "code is null");
        int separator = code.lastIndexOf(':');
        if (separator >= 0) {
            code = code.substring(separator + 1);
        }
        try {
            return Integer.parseInt(code);
        }
        catch (NumberFormatException e) {
            throw new NoSuchAuthorityCodeException("Invalid EPSG code: " + original, "EPSG", original);
        }
    }

    private static NoSuchAuthorityCodeException noSuchAuthorityCode(Class<?> type, String code)
    {
        return new NoSuchAuthorityCodeException("No EPSG " + type.getSimpleName() + " for code " + code, "EPSG", code);
    }
}
