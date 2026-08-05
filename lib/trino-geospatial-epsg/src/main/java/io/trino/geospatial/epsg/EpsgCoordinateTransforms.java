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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.trino.cache.NonEvictableLoadingCache;
import org.apache.sis.referencing.CRS;
import org.apache.sis.referencing.crs.AbstractCRS;
import org.apache.sis.referencing.cs.AxesConvention;
import org.apache.sis.referencing.operation.CoordinateOperationFinder;
import org.apache.sis.referencing.operation.DefaultCoordinateOperationFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.CoordinateOperation;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.util.FactoryException;

import java.util.concurrent.ExecutionException;

import static io.trino.cache.SafeCaches.buildNonEvictableCache;

public final class EpsgCoordinateTransforms
{
    static final long TRANSFORM_CACHE_MAXIMUM_SIZE = 1000;

    private static final EpsgAuthorityFactory AUTHORITY_FACTORY = new EpsgAuthorityFactory(EpsgCatalog.loadDefault());
    private static final NonEvictableLoadingCache<TransformKey, MathTransform> TRANSFORMS = buildNonEvictableCache(
            CacheBuilder.newBuilder()
                    .maximumSize(TRANSFORM_CACHE_MAXIMUM_SIZE),
            CacheLoader.from(EpsgCoordinateTransforms::loadTransform));

    private EpsgCoordinateTransforms() {}

    public static MathTransform transform(int sourceSrid, int targetSrid)
            throws FactoryException
    {
        try {
            return TRANSFORMS.get(new TransformKey(sourceSrid, targetSrid));
        }
        catch (ExecutionException e) {
            throw transformLoadException(e.getCause());
        }
        catch (UncheckedExecutionException e) {
            throw transformLoadException(e.getCause());
        }
    }

    public static CoordinateReferenceSystem crs(int srid)
            throws FactoryException
    {
        CoordinateReferenceSystem crs = displayOriented(AUTHORITY_FACTORY.createCoordinateReferenceSystem(Integer.toString(srid)));
        int dimension = CRS.getDimensionOrZero(crs);
        if (dimension != 2) {
            throw new FactoryException("ST_Transform only supports two-dimensional CRS. SRID %s has dimension %s".formatted(srid, dimension));
        }
        return crs;
    }

    private static MathTransform loadTransform(TransformKey key)
    {
        try {
            CoordinateReferenceSystem source = crs(key.sourceSrid());
            CoordinateReferenceSystem target = crs(key.targetSrid());
            CoordinateOperation operation = new CoordinateOperationFinder(AUTHORITY_FACTORY, DefaultCoordinateOperationFactory.provider(), null)
                    .createOperation(source, target);
            return operation.getMathTransform();
        }
        catch (FactoryException e) {
            throw new EpsgException(e.getMessage(), e);
        }
    }

    private static CoordinateReferenceSystem displayOriented(CoordinateReferenceSystem crs)
    {
        return AbstractCRS.castOrCopy(crs).forConvention(AxesConvention.DISPLAY_ORIENTED);
    }

    private static EpsgException transformLoadException(Throwable cause)
    {
        if (cause instanceof EpsgException epsgException) {
            return epsgException;
        }
        return new EpsgException(cause.getMessage(), cause);
    }

    private record TransformKey(int sourceSrid, int targetSrid) {}
}
