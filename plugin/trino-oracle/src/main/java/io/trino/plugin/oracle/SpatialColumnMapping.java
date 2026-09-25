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
package io.trino.plugin.oracle;

/**
 * How MDSYS.SDO_GEOMETRY and MDSYS.SDO_POINT_TYPE columns are surfaced to Trino.
 * <ul>
 *     <li>{@link #VARCHAR} — unbounded VARCHAR containing the WKT text produced by
 *         {@code MDSYS.SDO_UTIL.TO_WKTGEOMETRY}. Default; matches the initial fix for
 *         trinodb/trino#30020.</li>
 *     <li>{@link #GEOMETRY} — Trino's native GEOMETRY type. WKT is parsed inside the
 *         connector, equivalent to {@code ST_GeometryFromText(...)} on the VARCHAR path.
 *         Requires the geospatial types to be registered on the coordinator.</li>
 * </ul>
 */
public enum SpatialColumnMapping
{
    VARCHAR,
    GEOMETRY,
}
