# Geospatial functions

Trino Geospatial functions that begin with the `ST_` prefix support the SQL/MM specification
and are compliant with the Open Geospatial Consortium’s (OGC) OpenGIS Specifications.
As such, many Trino Geospatial functions require, or more accurately, assume that
geometries that are operated on are both simple and valid. For example, it does not
make sense to calculate the area of a polygon that has a hole defined outside the
polygon, or to construct a polygon from a non-simple boundary line.

Trino Geospatial functions support Well-Known Text (WKT), Extended Well-Known
Text (EWKT), Well-Known Binary (WKB), and Extended Well-Known Binary (EWKB)
forms of spatial objects:

- `POINT (0 0)`
- `LINESTRING (0 0, 1 1, 1 2)`
- `POLYGON ((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))`
- `MULTIPOINT (0 0, 1 2)`
- `MULTILINESTRING ((0 0, 1 1, 1 2), (2 3, 3 2, 5 4))`
- `MULTIPOLYGON (((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1)), ((-1 -1, -1 -2, -2 -2, -2 -1, -1 -1)))`
- `GEOMETRYCOLLECTION (POINT(2 3), LINESTRING (2 3, 3 4))`

Use {func}`ST_GeometryFromText` and {func}`ST_GeomFromBinary` functions to create geometry
objects from WKT or WKB.

The `SphericalGeography` type provides native support for spatial features represented on
*geographic* coordinates (sometimes called *geodetic* coordinates, or *lat/lon*, or *lon/lat*).
Geographic coordinates are spherical coordinates expressed in angular units (degrees).

The basis for the `Geometry` type is a plane. The shortest path between two points on the plane is a
straight line. That means calculations on geometries (areas, distances, lengths, intersections, etc.)
can be calculated using cartesian mathematics and straight line vectors.
Geometry values can include Z coordinates and SRID metadata. Trino preserves Z
coordinates and SRIDs in geometry values and supported format round trips, but
geometry calculations are planar and use the X and Y coordinates.
Functions that compare two `Geometry` values require matching SRIDs when both
values have non-zero SRIDs. An SRID of `0` means the SRID is unspecified.

The basis for the `SphericalGeography` type is a sphere. The shortest path between two points on the
sphere is a great circle arc. That means that calculations on geographies (areas, distances,
lengths, intersections, etc.) must be calculated on the sphere, using more complicated mathematics.
More accurate measurements that take the actual spheroidal shape of the world into account are not
supported.

For `Geometry`, values returned by measurement functions such as
{func}`ST_Distance`, {func}`ST_Length`, and {func}`ST_Area` are in the units of
the input coordinates. For `SphericalGeography`, distance and length are in
meters and area is in square meters.

Use {func}`to_spherical_geography()` function to convert a geometry object to geography object.

For example, `ST_Distance(ST_Point(-71.0882, 42.3607), ST_Point(-74.1197, 40.6976))` returns
`3.4577` in the unit of the passed-in values on the Euclidean plane, while
`ST_Distance(to_spherical_geography(ST_Point(-71.0882, 42.3607)), to_spherical_geography(ST_Point(-74.1197, 40.6976)))`
returns `312822.179` in meters.

## Constructors

:::{function} ST_AsBinary(Geometry) -> varbinary
Returns the ISO WKB representation of the geometry. WKB output does not carry SRID
metadata, so reading this value back with {func}`ST_GeomFromBinary` produces a
geometry with SRID `0`. Z coordinates are preserved when present. Use
{func}`ST_AsEWKB` when SRID metadata must be preserved.
:::

:::{function} ST_AsEWKB(Geometry) -> varbinary
Returns the EWKB representation of the geometry, including the SRID when it is
non-zero.
:::

:::{function} ST_AsText(Geometry) -> varchar
Returns the WKT representation of the geometry. For empty geometries,
`ST_AsText(ST_LineFromText('LINESTRING EMPTY'))` will produce `'MULTILINESTRING EMPTY'`
and `ST_AsText(ST_Polygon('POLYGON EMPTY'))` will produce `'MULTIPOLYGON EMPTY'`.
:::

:::{function} ST_AsEWKT(Geometry) -> varchar
Returns the EWKT representation of the geometry, including the SRID when it is
non-zero.
:::

:::{function} ST_GeometryFromText(varchar) -> Geometry
Returns a geometry type object from WKT representation.
:::

:::{function} ST_GeomFromBinary(varbinary) -> Geometry
Returns a geometry type object from a WKB or EWKB representation. WKB inputs
produce geometries with SRID `0`; EWKB inputs preserve the encoded SRID.
:::

:::{function} ST_GeomFromEWKT(varchar) -> Geometry
Returns a geometry type object from EWKT representation. EWKT accepts WKT with
an optional `SRID=<srid>;` prefix.
:::

:::{function} ST_GeomFromKML(varchar) -> Geometry
Returns a geometry type object from KML representation.
:::

:::{function} geometry_from_hadoop_shape(varbinary) -> Geometry
Returns a geometry type object from Spatial Framework for Hadoop representation.
:::

:::{function} ST_LineFromText(varchar) -> LineString
Returns a geometry type linestring object from WKT representation.
:::

:::{function} ST_LineString(array(Point)) -> LineString
Returns a LineString formed from an array of points. If there are fewer than
two non-empty points in the input array, an empty LineString will be returned.
Array elements must not be `NULL` or have the same X, Y, and Z coordinates as
the previous element.
The returned geometry may not be simple, e.g. may self-intersect or may contain
duplicate vertexes depending on the input. The returned geometry preserves input
Z coordinates and uses the non-zero SRID from the input points. Inputs with
different non-zero SRIDs fail.
:::

:::{function} ST_Collect(array(Geometry)) -> Geometry
Returns a multi-geometry or geometry collection containing the input geometries.
`NULL` array elements are ignored. The returned geometry preserves input Z
coordinates and uses the non-zero SRID from the input geometries. Inputs with
different non-zero SRIDs fail.
:::

:::{function} ST_MakeLine(array(Geometry)) -> LineString
Returns a LineString formed from an array of points or linestrings. Empty
geometries are ignored, and a coordinate with the same X, Y, and Z values at the
boundary of two inputs is included only once. Array elements must not be `NULL`.
The returned geometry preserves input Z coordinates and uses the non-zero SRID
from the input geometries. Inputs with different non-zero SRIDs fail.
:::

:::{function} ST_MakePolygon(LineString) -> Polygon
Returns a polygon formed from a closed LineString shell. The returned geometry
preserves the input Z coordinates and SRID.
:::

:::{function} ST_MakePolygon(LineString, array(LineString)) -> Polygon
:noindex: true

Returns a polygon formed from a closed LineString shell and an array of closed
LineString holes. Empty holes are ignored. The returned geometry preserves input
Z coordinates and uses the non-zero SRID from the shell and holes. Inputs with
different non-zero SRIDs fail.
:::

:::{function} ST_MultiPoint(array(Point)) -> MultiPoint
Returns a MultiPoint geometry object formed from the specified points. Returns `NULL` if input array is empty.
Array elements must not be `NULL` or empty.
The returned geometry may not be simple and may contain duplicate points if input array has duplicates.
The returned geometry preserves input Z coordinates and uses the non-zero SRID
from the input points. Inputs with different non-zero SRIDs fail.
:::

:::{function} ST_Multi(Geometry) -> Geometry
Returns a multi-geometry for a Point, LineString, or Polygon input. Multi-geometry
and geometry collection inputs are returned unchanged. The returned geometry
preserves the input Z coordinates and SRID. Empty Point, LineString, and Polygon
inputs produce an empty multi-geometry of the corresponding type.
:::

:::{function} ST_Point(x: double, y: double) -> Point
Returns a geometry type point object with the given coordinate values.
:::

:::{function} ST_Point(x: double, y: double, srid: integer) -> Point
:noindex: true

Returns a two-dimensional point with the given X and Y coordinate values and
SRID.
:::

:::{function} ST_Point(x: double, y: double, z: double) -> Point
:noindex: true

Returns a geometry type point object with the given X, Y, and Z coordinate
values. The Z coordinate must be finite.
:::

:::{warning}
The SQL type of the third argument selects between the three-argument
overloads. An `INTEGER` third argument is interpreted as the SRID, while a
`DOUBLE` third argument is interpreted as the Z coordinate. Cast the argument
explicitly when necessary. For example, use `ST_Point(1, 2, DOUBLE '3')` to
construct a point with Z coordinate `3`.
:::

:::{function} ST_Point(x: double, y: double, z: double, srid: integer) -> Point
:noindex: true

Returns a geometry type point object with the given X, Y, and Z coordinate
values and SRID. The Z coordinate must be finite.
:::

:::{function} ST_Polygon(varchar) -> Polygon
Returns a geometry type polygon object from WKT representation.
:::

:::{function} to_spherical_geography(Geometry) -> SphericalGeography
Converts a Geometry object to a SphericalGeography object on the sphere of the Earth's radius. This
function is only applicable to `POINT`, `MULTIPOINT`, `LINESTRING`, `MULTILINESTRING`,
`POLYGON`, `MULTIPOLYGON` geometries defined in 2D space, or `GEOMETRYCOLLECTION` of such
geometries. For each point of the input geometry, it verifies that `point.x` is within
`[-180.0, 180.0]` and `point.y` is within `[-90.0, 90.0]`, and uses them as (longitude, latitude)
degrees to construct the shape of the `SphericalGeography` result. SRID and Z
metadata are not part of the `SphericalGeography` value.
:::

:::{function} to_geometry(SphericalGeography) -> Geometry
Converts a SphericalGeography object to a Geometry object.
:::

## Relationship tests

:::{function} ST_Contains(geometryA: Geometry, geometryB: Geometry) -> boolean
Returns `true` if and only if no points of the second geometry lie in the exterior
of the first geometry, and at least one point of the interior of the first geometry
lies in the interior of the second geometry.
:::

:::{function} ST_Crosses(first: Geometry, second: Geometry) -> boolean
Returns `true` if the supplied geometries have some, but not all, interior points in common.
:::

:::{function} ST_Disjoint(first: Geometry, second: Geometry) -> boolean
Returns `true` if the give geometries do not *spatially intersect* --
if they do not share any space together.
:::

:::{function} ST_Equals(first: Geometry, second: Geometry) -> boolean
Returns `true` if the given geometries represent the same geometry.
:::

:::{function} ST_Intersects(first: Geometry, second: Geometry) -> boolean
Returns `true` if the given geometries spatially intersect in two dimensions
(share any portion of space) and `false` if they do not (they are disjoint).
:::

:::{function} ST_Overlaps(first: Geometry, second: Geometry) -> boolean
Returns `true` if the given geometries share space, are of the same dimension,
but are not completely contained by each other.
:::

:::{function} ST_Relate(first: Geometry, second: Geometry) -> boolean
Returns `true` if first geometry is spatially related to second geometry.
:::

:::{function} ST_Touches(first: Geometry, second: Geometry) -> boolean
Returns `true` if the given geometries have at least one point in common,
but their interiors do not intersect.
:::

:::{function} ST_Within(first: Geometry, second: Geometry) -> boolean
Returns `true` if first geometry is completely inside second geometry.
:::

## Operations

:::{function} geometry_nearest_points(first: Geometry, second: Geometry) -> row(Point, Point)
Returns the points on each geometry nearest the other.  If either geometry
is empty, return `NULL`.  Otherwise, return a row of two Points that have
the minimum distance of any two points on the geometries.  The first Point
will be from the first Geometry argument, the second from the second Geometry
argument.  If there are multiple pairs with the minimum distance, one pair
is chosen arbitrarily. The calculation is planar and uses the X and Y
coordinates.
:::

:::{function} geometry_union(array(Geometry)) -> Geometry
Returns a geometry that represents the point set union of the input geometries. Performance
of this function, in conjunction with {func}`array_agg` to first aggregate the input geometries,
may be better than {func}`geometry_union_agg`, at the expense of higher memory utilization.
:::

:::{function} ST_Boundary(Geometry) -> Geometry
Returns the closure of the combinatorial boundary of this geometry.
:::

:::{function} ST_Buffer(Geometry, distance) -> Geometry
Returns the geometry that represents all points whose distance from the specified geometry
is less than or equal to the specified distance. If the points of the geometry are extremely
close together (``delta < 1e-8``), this might return an empty geometry.
:::

:::{function} ST_Force2D(Geometry) -> Geometry
Returns the input geometry with Z coordinates removed. The returned geometry
preserves the input SRID.
:::

:::{function} ST_Force3D(Geometry) -> Geometry
Returns the input geometry with missing Z coordinates set to `0.0`. Existing Z
coordinates are preserved, and the returned geometry preserves the input SRID.
:::

:::{function} ST_Force3D(Geometry, z) -> Geometry
:noindex: true

Returns the input geometry with missing Z coordinates set to the specified value.
Existing Z coordinates are preserved, and the returned geometry preserves the
input SRID. The specified Z value must be finite.
:::

:::{function} ST_Difference(first: Geometry, second: Geometry) -> Geometry
Returns the geometry value that represents the point set difference of the given geometries.
:::

:::{function} ST_Envelope(Geometry) -> Geometry
Returns the bounding rectangular polygon of a geometry. The envelope is
computed from X and Y coordinates, preserves the input SRID, and returns a
two-dimensional geometry.
:::

:::{function} ST_EnvelopeAsPts(Geometry) -> array(Geometry)
Returns an array of two points: the lower left and upper right corners of the bounding
rectangular polygon of a geometry. The points are computed from X and Y
coordinates and preserve the input SRID, but do not include Z values. Returns
`NULL` if input geometry is empty.
:::

:::{function} ST_ExteriorRing(Geometry) -> Geometry
Returns a line string representing the exterior ring of the input polygon.
:::

:::{function} ST_Intersection(first: Geometry, second: Geometry) -> Geometry
Returns the geometry value that represents the point set intersection of two geometries.
:::

:::{function} ST_LineMerge(Geometry) -> Geometry
Returns a LineString or MultiLineString formed by merging connected linework.
The returned geometry preserves the input SRID and any Z coordinates provided by
JTS for retained vertices. Input other than a LineString or MultiLineString
produces an empty GeometryCollection.
:::

:::{function} ST_Normalize(Geometry) -> Geometry
Returns the canonical normalized representation of the input geometry. The
returned geometry preserves the input SRID and Z coordinates.
:::

:::{function} ST_Polygonize(array(Geometry)) -> Geometry
Returns polygons formed from the input linework. `NULL` array elements are
ignored. The returned geometry uses the non-zero SRID from the input geometries.
Inputs with different non-zero SRIDs fail.
:::

:::{function} ST_ReducePrecision(Geometry, gridSize) -> Geometry
Returns the input geometry with coordinates rounded to the specified grid size.
The grid size must be finite and positive. The returned geometry preserves the
input SRID and any Z coordinates provided by JTS for retained vertices.
:::

:::{function} ST_SymDifference(first: Geometry, second: Geometry) -> Geometry
Returns the geometry value that represents the point set symmetric difference of two geometries.
:::

:::{function} ST_Union(first: Geometry, second: Geometry) -> Geometry
Returns a geometry that represents the point set union of the input geometries.

See also:  {func}`geometry_union`, {func}`geometry_union_agg`
:::

:::{function} ST_VoronoiPolygons(Geometry) -> Geometry
Returns Voronoi polygons from the vertices of the input geometry. The returned
geometry preserves the input SRID.
:::

:::{function} ST_VoronoiPolygons(Geometry, tolerance) -> Geometry
:noindex: true

Returns Voronoi polygons from the vertices of the input geometry using the
specified tolerance. The tolerance must be finite and non-negative. The returned
geometry preserves the input SRID.
:::

## Accessors

:::{function} ST_Area(Geometry) -> double
Returns the 2D Euclidean area of a geometry.

For Point and LineString types, returns 0.0.
For GeometryCollection types, returns the sum of the areas of the individual
geometries.
:::

:::{function} ST_Area(SphericalGeography) -> double
:noindex: true

Returns the area of a polygon or multi-polygon in square meters using a spherical model for Earth.
:::

:::{function} ST_Centroid(Geometry) -> Geometry
Returns the point value that is the mathematical centroid of a geometry.
:::

:::{function} ST_ConvexHull(Geometry) -> Geometry
Returns the minimum convex geometry that encloses all input geometries.
:::

:::{function} ST_MinimumBoundingCircle(Geometry) -> Geometry
Returns the minimum bounding circle enclosing the geometry. The returned geometry
preserves the input SRID.
:::

:::{function} ST_OrientedEnvelope(Geometry) -> Geometry
Returns the minimum-area rotated rectangle enclosing the geometry. The returned
geometry preserves the input SRID.
:::

:::{function} ST_CoordDim(Geometry) -> tinyint
Returns the coordinate dimension of the geometry.
:::

:::{function} ST_Dimension(Geometry) -> tinyint
Returns the inherent dimension of this geometry object, which must be
less than or equal to the coordinate dimension.
:::

:::{function} ST_Distance(first: Geometry, second: Geometry) -> double
:noindex: true

Returns the 2-dimensional cartesian minimum distance (based on spatial ref)
between two geometries in projected units.
:::

:::{function} ST_Distance(first: SphericalGeography, second: SphericalGeography) -> double
Returns the great-circle distance in meters between two SphericalGeography points.
:::

:::{function} ST_GeometryN(Geometry, index) -> Geometry
Returns the geometry element at a given index (indices start at 1).
If the geometry is a collection of geometries (e.g., GEOMETRYCOLLECTION or MULTI\*),
returns the geometry at a given index.
If the given index is less than 1 or greater than the total number of elements in the collection,
returns `NULL`.
Use {func}`ST_NumGeometries` to find out the total number of elements.
Singular geometries (e.g., POINT, LINESTRING, POLYGON), are treated as collections of one element.
Empty geometries are treated as empty collections.
:::

:::{function} ST_InteriorRingN(Geometry, index) -> Geometry
Returns the interior ring element at the specified index (indices start at 1). If
the given index is less than 1 or greater than the total number of interior rings
in the input geometry, returns `NULL`. The input geometry must be a polygon.
Use {func}`ST_NumInteriorRing` to find out the total number of elements.
:::

:::{function} ST_GeometryType(Geometry) -> varchar
Returns the type of the geometry.
:::

:::{function} ST_IsClosed(Geometry) -> boolean
Returns `true` if the linestring's start and end points are coincident in the X
and Y coordinates.
:::

:::{function} ST_IsEmpty(Geometry) -> boolean
Returns `true` if this Geometry is an empty geometrycollection, polygon, point etc.
:::

:::{function} ST_IsSimple(Geometry) -> boolean
Returns `true` if this Geometry has no anomalous geometric points, such as self
intersection or self tangency. The simplicity check is planar and uses the X and
Y coordinates.
:::

:::{function} ST_IsRing(Geometry) -> boolean
Returns `true` if and only if the line is closed and simple in the X and Y
coordinates.
:::

:::{function} ST_IsValid(Geometry) -> boolean
Returns `true` if and only if the input geometry is well-formed.
Use {func}`geometry_invalid_reason` to determine why the geometry is not well-formed.
The validity check is planar and uses the X and Y coordinates.
:::

:::{function} ST_Length(Geometry) -> double
Returns the length of a linestring or multi-linestring using Euclidean measurement on a
two-dimensional plane (based on spatial ref) in projected units.
:::

:::{function} ST_Length(SphericalGeography) -> double
:noindex: true

Returns the length of a linestring or multi-linestring on a spherical model of the Earth.
This is equivalent to the sum of great-circle distances between adjacent points on the linestring.
:::

:::{function} ST_PointN(LineString, index) -> Point
Returns the vertex of a linestring at a given index (indices start at 1).
If the given index is less than 1 or greater than the total number of elements in the collection,
returns `NULL`.
Use {func}`ST_NumPoints` to find out the total number of elements.
:::

:::{function} ST_Points(Geometry) -> array(Point)
Returns an array of points in a linestring.
:::

:::{function} ST_PointOnSurface(Geometry) -> Geometry
Returns a point guaranteed to lie on the surface of the input geometry, or
`NULL` if the input geometry is empty. The returned point preserves the input
SRID.
:::

:::{function} ST_XMax(Geometry) -> double
Returns the maximum X coordinate of the bounding box of a geometry. The
bounding box is computed from X and Y coordinates; SRID and Z metadata do not
affect the result.
:::

:::{function} ST_YMax(Geometry) -> double
Returns the maximum Y coordinate of the bounding box of a geometry. The
bounding box is computed from X and Y coordinates; SRID and Z metadata do not
affect the result.
:::

:::{function} ST_XMin(Geometry) -> double
Returns the minimum X coordinate of the bounding box of a geometry. The
bounding box is computed from X and Y coordinates; SRID and Z metadata do not
affect the result.
:::

:::{function} ST_YMin(Geometry) -> double
Returns the minimum Y coordinate of the bounding box of a geometry. The
bounding box is computed from X and Y coordinates; SRID and Z metadata do not
affect the result.
:::

:::{function} ST_StartPoint(Geometry) -> point
Returns the first point of a LineString geometry as a Point.
This is a shortcut for `ST_PointN(geometry, 1)`.
:::

:::{function} ST_SRID(Geometry) -> integer
Returns the spatial reference identifier of the geometry. SRID `0` means the
SRID is unspecified.
:::

:::{function} ST_SetSRID(Geometry, srid) -> Geometry
Returns the input geometry with updated SRID metadata. This function does not
transform coordinates.
:::

:::{function} ST_Transform(Geometry, srid) -> Geometry
Transforms the coordinates of the input geometry from its source SRID to the
target EPSG SRID. The input geometry must have a non-zero SRID; use
`ST_SetSRID` first if the geometry's source SRID is known but unspecified in the
value.

The target SRID must be non-zero, and only two-dimensional CRS definitions are
currently supported. A non-empty geometry with Z coordinates cannot be
transformed between different SRIDs and causes an error. Use
{func}`ST_TransformXY` to transform only X and Y while preserving Z unchanged.
The returned geometry has the target SRID.
:::

:::{function} ST_TransformXY(Geometry, srid) -> Geometry
Transforms the X and Y coordinates of the input geometry from its source SRID
to the target EPSG SRID while preserving Z coordinates unchanged. The source
and target SRIDs must be non-zero and identify supported two-dimensional CRS
definitions. The returned geometry has the target SRID.
:::

:::{function} simplify_geometry(Geometry, double) -> Geometry
Returns a "simplified" version of the input geometry using the Douglas-Peucker algorithm.
Will avoid creating derived geometries (polygons in particular) that are invalid.
:::

:::{function} ST_EndPoint(Geometry) -> point
Returns the last point of a LineString geometry as a Point.
This is a shortcut for `ST_PointN(geometry, ST_NumPoints(geometry))`.
:::

:::{function} ST_X(Point) -> double
Returns the X coordinate of the point, or `NULL` if the point is empty.
:::

:::{function} ST_Y(Point) -> double
Returns the Y coordinate of the point, or `NULL` if the point is empty.
:::

:::{function} ST_Z(Point) -> double
Returns the Z coordinate of the point, or `NULL` if the point is empty or does
not have a Z coordinate.
:::

:::{function} ST_InteriorRings(Geometry) -> array(Geometry)
Returns an array of all interior rings found in the input geometry, or an empty
array if the polygon has no interior rings. Returns `NULL` if the input geometry
is empty. The input geometry must be a polygon. Returned rings preserve the
input geometry's SRID and Z coordinates.
:::

:::{function} ST_NumGeometries(Geometry) -> bigint
Returns the number of geometries in the collection.
If the geometry is a collection of geometries (e.g., GEOMETRYCOLLECTION or MULTI\*),
returns the number of geometries,
for single geometries returns 1,
for empty geometries returns 0.
:::

:::{function} ST_Geometries(Geometry) -> array(Geometry)
Returns an array of geometries in the specified collection. Returns a one-element array
if the input geometry is not a multi-geometry. Returns `NULL` if input geometry is empty.
Returned geometries preserve the input geometry's SRID and Z coordinates.
:::

:::{function} ST_NumPoints(Geometry) -> bigint
Returns the number of points in a geometry. This is an extension to the SQL/MM
`ST_NumPoints` function which only applies to point and linestring.
:::

:::{function} ST_NumInteriorRing(Geometry) -> bigint
Returns the cardinality of the collection of interior rings of a polygon.
:::

:::{function} line_interpolate_point(LineString, double) -> Geometry
Returns a Point interpolated along a LineString at the fraction given. The fraction
must be between 0 and 1, inclusive. The returned point preserves the input
geometry's SRID and uses the Z coordinate provided by JTS when available.
:::

:::{function} line_interpolate_points(LineString, double, repeated) -> array(Geometry)
Returns an array of Points interpolated along a LineString. The fraction must be
between 0 and 1, inclusive. Returned points preserve the input geometry's SRID
and use the Z coordinates provided by JTS when available.
:::

:::{function} line_locate_point(LineString, Point) -> double
Returns a float between 0 and 1 representing the location of the closest point on
the LineString to the given Point, as a fraction of total 2d line length.
Inputs with different non-zero SRIDs fail.

Returns `NULL` if a LineString or a Point is empty or `NULL`.
:::

:::{function} geometry_invalid_reason(Geometry) -> varchar
Returns the reason for why the input geometry is not valid.
Returns `NULL` if the input is valid.
:::

:::{function} great_circle_distance(latitude1, longitude1, latitude2, longitude2) -> double
Returns the great-circle distance between two points on Earth's surface in kilometers.
:::

:::{function} to_geojson_geometry(SphericalGeography) -> varchar
Returns the GeoJSON encoding defined by the input spherical geography.
:::

:::{function} to_geojson_geometry(Geometry) -> varchar
:noindex: true

Returns the GeoJSON encoding defined by the input geometry. GeoJSON output
preserves coordinates, including Z coordinates, but does not include Trino SRID
metadata.
:::

:::{function} from_geojson_geometry(varchar) -> SphericalGeography
Returns the spherical geography type object from the GeoJSON representation stripping non geometry key/values.
Feature and FeatureCollection are not supported.
:::

## Aggregations

:::{function} convex_hull_agg(Geometry) -> Geometry
Returns the minimum convex geometry that encloses all input geometries.
:::

:::{function} geometry_collect_agg(Geometry) -> Geometry
Returns a multi-geometry or geometry collection containing all input geometries.
The returned geometry preserves input Z coordinates and uses the non-zero SRID
from the input geometries. Inputs with different non-zero SRIDs fail.
:::

:::{function} geometry_union_agg(Geometry) -> Geometry
Returns a geometry that represents the point set union of all input geometries.
:::

## Spatial partitioning

These functions build and apply a KDB tree for spatial joins. They use the
two-dimensional envelope of each geometry, based on X and Y coordinates. SRID
and Z metadata do not affect partitioning.

Optimized spatial joins apply the same SRID compatibility rule before spatial
pruning. If the indexed side has no non-zero SRID, every probe SRID is allowed.
If it has one non-zero SRID, a probe must have SRID `0` or that SRID. If it has
multiple non-zero SRIDs, only probes with SRID `0` are allowed.

:::{function} spatial_partitioning(Geometry) -> varchar
Returns a serialized KDB tree partitioning for the input geometries. The
one-argument form is rewritten internally to
`spatial_partitioning(geometry, 100)`.
:::

:::{function} spatial_partitioning(Geometry, partition_count) -> varchar
:noindex: true

Returns a serialized KDB tree partitioning for the input geometries using the
specified partition count.
:::

:::{function} spatial_partitions(KdbTree, Geometry) -> array(integer)
Returns the IDs of spatial partitions whose extents intersect the
two-dimensional envelope of the input geometry. Returns `NULL` if the input
geometry is empty.
:::

:::{function} spatial_partitions(KdbTree, Geometry, distance) -> array(integer)
:noindex: true

Returns the IDs of spatial partitions whose extents intersect the
two-dimensional envelope of the input geometry expanded by `distance`. Returns
`NULL` if the input geometry is empty.
:::

## Bing tiles

These functions convert between geometries and
[Bing tiles](https://msdn.microsoft.com/library/bb259689.aspx).

:::{function} bing_tile(x, y, zoom_level) -> BingTile
Creates a Bing tile object from XY coordinates and a zoom level.
Zoom levels from 1 to 23 are supported.
:::

:::{function} bing_tile(quadKey) -> BingTile
:noindex: true

Creates a Bing tile object from a quadkey.
:::

:::{function} bing_tile_at(latitude, longitude, zoom_level) -> BingTile
Returns a Bing tile at a given zoom level containing a point at a given latitude
and longitude. Latitude must be within `[-85.05112878, 85.05112878]` range.
Longitude must be within `[-180, 180]` range. Zoom levels from 1 to 23 are supported.
:::

:::{function} bing_tiles_around(latitude, longitude, zoom_level) -> array(BingTile)
Returns a collection of Bing tiles that surround the point specified
by the latitude and longitude arguments at a given zoom level.
:::

:::{function} bing_tiles_around(latitude, longitude, zoom_level, radius_in_km) -> array(BingTile)
:noindex: true

Returns a minimum set of Bing tiles at specified zoom level that cover a circle of specified
radius in km around a specified (latitude, longitude) point.
:::

:::{function} bing_tile_coordinates(tile) -> row<x, y>
Returns the XY coordinates of a given Bing tile.
:::

:::{function} bing_tile_polygon(tile) -> Geometry
Returns the polygon representation of a given Bing tile as a 2D geometry with
SRID `0`.
:::

:::{function} bing_tile_quadkey(tile) -> varchar
Returns the quadkey of a given Bing tile.
:::

:::{function} bing_tile_zoom_level(tile) -> tinyint
Returns the zoom level of a given Bing tile.
:::

:::{function} geometry_to_bing_tiles(geometry, zoom_level) -> array(BingTile)
Returns the minimum set of Bing tiles that fully covers a given geometry at
a given zoom level. Zoom levels from 1 to 23 are supported. The calculation
uses the X and Y coordinates as longitude and latitude.
:::

## Encoded polylines

These functions convert between geometries and
[encoded polylines](https://developers.google.com/maps/documentation/utilities/polylinealgorithm).

:::{function} to_encoded_polyline(Geometry) -> varchar
Encodes a linestring or multipoint to a polyline. Encoded polylines are a 2D
format and only use the X and Y coordinates.
:::

:::{function} from_encoded_polyline(varchar) -> Geometry
Decodes a polyline to a 2D linestring with SRID `0`.
:::
