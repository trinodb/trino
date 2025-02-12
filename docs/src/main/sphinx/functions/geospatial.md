# Geospatial functions

Trino Geospatial functions that begin with the `ST_` prefix support the SQL/MM specification
and are compliant with the Open Geospatial Consortium’s (OGC) OpenGIS Specifications.
As such, many Trino Geospatial functions require, or more accurately, assume that
geometries that are operated on are both simple and valid. For example, it does not
make sense to calculate the area of a polygon that has a hole defined outside of the
polygon, or to construct a polygon from a non-simple boundary line.

Trino Geospatial functions support the Well-Known Text (WKT) and Well-Known Binary (WKB) form of spatial objects:

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
straight line. That means calculations on geometries (areas, distances, lengths, intersections, etc)
can be calculated using cartesian mathematics and straight line vectors.

The basis for the `SphericalGeography` type is a sphere. The shortest path between two points on the
sphere is a great circle arc. That means that calculations on geographies (areas, distances,
lengths, intersections, etc) must be calculated on the sphere, using more complicated mathematics.
More accurate measurements that take the actual spheroidal shape of the world into account are not
supported.

Values returned by the measurement functions {func}`ST_Distance` and {func}`ST_Length` are in the unit of meters;
values returned by {func}`ST_Area` are in square meters.

Use {func}`to_spherical_geography()` function to convert a geometry object to geography object.

For example, `ST_Distance(ST_Point(-71.0882, 42.3607), ST_Point(-74.1197, 40.6976))` returns
`3.4577` in the unit of the passed-in values on the euclidean plane, while
`ST_Distance(to_spherical_geography(ST_Point(-71.0882, 42.3607)), to_spherical_geography(ST_Point(-74.1197, 40.6976)))`
returns `312822.179` in meters.

## Constructors

:::{function} ST_AsBinary(Geometry) -> varbinary
Returns the WKB representation of the geometry.
:::

:::{function} ST_AsText(Geometry) -> varchar
Returns the WKT representation of the geometry. For empty geometries,
`ST_AsText(ST_LineFromText('LINESTRING EMPTY'))` will produce `'MULTILINESTRING EMPTY'`
and `ST_AsText(ST_Polygon('POLYGON EMPTY'))` will produce `'MULTIPOLYGON EMPTY'`.
:::

:::{function} ST_GeometryFromText(varchar) -> Geometry
Returns a geometry type object from WKT representation.
:::

:::{function} ST_GeomFromBinary(varbinary) -> Geometry
Returns a geometry type object from WKB or EWKB representation.
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
Array elements must not be `NULL` or the same as the previous element.
The returned geometry may not be simple, e.g. may self-intersect or may contain
duplicate vertexes depending on the input.
:::

:::{function} ST_MultiPoint(array(Point)) -> MultiPoint
Returns a MultiPoint geometry object formed from the specified points. Returns `NULL` if input array is empty.
Array elements must not be `NULL` or empty.
The returned geometry may not be simple and may contain duplicate points if input array has duplicates.
:::

:::{function} ST_Point(lon: double, lat: double) -> Point
Returns a geometry type point object with the given coordinate values.
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
degrees to construct the shape of the `SphericalGeography` result.
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
is chosen arbitrarily.
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

:::{function} ST_Difference(first: Geometry, second: Geometry) -> Geometry
Returns the geometry value that represents the point set difference of the given geometries.
:::

:::{function} ST_Envelope(Geometry) -> Geometry
Returns the bounding rectangular polygon of a geometry.
:::

:::{function} ST_EnvelopeAsPts(Geometry) -> array(Geometry)
Returns an array of two points: the lower left and upper right corners of the bounding
rectangular polygon of a geometry. Returns `NULL` if input geometry is empty.
:::

:::{function} ST_ExteriorRing(Geometry) -> Geometry
Returns a line string representing the exterior ring of the input polygon.
:::

:::{function} ST_Intersection(first: Geometry, second: Geometry) -> Geometry
Returns the geometry value that represents the point set intersection of two geometries.
:::

:::{function} ST_SymDifference(first: Geometry, second: Geometry) -> Geometry
Returns the geometry value that represents the point set symmetric difference of two geometries.
:::

:::{function} ST_Union(first: Geometry, second: Geometry) -> Geometry
Returns a geometry that represents the point set union of the input geometries.

See also:  {func}`geometry_union`, {func}`geometry_union_agg`
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

:::{function} ST_CoordDim(Geometry) -> bigint
Returns the coordinate dimension of the geometry.
:::

:::{function} ST_Dimension(Geometry) -> bigint
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
Returns `true` if the linestring's start and end points are coincident.
:::

:::{function} ST_IsEmpty(Geometry) -> boolean
Returns `true` if this Geometry is an empty geometrycollection, polygon, point etc.
:::

:::{function} ST_IsSimple(Geometry) -> boolean
Returns `true` if this Geometry has no anomalous geometric points, such as self intersection or self tangency.
:::

:::{function} ST_IsRing(Geometry) -> boolean
Returns `true` if and only if the line is closed and simple.
:::

:::{function} ST_IsValid(Geometry) -> boolean
Returns `true` if and only if the input geometry is well formed.
Use {func}`geometry_invalid_reason` to determine why the geometry is not well formed.
:::

:::{function} ST_Length(Geometry) -> double
Returns the length of a linestring or multi-linestring using Euclidean measurement on a
two dimensional plane (based on spatial ref) in projected units.
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

:::{function} ST_XMax(Geometry) -> double
Returns X maxima of a bounding box of a geometry.
:::

:::{function} ST_YMax(Geometry) -> double
Returns Y maxima of a bounding box of a geometry.
:::

:::{function} ST_XMin(Geometry) -> double
Returns X minima of a bounding box of a geometry.
:::

:::{function} ST_YMin(Geometry) -> double
Returns Y minima of a bounding box of a geometry.
:::

:::{function} ST_StartPoint(Geometry) -> point
Returns the first point of a LineString geometry as a Point.
This is a shortcut for `ST_PointN(geometry, 1)`.
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
Returns the X coordinate of the point.
:::

:::{function} ST_Y(Point) -> double
Returns the Y coordinate of the point.
:::

:::{function} ST_InteriorRings(Geometry) -> array(Geometry)
Returns an array of all interior rings found in the input geometry, or an empty
array if the polygon has no interior rings. Returns `NULL` if the input geometry
is empty. The input geometry must be a polygon.
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
must be between 0 and 1, inclusive.
:::

:::{function} line_interpolate_points(LineString, double, repeated) -> array(Geometry)
Returns an array of Points interpolated along a LineString. The fraction must be
between 0 and 1, inclusive.
:::

:::{function} line_locate_point(LineString, Point) -> double
Returns a float between 0 and 1 representing the location of the closest point on
the LineString to the given Point, as a fraction of total 2d line length.

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
Returns the GeoJSON encoded defined by the input spherical geography.
:::

:::{function} from_geojson_geometry(varchar) -> SphericalGeography
Returns the spherical geography type object from the GeoJSON representation stripping non geometry key/values.
Feature and FeatureCollection are not supported.
:::

## Aggregations

:::{function} convex_hull_agg(Geometry) -> Geometry
Returns the minimum convex geometry that encloses all input geometries.
:::

:::{function} geometry_union_agg(Geometry) -> Geometry
Returns a geometry that represents the point set union of all input geometries.
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
Returns the polygon representation of a given Bing tile.
:::

:::{function} bing_tile_quadkey(tile) -> varchar
Returns the quadkey of a given Bing tile.
:::

:::{function} bing_tile_zoom_level(tile) -> tinyint
Returns the zoom level of a given Bing tile.
:::

:::{function} geometry_to_bing_tiles(geometry, zoom_level) -> array(BingTile)
Returns the minimum set of Bing tiles that fully covers a given geometry at
a given zoom level. Zoom levels from 1 to 23 are supported.
:::

## Encoded polylines

These functions convert between geometries and
[encoded polylines](https://developers.google.com/maps/documentation/utilities/polylinealgorithm).

:::{function} to_encoded_polyline(Geometry) -> varchar
Encodes a linestring or multipoint to a polyline.
:::

:::{function} from_encoded_polyline(varchar) -> Geometry
Decodes a polyline to a linestring.
:::
