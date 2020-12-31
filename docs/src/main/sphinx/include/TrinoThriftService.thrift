enum TrinoThriftBound {
  BELOW = 1;
  EXACTLY = 2;
  ABOVE = 3;
}

exception TrinoThriftServiceException {
  1: string message;
  2: bool retryable;
}

struct TrinoThriftNullableSchemaName {
  1: optional string schemaName;
}

struct TrinoThriftSchemaTableName {
  1: string schemaName;
  2: string tableName;
}

struct TrinoThriftTableMetadata {
  1: TrinoThriftSchemaTableName schemaTableName;
  2: list<TrinoThriftColumnMetadata> columns;
  3: optional string comment;

  /**
   * Returns a list of key sets which can be used for index lookups.
   * The list is expected to have only unique key sets.
   * {@code set<set<string>>} is not used here because some languages (like php) don't support it.
   */
  4: optional list<set<string>> indexableKeys;
}

struct TrinoThriftColumnMetadata {
  1: string name;
  2: string type;
  3: optional string comment;
  4: bool hidden;
}

struct TrinoThriftNullableColumnSet {
  1: optional set<string> columns;
}

struct TrinoThriftTupleDomain {
  /**
   * Return a map of column names to constraints.
   */
  1: optional map<string, TrinoThriftDomain> domains;
}

/**
 * Set that either includes all values, or excludes all values.
 */
struct TrinoThriftAllOrNoneValueSet {
  1: bool all;
}

/**
 * A set containing values that are uniquely identifiable.
 * Assumes an infinite number of possible values. The values may be collectively included
 * or collectively excluded.
 * This structure is used with comparable, but not orderable types like "json", "map".
 */
struct TrinoThriftEquatableValueSet {
  1: bool inclusive;
  2: list<TrinoThriftBlock> values;
}

/**
 * Elements of {@code nulls} array determine if a value for a corresponding row is null.
 * Elements of {@code ints} array are values for each row. If row is null then value is ignored.
 */
struct TrinoThriftInteger {
  1: optional list<bool> nulls;
  2: optional list<i32> ints;
}

/**
 * Elements of {@code nulls} array determine if a value for a corresponding row is null.
 * Elements of {@code longs} array are values for each row. If row is null then value is ignored.
 */
struct TrinoThriftBigint {
  1: optional list<bool> nulls;
  2: optional list<i64> longs;
}

/**
 * Elements of {@code nulls} array determine if a value for a corresponding row is null.
 * Elements of {@code doubles} array are values for each row. If row is null then value is ignored.
 */
struct TrinoThriftDouble {
  1: optional list<bool> nulls;
  2: optional list<double> doubles;
}

/**
 * Elements of {@code nulls} array determine if a value for a corresponding row is null.
 * Each elements of {@code sizes} array contains the length in bytes for the corresponding element.
 * If row is null then the corresponding element in {@code sizes} is ignored.
 * {@code bytes} array contains UTF-8 encoded byte values.
 * Values for all rows are written to {@code bytes} array one after another.
 * The total number of bytes must be equal to the sum of all sizes.
 */
struct TrinoThriftVarchar {
  1: optional list<bool> nulls;
  2: optional list<i32> sizes;
  3: optional binary bytes;
}

/**
 * Elements of {@code nulls} array determine if a value for a corresponding row is null.
 * Elements of {@code booleans} array are values for each row. If row is null then value is ignored.
 */
struct TrinoThriftBoolean {
  1: optional list<bool> nulls;
  2: optional list<bool> booleans;
}

/**
 * Elements of {@code nulls} array determine if a value for a corresponding row is null.
 * Elements of {@code dates} array are date values for each row represented as the number
 * of days passed since 1970-01-01.
 * If row is null then value is ignored.
 */
struct TrinoThriftDate {
  1: optional list<bool> nulls;
  2: optional list<i32> dates;
}

/**
 * Elements of {@code nulls} array determine if a value for a corresponding row is null.
 * Elements of {@code timestamps} array are values for each row represented as the number
 * of milliseconds passed since 1970-01-01T00:00:00 UTC.
 * If row is null then value is ignored.
 */
struct TrinoThriftTimestamp {
  1: optional list<bool> nulls;
  2: optional list<i64> timestamps;
}

/**
 * Elements of {@code nulls} array determine if a value for a corresponding row is null.
 * Each elements of {@code sizes} array contains the length in bytes for the corresponding element.
 * If row is null then the corresponding element in {@code sizes} is ignored.
 * {@code bytes} array contains UTF-8 encoded byte values for string representation of json.
 * Values for all rows are written to {@code bytes} array one after another.
 * The total number of bytes must be equal to the sum of all sizes.
 */
struct TrinoThriftJson {
  1: optional list<bool> nulls;
  2: optional list<i32> sizes;
  3: optional binary bytes;
}

/**
 * Elements of {@code nulls} array determine if a value for a corresponding row is null.
 * Each elements of {@code sizes} array contains the length in bytes for the corresponding element.
 * If row is null then the corresponding element in {@code sizes} is ignored.
 * {@code bytes} array contains encoded byte values for HyperLogLog representation as defined in
 * Airlift specification: href="https://github.com/airlift/airlift/blob/master/stats/docs/hll.md
 * Values for all rows are written to {@code bytes} array one after another.
 * The total number of bytes must be equal to the sum of all sizes.
 */
struct TrinoThriftHyperLogLog {
  1: optional list<bool> nulls;
  2: optional list<i32> sizes;
  3: optional binary bytes;
}

/**
 * Elements of {@code nulls} array determine if a value for a corresponding row is null.
 * Each elements of {@code sizes} array contains the number of elements in the corresponding values array.
 * If row is null then the corresponding element in {@code sizes} is ignored.
 * {@code values} is a bigint block containing array elements one after another for all rows.
 * The total number of elements in bigint block must be equal to the sum of all sizes.
 */
struct TrinoThriftBigintArray {
  1: optional list<bool> nulls;
  2: optional list<i32> sizes;
  3: optional TrinoThriftBigint values;
}

/**
 * A set containing zero or more Ranges of the same type over a continuous space of possible values.
 * Ranges are coalesced into the most compact representation of non-overlapping Ranges.
 * This structure is used with comparable and orderable types like bigint, integer, double, varchar, etc.
 */
struct TrinoThriftRangeValueSet {
  1: list<TrinoThriftRange> ranges;
}

struct TrinoThriftId {
  1: binary id;
}

struct TrinoThriftSplitBatch {
  1: list<TrinoThriftSplit> splits;
  2: optional TrinoThriftId nextToken;
}

struct TrinoThriftSplit {
  /**
   * Encodes all the information needed to identify a batch of rows to return to Trino.
   * For a basic scan, includes schema name, table name, and output constraint.
   * For an index scan, includes schema name, table name, set of keys to lookup and output constraint.
   */
  1: TrinoThriftId splitId;

  /**
   * Identifies the set of hosts on which the rows are available. If empty, then the rows
   * are expected to be available on any host. The hosts in this list may be independent
   * from the hosts used to serve metadata requests.
   */
  2: list<TrinoThriftHostAddress> hosts;
}

struct TrinoThriftHostAddress {
  1: string host;
  2: i32 port;
}

struct TrinoThriftPageResult {
  /**
   * Returns data in a columnar format.
   * Columns in this list must be in the order they were requested by the engine.
   */
  1: list<TrinoThriftBlock> columnBlocks;

  2: i32 rowCount;
  3: optional TrinoThriftId nextToken;
}

struct TrinoThriftNullableTableMetadata {
  1: optional TrinoThriftTableMetadata tableMetadata;
}

struct TrinoThriftValueSet {
  1: optional TrinoThriftAllOrNoneValueSet allOrNoneValueSet;
  2: optional TrinoThriftEquatableValueSet equatableValueSet;
  3: optional TrinoThriftRangeValueSet rangeValueSet;
}

struct TrinoThriftBlock {
  1: optional TrinoThriftInteger integerData;
  2: optional TrinoThriftBigint bigintData;
  3: optional TrinoThriftDouble doubleData;
  4: optional TrinoThriftVarchar varcharData;
  5: optional TrinoThriftBoolean booleanData;
  6: optional TrinoThriftDate dateData;
  7: optional TrinoThriftTimestamp timestampData;
  8: optional TrinoThriftJson jsonData;
  9: optional TrinoThriftHyperLogLog hyperLogLogData;
  10: optional TrinoThriftBigintArray bigintArrayData;
}

/**
 * LOWER UNBOUNDED is specified with an empty value and an ABOVE bound
 * UPPER UNBOUNDED is specified with an empty value and a BELOW bound
 */
struct TrinoThriftMarker {
  1: optional TrinoThriftBlock value;
  2: TrinoThriftBound bound;
}

struct TrinoThriftNullableToken {
  1: optional TrinoThriftId token;
}

struct TrinoThriftDomain {
  1: TrinoThriftValueSet valueSet;
  2: bool nullAllowed;
}

struct TrinoThriftRange {
  1: TrinoThriftMarker low;
  2: TrinoThriftMarker high;
}

/**
 * Trino Thrift service definition.
 * This thrift service needs to be implemented in order to be used with Thrift Connector.
 */
service TrinoThriftService {
  /**
   * Returns available schema names.
   */
  list<string> trinoListSchemaNames()
    throws (1: TrinoThriftServiceException ex1);

  /**
   * Returns tables for the given schema name.
   *
   * @param schemaNameOrNull a structure containing schema name or {@literal null}
   * @return a list of table names with corresponding schemas. If schema name is null then returns
   * a list of tables for all schemas. Returns an empty list if a schema does not exist
   */
  list<TrinoThriftSchemaTableName> trinoListTables(
      1: TrinoThriftNullableSchemaName schemaNameOrNull)
    throws (1: TrinoThriftServiceException ex1);

  /**
   * Returns metadata for a given table.
   *
   * @param schemaTableName schema and table name
   * @return metadata for a given table, or a {@literal null} value inside if it does not exist
   */
  TrinoThriftNullableTableMetadata trinoGetTableMetadata(
      1: TrinoThriftSchemaTableName schemaTableName)
    throws (1: TrinoThriftServiceException ex1);

  /**
   * Returns a batch of splits.
   *
   * @param schemaTableName schema and table name
   * @param desiredColumns a superset of columns to return; empty set means "no columns", {@literal null} set means "all columns"
   * @param outputConstraint constraint on the returned data
   * @param maxSplitCount maximum number of splits to return
   * @param nextToken token from a previous split batch or {@literal null} if it is the first call
   * @return a batch of splits
   */
  TrinoThriftSplitBatch trinoGetSplits(
      1: TrinoThriftSchemaTableName schemaTableName,
      2: TrinoThriftNullableColumnSet desiredColumns,
      3: TrinoThriftTupleDomain outputConstraint,
      4: i32 maxSplitCount,
      5: TrinoThriftNullableToken nextToken)
    throws (1: TrinoThriftServiceException ex1);

  /**
   * Returns a batch of index splits for the given batch of keys.
   * This method is called if index join strategy is chosen for a query.
   *
   * @param schemaTableName schema and table name
   * @param indexColumnNames specifies columns and their order for keys
   * @param outputColumnNames a list of column names to return
   * @param keys keys for which records need to be returned; includes only unique and non-null values
   * @param outputConstraint constraint on the returned data
   * @param maxSplitCount maximum number of splits to return
   * @param nextToken token from a previous split batch or {@literal null} if it is the first call
   * @return a batch of splits
   */
  TrinoThriftSplitBatch trinoGetIndexSplits(
      1: TrinoThriftSchemaTableName schemaTableName,
      2: list<string> indexColumnNames,
      3: list<string> outputColumnNames,
      4: TrinoThriftPageResult keys,
      5: TrinoThriftTupleDomain outputConstraint,
      6: i32 maxSplitCount,
      7: TrinoThriftNullableToken nextToken)
    throws (1: TrinoThriftServiceException ex1);

  /**
   * Returns a batch of rows for the given split.
   *
   * @param splitId split id as returned in split batch
   * @param columns a list of column names to return
   * @param maxBytes maximum size of returned data in bytes
   * @param nextToken token from a previous batch or {@literal null} if it is the first call
   * @return a batch of table data
   */
  TrinoThriftPageResult trinoGetRows(
      1: TrinoThriftId splitId,
      2: list<string> columns,
      3: i64 maxBytes,
      4: TrinoThriftNullableToken nextToken)
    throws (1: TrinoThriftServiceException ex1);
}
