enum TrinoThriftBound {
  BELOW = 1;
  EXACTLY = 2;
  ABOVE = 3;
}

struct TrinoThriftSchemaTableName {
  1: string schemaName;
  2: string tableName;
}

struct TrinoThriftInteger {
  1: optional list<bool> nulls;
  2: optional list<i32> ints;
}

struct TrinoThriftBigint {
  1: optional list<bool> nulls;
  2: optional list<i64> longs;
}

struct TrinoThriftDouble {
  1: optional list<bool> nulls;
  2: optional list<double> doubles;
}

struct TrinoThriftVarchar {
  1: optional list<bool> nulls;
  2: optional list<i32> sizes;
  3: optional binary bytes;
}

struct TrinoThriftBoolean {
  1: optional list<bool> nulls;
  2: optional list<bool> booleans;
}

struct TrinoThriftDate {
  1: optional list<bool> nulls;
  2: optional list<i32> dates;
}

struct TrinoThriftTimestamp {
  1: optional list<bool> nulls;
  2: optional list<i64> timestamps;
}

struct TrinoThriftJson {
  1: optional list<bool> nulls;
  2: optional list<i32> sizes;
  3: optional binary bytes;
}

struct TrinoThriftHyperLogLog {
  1: optional list<bool> nulls;
  2: optional list<i32> sizes;
  3: optional binary bytes;
}

struct TrinoThriftBigintArray {
  1: optional list<bool> nulls;
  2: optional list<i32> sizes;
  3: optional TrinoThriftBigint values;
}

struct TrinoThriftId {
  1: binary id;
}

struct TrinoThriftTupleDomain {
  1: optional map<string, TrinoThriftDomain> domains;
}

struct TrinoThriftAllOrNoneValueSet {
  1: bool all;
}

struct TrinoThriftEquatableValueSet {
  1: bool inclusive;
  2: list<TrinoThriftBlock> values;
}

struct TrinoThriftRangeValueSet {
  1: list<TrinoThriftRange> ranges;
}

struct TrinoThriftNullableToken {
  1: optional TrinoThriftId token;
}

exception TrinoThriftServiceException {
  1: string message;
  2: bool retryable;
}

struct TrinoThriftSplitBatch {
  1: list<TrinoThriftSplit> splits;
  2: optional TrinoThriftId nextToken;
}

struct TrinoThriftSplit {
  1: TrinoThriftId splitId;
  2: list<TrinoThriftHostAddress> hosts;
}

struct TrinoThriftHostAddress {
  1: string host;
  2: i32 port;
}

struct TrinoThriftNullableColumnSet {
  1: optional set<string> columns;
}

struct TrinoThriftTableMetadata {
  1: TrinoThriftSchemaTableName schemaTableName;
  2: list<TrinoThriftColumnMetadata> columns;
  3: optional string comment;
  4: optional list<set<string>> indexableKeys;
}

struct TrinoThriftColumnMetadata {
  1: string name;
  2: string type;
  3: optional string comment;
  4: bool hidden;
}

struct TrinoThriftNullableSchemaName {
  1: optional string schemaName;
}

struct TrinoThriftPageResult {
  1: list<TrinoThriftBlock> columnBlocks;
  2: i32 rowCount;
  3: optional TrinoThriftId nextToken;
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

struct TrinoThriftValueSet {
  1: optional TrinoThriftAllOrNoneValueSet allOrNoneValueSet;
  2: optional TrinoThriftEquatableValueSet equatableValueSet;
  3: optional TrinoThriftRangeValueSet rangeValueSet;
}

struct TrinoThriftMarker {
  1: optional TrinoThriftBlock value;
  2: TrinoThriftBound bound;
}

struct TrinoThriftNullableTableMetadata {
  1: optional TrinoThriftTableMetadata tableMetadata;
}

struct TrinoThriftDomain {
  1: TrinoThriftValueSet valueSet;
  2: bool nullAllowed;
}

struct TrinoThriftRange {
  1: TrinoThriftMarker low;
  2: TrinoThriftMarker high;
}

service TrinoThriftService {
  TrinoThriftSplitBatch trinoGetIndexSplits(
      1: TrinoThriftSchemaTableName schemaTableName,
      2: list<string> indexColumnNames,
      3: list<string> outputColumnNames,
      4: TrinoThriftPageResult keys,
      5: TrinoThriftTupleDomain outputConstraint,
      6: i32 maxSplitCount,
      7: TrinoThriftNullableToken nextToken)
    throws (1: TrinoThriftServiceException ex1);

  TrinoThriftPageResult trinoGetRows(
      1: TrinoThriftId splitId,
      2: list<string> columns,
      3: i64 maxBytes,
      4: TrinoThriftNullableToken nextToken)
    throws (1: TrinoThriftServiceException ex1);

  TrinoThriftSplitBatch trinoGetSplits(
      1: TrinoThriftSchemaTableName schemaTableName,
      2: TrinoThriftNullableColumnSet desiredColumns,
      3: TrinoThriftTupleDomain outputConstraint,
      4: i32 maxSplitCount,
      5: TrinoThriftNullableToken nextToken)
    throws (1: TrinoThriftServiceException ex1);

  TrinoThriftNullableTableMetadata trinoGetTableMetadata(
      1: TrinoThriftSchemaTableName schemaTableName)
    throws (1: TrinoThriftServiceException ex1);

  list<string> trinoListSchemaNames()
    throws (1: TrinoThriftServiceException ex1);

  list<TrinoThriftSchemaTableName> trinoListTables(
      1: TrinoThriftNullableSchemaName schemaNameOrNull)
    throws (1: TrinoThriftServiceException ex1);
}
