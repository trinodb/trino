# EXPLAIN

## Synopsis

```text
EXPLAIN [ ( option [, ...] ) ] statement
```

where `option` can be one of:

```text
FORMAT { TEXT | GRAPHVIZ | JSON }
TYPE { LOGICAL | DISTRIBUTED | VALIDATE | IO }
```

## Description

Show the logical or distributed execution plan of a statement, or validate the statement.
The distributed plan is shown by default. Each plan fragment of the distributed plan is executed by
a single or multiple Trino nodes. Fragments separation represent the data exchange between Trino nodes.
Fragment type specifies how the fragment is executed by Trino nodes and how the data is
distributed between fragments:

`SINGLE`

: Fragment is executed on a single node.

`HASH`

: Fragment is executed on a fixed number of nodes with the input data
  distributed using a hash function.

`ROUND_ROBIN`

: Fragment is executed on a fixed number of nodes with the input data
  distributed in a round-robin fashion.

`BROADCAST`

: Fragment is executed on a fixed number of nodes with the input data
  broadcasted to all nodes.

`SOURCE`

: Fragment is executed on nodes where input splits are accessed.

## Examples

### EXPLAIN (TYPE LOGICAL)

Process the supplied query statement and create a logical plan in text format:

```
EXPLAIN (TYPE LOGICAL) SELECT regionkey, count(*) FROM nation GROUP BY 1;
```

```text
                                                   Query Plan
-----------------------------------------------------------------------------------------------------------------
 Trino version: version
 Output[regionkey, _col1]
 │   Layout: [regionkey:bigint, count:bigint]
 │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?}
 │   _col1 := count
 └─ RemoteExchange[GATHER]
    │   Layout: [regionkey:bigint, count:bigint]
    │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?}
    └─ Aggregate(FINAL)[regionkey]
       │   Layout: [regionkey:bigint, count:bigint]
       │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?}
       │   count := count("count_8")
       └─ LocalExchange[HASH][$hashvalue] ("regionkey")
          │   Layout: [regionkey:bigint, count_8:bigint, $hashvalue:bigint]
          │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?}
          └─ RemoteExchange[REPARTITION][$hashvalue_9]
             │   Layout: [regionkey:bigint, count_8:bigint, $hashvalue_9:bigint]
             │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?}
             └─ Project[]
                │   Layout: [regionkey:bigint, count_8:bigint, $hashvalue_10:bigint]
                │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?}
                │   $hashvalue_10 := "combine_hash"(bigint '0', COALESCE("$operator$hash_code"("regionkey"), 0))
                └─ Aggregate(PARTIAL)[regionkey]
                   │   Layout: [regionkey:bigint, count_8:bigint]
                   │   count_8 := count(*)
                   └─ TableScan[tpch:nation:sf0.01]
                          Layout: [regionkey:bigint]
                          Estimates: {rows: 25 (225B), cpu: 225, memory: 0B, network: 0B}
                          regionkey := tpch:regionkey
```

### EXPLAIN (TYPE LOGICAL, FORMAT JSON)

:::{warning}
The output format is not guaranteed to be backward compatible across Trino versions.
:::

Process the supplied query statement and create a logical plan in JSON format:

```
EXPLAIN (TYPE LOGICAL, FORMAT JSON) SELECT regionkey, count(*) FROM nation GROUP BY 1;
```

```json
{
   "id": "9",
   "name": "Output",
   "descriptor": {
      "columnNames": "[regionkey, _col1]"
   },
   "outputs": [
      {
         "symbol": "regionkey",
         "type": "bigint"
      },
      {
         "symbol": "count",
         "type": "bigint"
      }
   ],
   "details": [
      "_col1 := count"
   ],
   "estimates": [
      {
         "outputRowCount": "NaN",
         "outputSizeInBytes": "NaN",
         "cpuCost": "NaN",
         "memoryCost": "NaN",
         "networkCost": "NaN"
      }
   ],
   "children": [
      {
         "id": "145",
         "name": "RemoteExchange",
         "descriptor": {
            "type": "GATHER",
            "isReplicateNullsAndAny": "",
            "hashColumn": ""
         },
         "outputs": [
            {
               "symbol": "regionkey",
               "type": "bigint"
            },
            {
               "symbol": "count",
               "type": "bigint"
            }
         ],
         "details": [

         ],
         "estimates": [
            {
               "outputRowCount": "NaN",
               "outputSizeInBytes": "NaN",
               "cpuCost": "NaN",
               "memoryCost": "NaN",
               "networkCost": "NaN"
            }
         ],
         "children": [
            {
               "id": "4",
               "name": "Aggregate",
               "descriptor": {
                  "type": "FINAL",
                  "keys": "[regionkey]",
                  "hash": ""
               },
               "outputs": [
                  {
                     "symbol": "regionkey",
                     "type": "bigint"
                  },
                  {
                     "symbol": "count",
                     "type": "bigint"
                  }
               ],
               "details": [
                  "count := count(\"count_0\")"
               ],
               "estimates": [
                  {
                     "outputRowCount": "NaN",
                     "outputSizeInBytes": "NaN",
                     "cpuCost": "NaN",
                     "memoryCost": "NaN",
                     "networkCost": "NaN"
                  }
               ],
               "children": [
                  {
                     "id": "194",
                     "name": "LocalExchange",
                     "descriptor": {
                        "partitioning": "HASH",
                        "isReplicateNullsAndAny": "",
                        "hashColumn": "[$hashvalue]",
                        "arguments": "[\"regionkey\"]"
                     },
                     "outputs": [
                        {
                           "symbol": "regionkey",
                           "type": "bigint"
                        },
                        {
                           "symbol": "count_0",
                           "type": "bigint"
                        },
                        {
                           "symbol": "$hashvalue",
                           "type": "bigint"
                        }
                     ],
                     "details":[],
                     "estimates": [
                        {
                           "outputRowCount": "NaN",
                           "outputSizeInBytes": "NaN",
                           "cpuCost": "NaN",
                           "memoryCost": "NaN",
                           "networkCost": "NaN"
                        }
                     ],
                     "children": [
                        {
                           "id": "200",
                           "name": "RemoteExchange",
                           "descriptor": {
                              "type": "REPARTITION",
                              "isReplicateNullsAndAny": "",
                              "hashColumn": "[$hashvalue_1]"
                           },
                           "outputs": [
                              {
                                 "symbol": "regionkey",
                                 "type": "bigint"
                              },
                              {
                                 "symbol": "count_0",
                                 "type": "bigint"
                              },
                              {
                                 "symbol": "$hashvalue_1",
                                 "type": "bigint"
                              }
                           ],
                           "details":[],
                           "estimates": [
                              {
                                 "outputRowCount": "NaN",
                                 "outputSizeInBytes": "NaN",
                                 "cpuCost": "NaN",
                                 "memoryCost": "NaN",
                                 "networkCost": "NaN"
                              }
                           ],
                           "children": [
                              {
                                 "id": "226",
                                 "name": "Project",
                                 "descriptor": {}
                                 "outputs": [
                                    {
                                       "symbol": "regionkey",
                                       "type": "bigint"
                                    },
                                    {
                                       "symbol": "count_0",
                                       "type": "bigint"
                                    },
                                    {
                                       "symbol": "$hashvalue_2",
                                       "type": "bigint"
                                    }
                                 ],
                                 "details": [
                                    "$hashvalue_2 := combine_hash(bigint '0', COALESCE(\"$operator$hash_code\"(\"regionkey\"), 0))"
                                 ],
                                 "estimates": [
                                    {
                                       "outputRowCount": "NaN",
                                       "outputSizeInBytes": "NaN",
                                       "cpuCost": "NaN",
                                       "memoryCost": "NaN",
                                       "networkCost": "NaN"
                                    }
                                 ],
                                 "children": [
                                    {
                                       "id": "198",
                                       "name": "Aggregate",
                                       "descriptor": {
                                          "type": "PARTIAL",
                                          "keys": "[regionkey]",
                                          "hash": ""
                                       },
                                       "outputs": [
                                          {
                                             "symbol": "regionkey",
                                             "type": "bigint"
                                          },
                                          {
                                             "symbol": "count_0",
                                             "type": "bigint"
                                          }
                                       ],
                                       "details": [
                                          "count_0 := count(*)"
                                       ],
                                       "estimates":[],
                                       "children": [
                                          {
                                             "id": "0",
                                             "name": "TableScan",
                                             "descriptor": {
                                                "table": "hive:tpch_sf1_orc_part:nation"
                                             },
                                             "outputs": [
                                                {
                                                   "symbol": "regionkey",
                                                   "type": "bigint"
                                                }
                                             ],
                                             "details": [
                                                "regionkey := regionkey:bigint:REGULAR"
                                             ],
                                             "estimates": [
                                                {
                                                   "outputRowCount": 25,
                                                   "outputSizeInBytes": 225,
                                                   "cpuCost": 225,
                                                   "memoryCost": 0,
                                                   "networkCost": 0
                                                }
                                             ],
                                             "children": []
                                          }
                                       ]
                                    }
                                 ]
                              }
                           ]
                        }
                     ]
                  }
               ]
            }
         ]
      }
   ]
}
```

### EXPLAIN (TYPE DISTRIBUTED)

Process the supplied query statement and create a distributed plan in text
format. The distributed plan splits the logical plan into stages, and therefore
explicitly shows the data exchange between workers:

```
EXPLAIN (TYPE DISTRIBUTED) SELECT regionkey, count(*) FROM nation GROUP BY 1;
```

```text
                                              Query Plan
------------------------------------------------------------------------------------------------------
 Trino version: version
 Fragment 0 [SINGLE]
     Output layout: [regionkey, count]
     Output partitioning: SINGLE []
     Output[regionkey, _col1]
     │   Layout: [regionkey:bigint, count:bigint]
     │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?}
     │   _col1 := count
     └─ RemoteSource[1]
            Layout: [regionkey:bigint, count:bigint]

 Fragment 1 [HASH]
     Output layout: [regionkey, count]
     Output partitioning: SINGLE []
     Aggregate(FINAL)[regionkey]
     │   Layout: [regionkey:bigint, count:bigint]
     │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?}
     │   count := count("count_8")
     └─ LocalExchange[HASH][$hashvalue] ("regionkey")
        │   Layout: [regionkey:bigint, count_8:bigint, $hashvalue:bigint]
        │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?}
        └─ RemoteSource[2]
               Layout: [regionkey:bigint, count_8:bigint, $hashvalue_9:bigint]

 Fragment 2 [SOURCE]
     Output layout: [regionkey, count_8, $hashvalue_10]
     Output partitioning: HASH [regionkey][$hashvalue_10]
     Project[]
     │   Layout: [regionkey:bigint, count_8:bigint, $hashvalue_10:bigint]
     │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: ?}
     │   $hashvalue_10 := "combine_hash"(bigint '0', COALESCE("$operator$hash_code"("regionkey"), 0))
     └─ Aggregate(PARTIAL)[regionkey]
        │   Layout: [regionkey:bigint, count_8:bigint]
        │   count_8 := count(*)
        └─ TableScan[tpch:nation:sf0.01, grouped = false]
               Layout: [regionkey:bigint]
               Estimates: {rows: 25 (225B), cpu: 225, memory: 0B, network: 0B}
               regionkey := tpch:regionkey
```

### EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON)

:::{warning}
The output format is not guaranteed to be backward compatible across Trino versions.
:::

Process the supplied query statement and create a distributed plan in JSON
format. The distributed plan splits the logical plan into stages, and therefore
explicitly shows the data exchange between workers:

```
EXPLAIN (TYPE DISTRIBUTED, FORMAT JSON) SELECT regionkey, count(*) FROM nation GROUP BY 1;
```

```json
{
   "0" : {
      "id" : "9",
      "name" : "Output",
      "descriptor" : {
         "columnNames" : "[regionkey, _col1]"
      },
      "outputs" : [ {
         "symbol" : "regionkey",
         "type" : "bigint"
      }, {
         "symbol" : "count",
         "type" : "bigint"
      } ],
      "details" : [ "_col1 := count" ],
      "estimates" : [ {
         "outputRowCount" : "NaN",
         "outputSizeInBytes" : "NaN",
         "cpuCost" : "NaN",
         "memoryCost" : "NaN",
         "networkCost" : "NaN"
      } ],
      "children" : [ {
         "id" : "145",
         "name" : "RemoteSource",
         "descriptor" : {
            "sourceFragmentIds" : "[1]"
         },
         "outputs" : [ {
            "symbol" : "regionkey",
            "type" : "bigint"
         }, {
            "symbol" : "count",
            "type" : "bigint"
         } ],
         "details" : [ ],
         "estimates" : [ ],
         "children" : [ ]
      } ]
   },
   "1" : {
      "id" : "4",
      "name" : "Aggregate",
      "descriptor" : {
         "type" : "FINAL",
         "keys" : "[regionkey]",
         "hash" : "[]"
      },
      "outputs" : [ {
         "symbol" : "regionkey",
         "type" : "bigint"
      }, {
         "symbol" : "count",
         "type" : "bigint"
      } ],
      "details" : [ "count := count(\"count_0\")" ],
      "estimates" : [ {
         "outputRowCount" : "NaN",
         "outputSizeInBytes" : "NaN",
         "cpuCost" : "NaN",
         "memoryCost" : "NaN",
         "networkCost" : "NaN"
      } ],
      "children" : [ {
         "id" : "194",
         "name" : "LocalExchange",
         "descriptor" : {
            "partitioning" : "SINGLE",
            "isReplicateNullsAndAny" : "",
            "hashColumn" : "[]",
            "arguments" : "[]"
         },
         "outputs" : [ {
            "symbol" : "regionkey",
            "type" : "bigint"
         }, {
            "symbol" : "count_0",
            "type" : "bigint"
         } ],
         "details" : [ ],
         "estimates" : [ {
            "outputRowCount" : "NaN",
            "outputSizeInBytes" : "NaN",
            "cpuCost" : "NaN",
            "memoryCost" : "NaN",
            "networkCost" : "NaN"
         } ],
         "children" : [ {
            "id" : "227",
            "name" : "Project",
            "descriptor" : { },
            "outputs" : [ {
               "symbol" : "regionkey",
               "type" : "bigint"
            }, {
               "symbol" : "count_0",
               "type" : "bigint"
            } ],
            "details" : [ ],
            "estimates" : [ {
               "outputRowCount" : "NaN",
               "outputSizeInBytes" : "NaN",
               "cpuCost" : "NaN",
               "memoryCost" : "NaN",
               "networkCost" : "NaN"
            } ],
            "children" : [ {
               "id" : "200",
               "name" : "RemoteSource",
               "descriptor" : {
                  "sourceFragmentIds" : "[2]"
               },
               "outputs" : [ {
                  "symbol" : "regionkey",
                  "type" : "bigint"
               }, {
                  "symbol" : "count_0",
                  "type" : "bigint"
               }, {
                  "symbol" : "$hashvalue",
                  "type" : "bigint"
               } ],
               "details" : [ ],
               "estimates" : [ ],
               "children" : [ ]
            } ]
         } ]
      } ]
   },
   "2" : {
      "id" : "226",
      "name" : "Project",
      "descriptor" : { },
      "outputs" : [ {
         "symbol" : "regionkey",
         "type" : "bigint"
      }, {
         "symbol" : "count_0",
         "type" : "bigint"
      }, {
         "symbol" : "$hashvalue_1",
         "type" : "bigint"
      } ],
      "details" : [ "$hashvalue_1 := combine_hash(bigint '0', COALESCE(\"$operator$hash_code\"(\"regionkey\"), 0))" ],
      "estimates" : [ {
         "outputRowCount" : "NaN",
         "outputSizeInBytes" : "NaN",
         "cpuCost" : "NaN",
         "memoryCost" : "NaN",
         "networkCost" : "NaN"
      } ],
      "children" : [ {
         "id" : "198",
         "name" : "Aggregate",
         "descriptor" : {
            "type" : "PARTIAL",
            "keys" : "[regionkey]",
            "hash" : "[]"
         },
         "outputs" : [ {
            "symbol" : "regionkey",
            "type" : "bigint"
         }, {
            "symbol" : "count_0",
            "type" : "bigint"
         } ],
         "details" : [ "count_0 := count(*)" ],
         "estimates" : [ ],
         "children" : [ {
            "id" : "0",
            "name" : "TableScan",
            "descriptor" : {
               "table" : "tpch:tiny:nation"
            },
            "outputs" : [ {
               "symbol" : "regionkey",
               "type" : "bigint"
            } ],
            "details" : [ "regionkey := tpch:regionkey" ],
            "estimates" : [ {
               "outputRowCount" : 25.0,
               "outputSizeInBytes" : 225.0,
               "cpuCost" : 225.0,
               "memoryCost" : 0.0,
               "networkCost" : 0.0
            } ],
            "children" : [ ]
         } ]
      } ]
   }
}
```

### EXPLAIN (TYPE VALIDATE)

Validate the supplied query statement for syntactical and semantic correctness.
Returns true if the statement is valid:

```
EXPLAIN (TYPE VALIDATE) SELECT regionkey, count(*) FROM nation GROUP BY 1;
```

```text
 Valid
-------
 true
```

If the statement is not correct because a syntax error, such as an unknown
keyword, is found the error message details the problem:

```
EXPLAIN (TYPE VALIDATE) SELET 1=0;
```

```text
Query 20220929_234840_00001_vjwxj failed: line 1:25: mismatched input 'SELET'.
Expecting: 'ALTER', 'ANALYZE', 'CALL', 'COMMENT', 'COMMIT', 'CREATE',
'DEALLOCATE', 'DELETE', 'DENY', 'DESC', 'DESCRIBE', 'DROP', 'EXECUTE',
'EXPLAIN', 'GRANT', 'INSERT', 'MERGE', 'PREPARE', 'REFRESH', 'RESET',
'REVOKE', 'ROLLBACK', 'SET', 'SHOW', 'START', 'TRUNCATE', 'UPDATE', 'USE',
<query>
```

Similarly if semantic issues are detected, such as an invalid object name
`nations` instead of `nation`, the error message returns useful
information:

```
EXPLAIN(TYPE VALIDATE) SELECT * FROM tpch.tiny.nations;
```

```text
Query 20220929_235059_00003_vjwxj failed: line 1:15: Table 'tpch.tiny.nations' does not exist
SELECT * FROM tpch.tiny.nations
```

### EXPLAIN (TYPE IO)

Process the supplied query statement and create a plan with input and output
details about the accessed objects in JSON format:

```
EXPLAIN (TYPE IO, FORMAT JSON) INSERT INTO test_lineitem
SELECT * FROM lineitem WHERE shipdate = '2020-02-01' AND quantity > 10;
```

```text
            Query Plan
-----------------------------------
{
   inputTableColumnInfos: [
      {
         table: {
            catalog: "hive",
            schemaTable: {
               schema: "tpch",
               table: "test_orders"
            }
         },
         columnConstraints: [
            {
               columnName: "orderkey",
               type: "bigint",
               domain: {
                  nullsAllowed: false,
                  ranges: [
                     {
                        low: {
                           value: "1",
                           bound: "EXACTLY"
                        },
                        high: {
                           value: "1",
                           bound: "EXACTLY"
                        }
                     },
                     {
                        low: {
                           value: "2",
                           bound: "EXACTLY"
                        },
                        high: {
                           value: "2",
                           bound: "EXACTLY"
                        }
                     }
                  ]
               }
            },
            {
               columnName: "processing",
               type: "boolean",
               domain: {
                  nullsAllowed: false,
                  ranges: [
                     {
                        low: {
                           value: "false",
                           bound: "EXACTLY"
                        },
                        high: {
                           value: "false",
                           bound: "EXACTLY"
                        }
                     }
                  ]
               }
            },
            {
               columnName: "custkey",
               type: "bigint",
               domain: {
                  nullsAllowed: false,
                  ranges: [
                     {
                        low: {
                           bound: "ABOVE"
                        },
                        high: {
                           value: "10",
                           bound: "EXACTLY"
                        }
                     }
                  ]
               }
            }
         ],
         estimate: {
            outputRowCount: 2,
            outputSizeInBytes: 40,
            cpuCost: 40,
            maxMemory: 0,
            networkCost: 0
         }
      }
   ],
   outputTable: {
      catalog: "hive",
      schemaTable: {
         schema: "tpch",
         table: "test_orders"
      }
   },
   estimate: {
      outputRowCount: "NaN",
      outputSizeInBytes: "NaN",
      cpuCost: "NaN",
      maxMemory: "NaN",
      networkCost: "NaN"
   }
}
```

## See also

{doc}`explain-analyze`
