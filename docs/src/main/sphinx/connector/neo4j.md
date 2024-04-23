# Neo4j connector

```{raw} html
<img src="../_static/img/neo4j.png" class="connector-logo">
```

[Neo4j](https://neo4j.com) is a graph database implementing the
labeled property graph (LPG) model. The Neo4j connector provides
access to data stored in Neo4j.

Data in a Neo4j is queried and modified using the [Cypher query
language](https://neo4j.com/product/cypher-graph-query-language/)
(CQL).

This connector does not attempt to provide a complete mapping between
SQL and CQL, but instead provides:
- a simple general mapping from the graph model to the relational
model, with limited query capabilities
- the ability to execute any CQL query and map the result to SQL row types


See [SQL support](#sql-support) for more info.

## Requirements

To use Neo4j, you need:

- Network access from the Trino coordinator and workers to the Neo4j
  instances.


## General configuration

To configure the Neo4j connector, create a catalog properties file
`etc/catalog/example.properties` that references the `neo4j`
connector.  At least `neo4j.uri` must be set to a Neo4j instance URI:

```properties
connector.name=neo4j
neo4j.uri=bolt://127.0.0.1:7687/
```

### Configuration properties

```{eval-rst}
.. list-table:: Neo4j general configuration properties
  :widths: 30, 58, 12
  :header-rows: 1
  * - Property name
    - Description
    - Default
  * - ``neo4j.uri``
    - URI for Neo4j instance
    -
  * - ``neo4j.auth.type``
    - Authentication type to use.
      Possible values are:
      * ``none``
      * ``basic``
      * ``bearer``
    - ``basic``
  * - ``neo4j.auth.basic.user``
    - The user to use for basic auth.
    - ``neo4j``
  * - ``neo4j.auth.basic.password``
    - Password for basic auth.
    -
  * - ``neo4j.auth.bearer.token``
    - Token for bearer auth
    -
```

## Type mapping

Neo4j has a dynamically typed data model wihout schemas. Types are
associated with values, the same property name can have values of
different types.

This connector handles the lack of statically known types in the
following ways:

For the [relational mapping](#relational-mapping), types are inferred
from the graph and each property type name is statically mapped to a
canonical Trino type according to [this](#property-type-mapping)
table.

For the [table function](#system.query), a schema is either supplied
by the caller, or the result is returned as a single JSON object.

All Neo4j types have a mapping to the Trino `JSON` type.


### Neo4j property types to Trino type mapping

Not all Cypher types can be used as properties on graph entities. See
[here](https://neo4j.com/docs/cypher-manual/current/values-and-types/property-structural-constructed/)
for details.

For types that can be used as properties, the connector uses the
following mapping from property type name to Trino type:

(property-type-mapping)=

```{eval-rst}
.. list-table:: Neo4j to Trino type mapping
  :widths: 40, 60
  :header-rows: 1
  * - Neo4j property type
    - Trino type
  * - ``Boolean``
    - ``BOOLEAN``
  * - ``Float``
    - ``DOUBLE``
  * - ``Integer``
    - ``BIGINT``
  * - ``Long``
    - ``BIGINT``
  * - ``String``
    - ``VARCHAR``
  * - ``Duration``
    - ``JSON``
  * - ``Date``
    - ``DATE``
  * - ``LocalTime``
    - ``TIME``
  * - ``Time``
    - ``TIME WITH TIMESTAMP``
  * - ``LocalDateTime``
    - ``TIMESTAMP``
  * - ``DateTime``
    - ``TIMESTAMP WITH TIME ZONE``
  * - ``*Array``
    - ``ARRAY(e)``
```


(sql-support)=
## SQL support

This connector provides access to data stored in Neo4j in two different ways:

- A simple [mapping](#relational-mapping) from the graph model to the
  relational model, with limited query capabilities.
- A [table function](#system.query) for executing any CQL and mapping
  the result to SQL row types

(relational-mapping)=
### Relational mapping

Labeled nodes are mapped to tables, and node properties to columns.
Labeled node tables are named `(Label)`, following CQL naming
conventions.

Similarly, relationships are mapped to tables, and relationship
properties to columns.  Relationship tables are named
`[RELATIONSHIP_TYPE]`.

Note that since Trino is case insensitive, so are the
node/relationship table names.

Available nodes and relationships, and their properties and types are
queried using `db.schema.nodeTypeProperties()` and
`db.schema.relTypeProperties()`. These built-in functions infer a
schema by sampling the graph, and can theoretically miss entities and
properties, but should be good enough in practice for sensible graphs.

#### Examples

Find all properties for all nodes labeled `Person`:

```
SELECT * FROM "(Person)"
```

Find properties for all `LIKES` relationships:

```
SELECT * FROM "[LIKES]"
```

#### Pushdown support

Currently, only `LIMIT` pushdown is supported.


### Table functions

(system.query)=
:::{function} system.query(query => varchar, schema => descriptor, database => varchar) -> table
Executes `query` returning rows according to `schema`.

- `query` is a CQL query.
- `schema` is a descriptor with the desired row structure. If not
supplied, the returned rows will have a single column named `result`
with a JSON object constructed from the return result of the query.
- `database` is the Neo4j database to use for the query. If not
  supplied, defaults to the default database for the instance.

**Examples**

Without `schema`:

```
SELECT * FROM TABLE(
  system.query(
    query => 'MATCH (p:Person) return p.name as name, p.age as age'
  )
)
```

| result                                |
|---------------------------------------|
| `{ "name": "Micky Mouse", age: 47 } ` |
| `{ "name": "Donald Duck", age: 58 } ` |


With `schema`:

```
SELECT * FROM TABLE(
  system.query(
    query => 'MATCH (p:Person) return p.name, p.age',
    schema => DESCRIPTOR(name varchar, age integer)
  )
)
```

| name          | age |
|---------------|-----|
| "Micky Mouse" | 47  |
| "Donald Duck" | 58  |


:::
