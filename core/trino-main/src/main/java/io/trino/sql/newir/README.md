# The new IR

The new IR for Trino is conceptually based on [MLIR](https://mlir.llvm.org/).\
This is a prototype implementation. It will change over time.

## Abstractions

### Operation

[Operation](Operation.java) is the main building block. When representing a
Trino query plan, we use operations to represent relational and scalar SQL
computations. Some examples of operations are:

- [Constant](../dialect/trino/operation/Constant.java): represents a constant scalar value or a constant relation,
- [FieldSelection](../dialect/trino/operation/FieldSelection.java): selects a row field by name,
- [Filter](../dialect/trino/operation/Filter.java): relational filter operation,
- [Output](../dialect/trino/operation/Output.java): query output,
- [Query](../dialect/trino/operation/Query.java): represents a SELECT statement,
- [Return](../dialect/trino/operation/Return.java): terminal operation,
- [Row](../dialect/trino/operation/Row.java): row constructor,
- [Values](../dialect/trino/operation/Values.java): SQL values.

All operations share common features:

1. Operations return a single result, being a typed value. An operation derives its
   output type based on its inputs and attributes.
2. Operations can have a list of [Regions](Region.java) containing [Blocks](Block.java)
   to express nested logic. For example, [Filter](../dialect/trino/operation/Filter.java) has a block
   with the predicate, and [CorrelatedJoin](../dialect/trino/operation/CorrelatedJoin.java) has a block with the subquery program. A block
   has a list of operations, forming a recursive structure.
3. Operations can take arguments, being either results of another operation,
   or parameters of an enclosing block. Operations can require that arguments
   adhere to type constraints.
4. Operations can have attributes. Attributes represent
   logical properties of the operation's result, like cardinality. They can also represent
   operation's internal properties, like the join type.

### Value

[Value](Value.java) is another abstraction important for MLIR-style modeling.
A value is either an operation result or a block parameter.\
Values follow the
[SSA form](https://en.wikipedia.org/wiki/Static_single-assignment_form), which means
that each value is assigned exactly once.\
Values follow the usual visibility rules: a value is visible if it was assigned
earlier in the same region, or above and outside the region of use.\
Visibility across nested regions is useful for modeling nested lambdas, like
deeply correlated subqueries.

In the current Trino IR, the PlanNodes take other PlanNodes as sources.
Using explicit values introduces indirection between the operation which
produces the value and the operation which consumes it.

### Program

A [Program](Program.java) consists of a root operation and a mapping of all values to the
declaring operations or blocks.

We use program to represent a query plan. A program for a query has
one top-level [Query](../dialect/trino/operation/Query.java) operation.
This operation has one block which contains the full query program, ending with a
terminal [Output](../dialect/trino/operation/Output.java) operation.
In the future, this model can be easily extended to represent a set of statements.

A program has a print method which creates a complete textual representation
from which the program can be recreated. This textual representation is used
as the transport format.

### Dialect
A [Dialect](Dialect.java) is a collection of operations, types, and attributes.

## Evolution of the project

#### Phase 1: new plan representation (in progress)

- Define the abstractions in the spirit of MLIR.
- Support programmatic creation.
- Implement serialization-deserialization using the MLIR assembly format.
- Implement the scalar and relational operations which are present in the optimized Trino plan.
  Use high level of abstraction, similar to that of PlanNodes.
