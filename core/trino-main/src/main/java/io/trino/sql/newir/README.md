# The new IR

The new IR for Trino is conceptually based on [MLIR](https://mlir.llvm.org/).\
This is a prototype implementation. It will change over time.

## Abstractions

### Operation

[Operation](Operation.java) is the main abstraction. It is used to represent
relational and scalar SQL operations in a uniform way. Some examples of
operations are:

- [Constant](Constant.java): represents a constant scalar value or a constant relation,
- [FieldSelection](FieldSelection.java): selects a row field by name,
- [Filter](Filter.java): relational filter operation,
- [Output](Output.java): query output,
- [Query](Query.java): represents a SELECT statement,
- [Return](Return.java): terminal operation,
- [Row](Row.java): row constructor,
- [Values](Values.java): SQL values.

All operations share common features:

1. Operations return a single result, being a typed value. An operation derives its
   output type based on its inputs and attributes.
2. Operations can have a list of [Regions](Region.java) containing [Blocks](Block.java)
   to express nested logic as a lambda. For example, [Filter](Filter.java) has a block
   with the predicate, CorrelatedJoin has a block with the subquery program. A block
   has a list of operations, forming a recursive structure.
3. Operations can take arguments, being either results of another operation,
   or parameters of an enclosing block. Operations can require that arguments
   adhere to [TypeConstraints](TypeConstraint.java).
4. Operations can have [Attributes](Attribute.java). Attributes represent
   logical properties of the operation's result, like cardinality. They can also represent
   operation's properties, like join type.

### Value

[Value](Value.java) is another abstraction important for MLIR-style modeling.
A value is either an operation result or a block parameter.\
Values follow the
[SSA form](https://en.wikipedia.org/wiki/Static_single-assignment_form), which means
that each value is assigned exactly once.\
Values follow the usual visibility rules: a value is visible if it was assigned
earlier in the same block, or it is a parameter of some enclosing block.
Visibility across nested blocks is useful for modeling nested lambdas, like
deeply correlated subqueries.\
In MLIR, blocks can invoke other blocks and pass values. In our initial model,
we operate on a higher abstraction level, and do not use it.

In the current Trino IR, the PlanNodes take other PlanNodes as sources.
Using explicit values introduces indirection between the operation which
produces the value and the operation which consumes it.

### Program

Program represents a query plan.\
It has one top-level [Query](Query.java) operation.
This operation has one block which contains the full query program, ending with a
terminal [Output](Output.java) operation.\
Program also has a map which links each value to its source. The source is either
an operation returning the value or a block declaring the value as a parameter.

In the future, this model can be easily extended to represent a set of statements.

## Conversions between old and new IRs

Initially, the old and new IRs will coexist in Trino. We need a way to go from one
representation to the other.\
[ProgramBuilder](ProgramBuilder.java) converts a tree of PlanNodes into a Program.
For now, it supports a small subset of PlanNodes.\
For the conversion, it uses two visitor-based rewriters:

- [RelationalProgramBuilder](RelationalProgramBuilder.java) rewrites PlanNodes based on [PlanVisitor](../planner/plan/PlanVisitor.java)
- [ScalarProgramBuilder](ScalarProgramBuilder.java) rewrites scalar expressions based on [IrVisitor](../ir/IrVisitor.java)

We need the two rewriters because the old IR has different representations for scalar
and relational operations. In the new IR, all operations are represented in a uniform way.

## Evolution of the project

#### Phase 1: new plan representation (in progress)

- Define the abstractions in the spirit of MLIR.
- Support programmatic creation.
- Implement serialization-deserialization using the MLIR assembly format.
- Implement the scalar and relational operations which are present in the optimized Trino plan.
  Use high level of abstraction, similar to that of PlanNodes.
