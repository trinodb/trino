================
Custom Optimizer
================

Trino provides support to custom optimizer, registering of custom optimizer class is described below.

Custom Optimizer Class Register
-------------------------------
1. Create an Implementation class of `io.trino.sql.planner.optimizations.CustomPlanOptimizer`.
   Example of CustomPlanOptimizer Implementation class is ``io.trino.sql.planner.SampleCustomPlanOptimizer``
2. Set optimizer.custom-optimizer.allow properties to enable custom-optimizer
   ``optimizer.custom-optimizer.allow=true``
3. Set ``optimizer.custom-optimizer.list`` properties to register custom-optimizer classes.
   ``optimizer.custom-optimizer.list=io.trino.optimizer.CustomOptimizerImpl1,io.trino.optimizer.CustomOptimizerImpl2``



