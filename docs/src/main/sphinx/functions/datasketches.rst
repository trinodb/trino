======================
DataSketches Functions
======================
DataSketches is a high-performance library of stochastic streaming
algorithms commonly called ”sketches” in the data sciences. Sketches are
small, stateful programs that process massive data as a stream and can
provide approximate answers, with mathematical guarantees, to
computationally difficult queries orders-of-magnitude faster than
traditional, exact methods.
The DataSketches functions allows querying the fast and memory-efficient `Apache
DataSkecthes <https://datasketches.apache.org/docs/Community/Research.html>`_
from Trino. Support for `Theta Sketch Framework <https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html>`_
is added, specifically :func:`theta_sketch_union` and :func:`theta_sketch_estimate` functions.
These functions are used in the ``count distinct`` queries using sketches.
Datasketches can be created using Hive or Pig using respective sketch APIs.

DataSketches functions
----------------------

.. function:: theta_sketch_union(sketches) -> sketch

    Returns a single sketch which is a merged collection of sketches.

.. function:: theta_sketch_estimate(sketch) -> double

    Returns the estimated value of the sketch.

Example in Trino for using DataSketches
---------------------------------------
Query::

    sql
    SELECT
      o_orderdate as date,
      theta_sketch_estimate(theta_sketch_union(o_custkey_sketch)) AS unique_user_count
      SUM(o_totalprice) AS user_spent,
    FROM tpch.sf100000.orders WHERE o_orderdate >= dateadd(day, -90, current_date)
    GROUP BY o_orderdate;

