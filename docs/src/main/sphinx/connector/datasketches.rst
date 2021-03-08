======================
DataSketches Connector
======================
DataSketches is a high-performance library of stochastic streaming
algorithms commonly called ”sketches” in the data sciences. Sketches are
small, stateful programs that process massive data as a stream and can
provide approximate answers, with mathematical guarantees, to
computationally difficult queries orders-of-magnitude faster than
traditional, exact methods.
The DataSketches connector allows querying the fast and memory-efficient `Apache
DataSkecthes <https://datasketches.apache.org/docs/Community/Research.html>`_
from Trino. Support for `Thetasketch Framework <https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html>`_
is added, specifically ``thetasketch_union`` and ``thetasketch_estimate`` functions.
These functions are used in the ``count distinct`` queries using sketches.
Datasketches can be created using Hive or Pig using sketch respective APIs.

============================================ =================================================================
Function Name                                Description
============================================ =================================================================
``thetasketch_union``                        Merges a collection of sketches into a single sketch

``thetasketch_estimate``                     Estimate the value of the merged sketches
============================================ =================================================================

Example in Trino for using DataSketches
---------------------------------------
Query::

    sql
    SELECT
      brand,
      SUM(user_spent) AS user_spent,
      thetasketch_estimate(thetasketch_union(sketch)) AS unique_user_count;
    FROM catalog.schema.table WHERE datestamp = '<date>' AND data_source = '<datasource>'
    GROUP BY brand;

