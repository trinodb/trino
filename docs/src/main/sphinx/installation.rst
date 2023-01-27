************
Installation
************

A Trino server can be installed and deployed on a number of different
platforms. Typically you run a cluster of machines with one coordinator and many
workers. You can find instructions for deploying such a cluster, and related
information, in the following sections:

.. toctree::
    :maxdepth: 1

    installation/deployment
    installation/containers
    installation/kubernetes
    installation/rpm
    installation/query-resiliency

Once you have a completed the deployment, or if you have access to a running
cluster already, you can proceed to configure your :doc:`client application
</client>`.
