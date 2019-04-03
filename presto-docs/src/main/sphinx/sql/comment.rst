=======
COMMENT
=======

Synopsis
--------

.. code-block:: none

    COMMENT ON TABLE name IS 'comments'

Description
-----------

Set the comment for a table. The comment can be removed by setting the comment to ``NULL``.

Examples
--------

Change the comment for the ``users`` table to be ``master table``::

    COMMENT ON TABLE users IS 'master table';

