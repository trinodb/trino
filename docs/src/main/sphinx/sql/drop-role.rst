=========
DROP ROLE
=========

Synopsis
--------

.. code-block:: text

    DROP ROLE role_name
    [ IN catalog ]

Description
-----------

``DROP ROLE`` drops the specified role.

For ``DROP ROLE`` statement to succeed, the user executing it should possess
admin privileges for the given role.

The optional ``IN catalog`` clause drops the role in a catalog as opposed
to a system role.

Examples
--------

Drop role ``admin`` ::

    DROP ROLE admin;

Limitations
-----------

Some connectors do not support role management.
See connector documentation for more details.

See also
--------

:doc:`create-role`, :doc:`set-role`, :doc:`grant-roles`, :doc:`revoke-roles`
