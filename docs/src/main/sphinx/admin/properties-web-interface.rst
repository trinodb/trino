Web UI Properties
-----------------

The following properties can be used to configure the :doc:`web-interface`.

``web-ui.authentication.type``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Allowed values:** ``form``, ``fixed``, ``certificate``, ``kerberos``, ``jwt``
* **Default value:** ``form``

The authentication mechanism to allow user access to the Web UI. See
:ref:`Web UI Authentication <web-ui-authentication>`.

``web-ui.enabled``
^^^^^^^^^^^^^^^^^^

* **Type:** ``boolean``
* **Default value:** ``true``

This property controls whether or not the Web UI is available.

``web-ui.shared-secret``
^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:** randomly generated unless set

The shared secret is used to generate authentication cookies for users of
the Web UI. If not set to a static value, any coordinator restart generates
a new random value, which in turn invalidates the session of any currently
logged in Web UI user.

``web-ui.session-timeout``
^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Type:** ``duration``
* **Default value:** ``1 day``

The duration how long a user can be logged into the Web UI, before the
session times out, which forces an automatic log-out.

``web-ui.user``
^^^^^^^^^^^^^^^

* **Type:** ``string``
* **Default value:**

The username automatically used for authentication to the Web UI with the ``fixed``
authentication type. See :ref:`Web UI Authentication <web-ui-authentication>`.
