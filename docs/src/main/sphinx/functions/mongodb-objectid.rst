===============================
MongoDB ObjectID functions
===============================

.. _objectid:

ObjectID
----------------
The ``ObjectID`` function can be used to create unique 
identifiers for any document in a database. An ObjectID is 
assigned to any document created in MongoDB.

Each ObjectID is generated randomly with the use of an 
algorithm to ensure the ObjectID's uniqueness. 
ObjectID values are 12-bytes or 24 characters in length 
and contain the following parameters in order:

1. **4 bytes:** Representing the timestamp value of the ObjectID's creation in seconds
2. **5 bytes:** Representing a random value
3. **3 bytes:** Representing the increment value of the ObjectID

Creating a new ObjectID
-------------------------
You can create a new ObjectID by using the ``ObjectID`` function 
without an argument. The following code sample generates 
a new ObjectID for the variable. ::

    apples = ObjectID()
    
This code sample generates a unique ObjectID for ``apples``.

Assigning a specific ObjectID
-------------------------------
You can assign an ObjectID to a variable by setting an argument 
to the ``ObjectID`` function. The following code sample 
assigns a variable with an unique ObjectID. :: 

    oranges = ObjectID("60957e49e39ea7b4b103c181")

The argument assigns ``oranges`` with an ObjectID of 
"60957e49e39ea7b4b103c181".

Accessing an ObjectID
-----------------------
You can access the ObjectID of a variable by converting the 
ObjectID into the string data type. The following code sample 
converts the ObjectID value to a string. ::

    ObjectID("60957e49e39ea7b4b103c181").str

The code sample returns the hexadecimal 
"60957e49e39ea7b4b103c181" in string format.

.. _objectid_timestamp:

ObjectID_timestamp
--------------------
The ``ObjectID_timestamp`` function can return the timestamp 
portion of the ObjectID as a date. The following code sample 
returns the date that the ObjectID was generated. ::

    ObjectID("60957e49e39ea7b4b103c181")_timestamp()

The ObjectID_timestamp function returns ``ISODate("2021-05-07T17:52:09Z")``
