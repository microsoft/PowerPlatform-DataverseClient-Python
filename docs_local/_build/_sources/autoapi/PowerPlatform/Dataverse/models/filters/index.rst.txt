PowerPlatform.Dataverse.models.filters
======================================

.. py:module:: PowerPlatform.Dataverse.models.filters

.. autoapi-nested-parse::

   Composable OData filter expressions for the Dataverse SDK.

   Provides an expression tree that compiles to OData ``$filter`` strings,
   with Python operator overloads (``&``, ``|``, ``~``) for composing
   complex filter conditions.

   Example::

       from PowerPlatform.Dataverse.models.filters import col, raw

       # Preferred GA idiom — col() proxy
       expr = col("statecode") == 0
       print(expr.to_odata())  # statecode eq 0

       # Complex composition with OR and AND
       expr = (col("statecode") == 0) | (col("statecode") == 1) & (col("revenue") > 100000)
       print(expr.to_odata())

       # In / not-in
       expr = col("statecode").in_([0, 1, 2])
       print(expr.to_odata())
       # Microsoft.Dynamics.CRM.In(PropertyName='statecode',PropertyValues=["0","1","2"])

       # Raw OData escape hatch (no deprecation warning)
       expr = raw("Microsoft.Dynamics.CRM.Today(PropertyName='createdon')")

       # Negation
       expr = ~(col("statecode") == 1)
       print(expr.to_odata())  # not (statecode eq 1)



Classes
-------

.. autoapisummary::

   PowerPlatform.Dataverse.models.filters.FilterExpression
   PowerPlatform.Dataverse.models.filters.ColumnProxy


Functions
---------

.. autoapisummary::

   PowerPlatform.Dataverse.models.filters.col
   PowerPlatform.Dataverse.models.filters.raw
   PowerPlatform.Dataverse.models.filters.eq
   PowerPlatform.Dataverse.models.filters.ne
   PowerPlatform.Dataverse.models.filters.gt
   PowerPlatform.Dataverse.models.filters.ge
   PowerPlatform.Dataverse.models.filters.lt
   PowerPlatform.Dataverse.models.filters.le
   PowerPlatform.Dataverse.models.filters.contains
   PowerPlatform.Dataverse.models.filters.startswith
   PowerPlatform.Dataverse.models.filters.endswith
   PowerPlatform.Dataverse.models.filters.between
   PowerPlatform.Dataverse.models.filters.is_null
   PowerPlatform.Dataverse.models.filters.is_not_null
   PowerPlatform.Dataverse.models.filters.filter_in
   PowerPlatform.Dataverse.models.filters.not_in
   PowerPlatform.Dataverse.models.filters.not_between


Module Contents
---------------

.. py:class:: FilterExpression

   Base class for composable OData filter expressions.

   Supports Python operator overloads for logical composition:

   - ``expr1 & expr2`` produces ``(expr1 and expr2)``
   - ``expr1 | expr2`` produces ``(expr1 or expr2)``
   - ``~expr`` produces ``not (expr)``


   .. py:method:: to_odata() -> str
      :abstractmethod:


      Compile this expression to an OData ``$filter`` string.



.. py:class:: ColumnProxy(name: str)

   Fluent proxy for building OData filter expressions from a column name.

   Returned by :func:`col`. Operator overloads and methods produce
   :class:`FilterExpression` instances that can be passed to
   ``QueryBuilder.where()``.

   Example::

       from PowerPlatform.Dataverse.models.filters import col

       expr = col("statecode") == 0               # equality
       expr = col("revenue") > 1_000_000          # comparison
       expr = col("name").like("Contoso%")        # startswith
       expr = col("name").is_null()               # null check
       expr = col("statecode").in_([0, 1])        # in


   .. py:method:: is_null() -> FilterExpression

      Column equals null: ``column eq null``.



   .. py:method:: is_not_null() -> FilterExpression

      Column not null: ``column ne null``.



   .. py:method:: in_(values: Collection[Any]) -> FilterExpression

      In filter using ``Microsoft.Dynamics.CRM.In``.

      :param values: Non-empty collection of values.
      :raises ValueError: If ``values`` is empty.



   .. py:method:: not_in(values: Collection[Any]) -> FilterExpression

      Not-in filter using ``Microsoft.Dynamics.CRM.NotIn``.

      :param values: Non-empty collection of values.
      :raises ValueError: If ``values`` is empty.



   .. py:method:: between(lo: Any, hi: Any) -> FilterExpression

      Between filter: ``(column ge lo and column le hi)``.



   .. py:method:: not_between(lo: Any, hi: Any) -> FilterExpression

      Not-between filter: ``not (column ge lo and column le hi)``.



   .. py:method:: contains(value: str) -> FilterExpression

      Contains filter: ``contains(column, value)``.



   .. py:method:: startswith(value: str) -> FilterExpression

      Startswith filter: ``startswith(column, value)``.



   .. py:method:: endswith(value: str) -> FilterExpression

      Endswith filter: ``endswith(column, value)``.



   .. py:method:: like(pattern: str) -> FilterExpression

      Pattern-match filter compiled to the closest OData equivalent.

      +-----------------+-----------------------------+-------------------------------------+
      | Pattern form    | Example                     | Compiles to                         |
      +=================+=============================+=====================================+
      | ``val%``        | ``like("Contoso%")``        | ``startswith(column,'Contoso')``     |
      +-----------------+-----------------------------+-------------------------------------+
      | ``%val``        | ``like("%Ltd")``            | ``endswith(column,'Ltd')``          |
      +-----------------+-----------------------------+-------------------------------------+
      | ``%val%``       | ``like("%Corp%")``          | ``contains(column,'Corp')``         |
      +-----------------+-----------------------------+-------------------------------------+
      | No wildcard     | ``like("Contoso")``         | ``column eq 'Contoso'``             |
      +-----------------+-----------------------------+-------------------------------------+
      | Other           | ``like("Con%oso")``         | :class:`ValueError`                 |
      +-----------------+-----------------------------+-------------------------------------+

      :param pattern: LIKE-style pattern string.
      :raises ValueError: If the pattern cannot be reduced to a single OData function.



   .. py:method:: not_like(pattern: str) -> FilterExpression

      Negated pattern-match filter; mirrors :meth:`like` rules then negates.

      :param pattern: LIKE-style pattern string (same rules as :meth:`like`).
      :raises ValueError: If the pattern cannot be reduced to a single OData function.



.. py:function:: col(name: str) -> ColumnProxy

   Return a :class:`ColumnProxy` for building filter expressions.

   This is the preferred GA idiom for constructing filter expressions::

       from PowerPlatform.Dataverse.models.filters import col

       expr = col("statecode") == 0
       expr = col("revenue") > 1_000_000
       expr = col("name").like("Contoso%")
       expr = col("statecode").in_([0, 1])
       expr = col("parentaccountid").is_null()

   :param name: Column logical name (case-insensitive, will be lowercased).
   :return: A :class:`ColumnProxy` bound to the column.
   :raises ValueError: If ``name`` is empty.


.. py:function:: raw(filter_string: str) -> FilterExpression

   Verbatim OData filter expression (passed through unchanged).

   This function is **not** deprecated — it is the OData escape hatch with
   no typed replacement.

   :param filter_string: Raw OData filter string.
   :return: A :class:`FilterExpression`.

   Example::

       raw("Microsoft.Dynamics.CRM.Today(PropertyName='createdon')")


.. py:function:: eq(column: str, value: Any) -> FilterExpression

   Equality filter: ``column eq value``.

   .. deprecated::
       Use ``col(column) == value`` instead.


.. py:function:: ne(column: str, value: Any) -> FilterExpression

   Not-equal filter: ``column ne value``.

   .. deprecated::
       Use ``col(column) != value`` instead.


.. py:function:: gt(column: str, value: Any) -> FilterExpression

   Greater-than filter: ``column gt value``.

   .. deprecated::
       Use ``col(column) > value`` instead.


.. py:function:: ge(column: str, value: Any) -> FilterExpression

   Greater-than-or-equal filter: ``column ge value``.

   .. deprecated::
       Use ``col(column) >= value`` instead.


.. py:function:: lt(column: str, value: Any) -> FilterExpression

   Less-than filter: ``column lt value``.

   .. deprecated::
       Use ``col(column) < value`` instead.


.. py:function:: le(column: str, value: Any) -> FilterExpression

   Less-than-or-equal filter: ``column le value``.

   .. deprecated::
       Use ``col(column) <= value`` instead.


.. py:function:: contains(column: str, value: str) -> FilterExpression

   Contains filter: ``contains(column, value)``.

   .. deprecated::
       Use ``col(column).contains(value)`` instead.


.. py:function:: startswith(column: str, value: str) -> FilterExpression

   Startswith filter: ``startswith(column, value)``.

   .. deprecated::
       Use ``col(column).startswith(value)`` instead.


.. py:function:: endswith(column: str, value: str) -> FilterExpression

   Endswith filter: ``endswith(column, value)``.

   .. deprecated::
       Use ``col(column).endswith(value)`` instead.


.. py:function:: between(column: str, low: Any, high: Any) -> FilterExpression

   Between filter: ``(column ge low and column le high)``.

   .. deprecated::
       Use ``col(column).between(low, high)`` instead.


.. py:function:: is_null(column: str) -> FilterExpression

   Null check: ``column eq null``.

   .. deprecated::
       Use ``col(column).is_null()`` instead.


.. py:function:: is_not_null(column: str) -> FilterExpression

   Not-null check: ``column ne null``.

   .. deprecated::
       Use ``col(column).is_not_null()`` instead.


.. py:function:: filter_in(column: str, values: Collection[Any]) -> FilterExpression

   In filter using ``Microsoft.Dynamics.CRM.In``.

   Named ``filter_in`` because ``in`` is a Python keyword.

   .. deprecated::
       Use ``col(column).in_(values)`` instead.

   :raises ValueError: If ``values`` is empty.


.. py:function:: not_in(column: str, values: Collection[Any]) -> FilterExpression

   Not-in filter using ``Microsoft.Dynamics.CRM.NotIn``.

   .. deprecated::
       Use ``col(column).not_in(values)`` instead.

   :raises ValueError: If ``values`` is empty.


.. py:function:: not_between(column: str, low: Any, high: Any) -> FilterExpression

   Not-between filter: ``not (column ge low and column le high)``.

   .. deprecated::
       Use ``col(column).not_between(low, high)`` instead.


