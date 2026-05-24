PowerPlatform.Dataverse.models.upsert
=====================================

.. py:module:: PowerPlatform.Dataverse.models.upsert

.. autoapi-nested-parse::

   Upsert data models for the Dataverse SDK.



Classes
-------

.. autoapisummary::

   PowerPlatform.Dataverse.models.upsert.UpsertItem


Module Contents
---------------

.. py:class:: UpsertItem

   Represents a single upsert operation targeting a record by its alternate key.

   Used with :meth:`~PowerPlatform.Dataverse.operations.records.RecordOperations.upsert`
   to upsert one or more records identified by alternate keys rather than primary GUIDs.

   :param alternate_key: Dictionary mapping alternate key attribute names to their values.
       String values are automatically quoted and escaped in the OData URL. Integer and
       other non-string values are included without quotes.
   :type alternate_key: dict[str, Any]
   :param record: Dictionary of attribute names to values for the record payload.
       Keys are automatically lowercased. Picklist labels are resolved to integer option
       values when a matching option set is found.
   :type record: dict[str, Any]

   Example::

       item = UpsertItem(
           alternate_key={"accountnumber": "ACC-001", "address1_postalcode": "98052"},
           record={"name": "Contoso Ltd", "telephone1": "555-0100"},
       )


   .. py:attribute:: alternate_key
      :type:  Dict[str, Any]


   .. py:attribute:: record
      :type:  Dict[str, Any]


