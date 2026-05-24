PowerPlatform.Dataverse.models.table_info
=========================================

.. py:module:: PowerPlatform.Dataverse.models.table_info

.. autoapi-nested-parse::

   Table and column metadata models for Dataverse.



Classes
-------

.. autoapisummary::

   PowerPlatform.Dataverse.models.table_info.ColumnInfo
   PowerPlatform.Dataverse.models.table_info.TableInfo
   PowerPlatform.Dataverse.models.table_info.AlternateKeyInfo


Module Contents
---------------

.. py:class:: ColumnInfo

   Column metadata from a Dataverse table definition.

   :param schema_name: Column schema name (e.g. ``"new_Price"``).
   :type schema_name: :class:`str`
   :param logical_name: Column logical name (lowercase).
   :type logical_name: :class:`str`
   :param type: Column type string (e.g. ``"String"``, ``"Integer"``).
   :type type: :class:`str`
   :param is_primary: Whether this is the primary name column.
   :type is_primary: :class:`bool`
   :param is_required: Whether the column is required.
   :type is_required: :class:`bool`
   :param max_length: Maximum length for string columns.
   :type max_length: :class:`int` or None
   :param display_name: Human-readable display name.
   :type display_name: :class:`str` or None
   :param description: Column description.
   :type description: :class:`str` or None


   .. py:attribute:: schema_name
      :type:  str
      :value: ''



   .. py:attribute:: logical_name
      :type:  str
      :value: ''



   .. py:attribute:: type
      :type:  str
      :value: ''



   .. py:attribute:: is_primary
      :type:  bool
      :value: False



   .. py:attribute:: is_required
      :type:  bool
      :value: False



   .. py:attribute:: max_length
      :type:  Optional[int]
      :value: None



   .. py:attribute:: display_name
      :type:  Optional[str]
      :value: None



   .. py:attribute:: description
      :type:  Optional[str]
      :value: None



   .. py:method:: from_api_response(response_data: Dict[str, Any]) -> ColumnInfo
      :classmethod:


      Create from a raw Dataverse ``AttributeMetadata`` API response.

      :param response_data: Raw attribute metadata dict (PascalCase keys).
      :type response_data: :class:`dict`
      :rtype: :class:`ColumnInfo`



.. py:class:: TableInfo

   Table metadata with dict-like backward compatibility.

   Supports both new attribute access (``info.schema_name``) and legacy
   dict-key access (``info["table_schema_name"]``) for backward
   compatibility with code written against the raw dict API.

   :param schema_name: Table schema name (e.g. ``"Account"``).
   :type schema_name: :class:`str`
   :param logical_name: Table logical name (lowercase).
   :type logical_name: :class:`str`
   :param entity_set_name: OData entity set name.
   :type entity_set_name: :class:`str`
   :param metadata_id: Metadata GUID.
   :type metadata_id: :class:`str`
   :param display_name: Human-readable display name.
   :type display_name: :class:`str` or None
   :param description: Table description.
   :type description: :class:`str` or None
   :param columns: Column metadata (when retrieved).
   :type columns: list[ColumnInfo] or None
   :param columns_created: Column schema names created with the table.
   :type columns_created: list[str] or None

   Example::

       info = client.tables.create("new_Product", {"new_Price": "decimal"})
       print(info.schema_name)              # new attribute access
       print(info["table_schema_name"])     # legacy dict-key access


   .. py:attribute:: schema_name
      :type:  str
      :value: ''



   .. py:attribute:: logical_name
      :type:  str
      :value: ''



   .. py:attribute:: entity_set_name
      :type:  str
      :value: ''



   .. py:attribute:: metadata_id
      :type:  str
      :value: ''



   .. py:attribute:: primary_name_attribute
      :type:  Optional[str]
      :value: None



   .. py:attribute:: primary_id_attribute
      :type:  Optional[str]
      :value: None



   .. py:attribute:: display_name
      :type:  Optional[str]
      :value: None



   .. py:attribute:: description
      :type:  Optional[str]
      :value: None



   .. py:attribute:: columns
      :type:  Optional[List[ColumnInfo]]
      :value: None



   .. py:attribute:: columns_created
      :type:  Optional[List[str]]
      :value: None



   .. py:method:: get(key: str, default: Any = None) -> Any

      Return value for *key*, or *default* if not present.



   .. py:method:: keys() -> KeysView[str]

      Return legacy dict keys.



   .. py:method:: values() -> List[Any]

      Return values corresponding to legacy dict keys.



   .. py:method:: items() -> List[tuple]

      Return (legacy_key, value) pairs.



   .. py:method:: from_dict(data: Dict[str, Any]) -> TableInfo
      :classmethod:


      Create from an SDK internal dict (snake_case keys).

      This handles the dict format returned by ``_create_table`` and
      ``_get_table_info`` in the OData layer.

      :param data: Dictionary with SDK snake_case keys.
      :type data: :class:`dict`
      :rtype: :class:`TableInfo`



   .. py:method:: from_api_response(response_data: Dict[str, Any]) -> TableInfo
      :classmethod:


      Create from a raw Dataverse ``EntityDefinition`` API response.

      :param response_data: Raw entity metadata dict (PascalCase keys).
      :type response_data: :class:`dict`
      :rtype: :class:`TableInfo`



   .. py:method:: to_dict() -> Dict[str, Any]

      Return a dict with legacy keys for backward compatibility.



.. py:class:: AlternateKeyInfo

   Alternate key metadata for a Dataverse table.

   :param metadata_id: Key metadata GUID.
   :type metadata_id: :class:`str`
   :param schema_name: Key schema name.
   :type schema_name: :class:`str`
   :param key_attributes: List of column logical names that compose the key.
   :type key_attributes: list[str]
   :param status: Index creation status (``"Active"``, ``"Pending"``, ``"InProgress"``, ``"Failed"``).
   :type status: :class:`str`


   .. py:attribute:: metadata_id
      :type:  str
      :value: ''



   .. py:attribute:: schema_name
      :type:  str
      :value: ''



   .. py:attribute:: key_attributes
      :type:  List[str]
      :value: []



   .. py:attribute:: status
      :type:  str
      :value: ''



   .. py:method:: from_api_response(response_data: Dict[str, Any]) -> AlternateKeyInfo
      :classmethod:


      Create from raw EntityKeyMetadata API response.

      :param response_data: Raw key metadata dictionary from the Web API.
      :type response_data: :class:`dict`
      :rtype: :class:`AlternateKeyInfo`



