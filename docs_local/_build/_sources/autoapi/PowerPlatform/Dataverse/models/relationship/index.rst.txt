PowerPlatform.Dataverse.models.relationship
===========================================

.. py:module:: PowerPlatform.Dataverse.models.relationship

.. autoapi-nested-parse::

   Relationship models for Dataverse (input and output).



Classes
-------

.. autoapisummary::

   PowerPlatform.Dataverse.models.relationship.CascadeConfiguration
   PowerPlatform.Dataverse.models.relationship.LookupAttributeMetadata
   PowerPlatform.Dataverse.models.relationship.OneToManyRelationshipMetadata
   PowerPlatform.Dataverse.models.relationship.ManyToManyRelationshipMetadata
   PowerPlatform.Dataverse.models.relationship.RelationshipInfo


Module Contents
---------------

.. py:class:: CascadeConfiguration

   Defines cascade behavior for relationship operations.

   :param assign: Cascade behavior for assign operations.
   :type assign: str
   :param delete: Cascade behavior for delete operations.
   :type delete: str
   :param merge: Cascade behavior for merge operations.
   :type merge: str
   :param reparent: Cascade behavior for reparent operations.
   :type reparent: str
   :param share: Cascade behavior for share operations.
   :type share: str
   :param unshare: Cascade behavior for unshare operations.
   :type unshare: str
   :param additional_properties: Optional dict of additional properties to include
       in the Web API payload (e.g., "Archive", "RollupView"). These are merged
       last and can override default values.
   :type additional_properties: Optional[Dict[str, Any]]

   Valid values for each parameter:
       - "Cascade": Perform the operation on all related records
       - "NoCascade": Do not perform the operation on related records
       - "RemoveLink": Remove the relationship link but keep the records
       - "Restrict": Prevent the operation if related records exist


   .. py:attribute:: assign
      :type:  str
      :value: 'NoCascade'



   .. py:attribute:: delete
      :type:  str
      :value: 'RemoveLink'



   .. py:attribute:: merge
      :type:  str
      :value: 'NoCascade'



   .. py:attribute:: reparent
      :type:  str
      :value: 'NoCascade'



   .. py:attribute:: share
      :type:  str
      :value: 'NoCascade'



   .. py:attribute:: unshare
      :type:  str
      :value: 'NoCascade'



   .. py:attribute:: additional_properties
      :type:  Optional[Dict[str, Any]]
      :value: None



   .. py:method:: to_dict() -> Dict[str, Any]

      Convert to Web API JSON format.

      Example::

          >>> config = CascadeConfiguration(delete="Cascade", assign="NoCascade")
          >>> config.to_dict()
          {
              'Assign': 'NoCascade',
              'Delete': 'Cascade',
              'Merge': 'NoCascade',
              'Reparent': 'NoCascade',
              'Share': 'NoCascade',
              'Unshare': 'NoCascade'
          }



.. py:class:: LookupAttributeMetadata

   Metadata for a lookup attribute.

   :param schema_name: Schema name for the attribute (e.g., "new_AccountId").
   :type schema_name: str
   :param display_name: Display name for the attribute.
   :type display_name: Label
   :param description: Optional description of the attribute.
   :type description: Optional[Label]
   :param required_level: Requirement level for the attribute.
   :type required_level: str
   :param additional_properties: Optional dict of additional properties to include
       in the Web API payload. Useful for setting properties like "Targets" (to
       specify which entity types the lookup can reference), "LogicalName",
       "IsSecured", "IsValidForAdvancedFind", etc. These are merged last and
       can override default values.
   :type additional_properties: Optional[Dict[str, Any]]

   Valid required_level values:
       - "None": The attribute is optional
       - "Recommended": The attribute is recommended
       - "ApplicationRequired": The attribute is required


   .. py:attribute:: schema_name
      :type:  str


   .. py:attribute:: display_name
      :type:  PowerPlatform.Dataverse.models.labels.Label


   .. py:attribute:: description
      :type:  Optional[PowerPlatform.Dataverse.models.labels.Label]
      :value: None



   .. py:attribute:: required_level
      :type:  str
      :value: 'None'



   .. py:attribute:: additional_properties
      :type:  Optional[Dict[str, Any]]
      :value: None



   .. py:method:: to_dict() -> Dict[str, Any]

      Convert to Web API JSON format.

      Example::

          >>> lookup = LookupAttributeMetadata(
          ...     schema_name="new_AccountId",
          ...     display_name=Label([LocalizedLabel("Account", 1033)])
          ... )
          >>> lookup.to_dict()
          {
              '@odata.type': 'Microsoft.Dynamics.CRM.LookupAttributeMetadata',
              'SchemaName': 'new_AccountId',
              'AttributeType': 'Lookup',
              'AttributeTypeName': {'Value': 'LookupType'},
              'DisplayName': {...},
              'RequiredLevel': {'Value': 'None', 'CanBeChanged': True, ...}
          }



.. py:class:: OneToManyRelationshipMetadata

   Metadata for a one-to-many entity relationship.

   :param schema_name: Schema name for the relationship (e.g., "new_Account_Orders").
   :type schema_name: str
   :param referenced_entity: Logical name of the referenced (parent) entity.
   :type referenced_entity: str
   :param referencing_entity: Logical name of the referencing (child) entity.
   :type referencing_entity: str
   :param referenced_attribute: Attribute on the referenced entity (typically the primary key).
   :type referenced_attribute: str
   :param cascade_configuration: Cascade behavior configuration.
   :type cascade_configuration: CascadeConfiguration
   :param referencing_attribute: Optional name for the referencing attribute (usually auto-generated).
   :type referencing_attribute: Optional[str]
   :param additional_properties: Optional dict of additional properties to include
       in the Web API payload. Useful for setting inherited properties like
       "IsValidForAdvancedFind", "IsCustomizable", "SecurityTypes", etc.
       These are merged last and can override default values.
   :type additional_properties: Optional[Dict[str, Any]]


   .. py:attribute:: schema_name
      :type:  str


   .. py:attribute:: referenced_entity
      :type:  str


   .. py:attribute:: referencing_entity
      :type:  str


   .. py:attribute:: referenced_attribute
      :type:  str


   .. py:attribute:: cascade_configuration
      :type:  CascadeConfiguration


   .. py:attribute:: referencing_attribute
      :type:  Optional[str]
      :value: None



   .. py:attribute:: additional_properties
      :type:  Optional[Dict[str, Any]]
      :value: None



   .. py:method:: to_dict() -> Dict[str, Any]

      Convert to Web API JSON format.

      Example::

          >>> rel = OneToManyRelationshipMetadata(
          ...     schema_name="new_account_orders",
          ...     referenced_entity="account",
          ...     referencing_entity="new_order",
          ...     referenced_attribute="accountid"
          ... )
          >>> rel.to_dict()
          {
              '@odata.type': 'Microsoft.Dynamics.CRM.OneToManyRelationshipMetadata',
              'SchemaName': 'new_account_orders',
              'ReferencedEntity': 'account',
              'ReferencingEntity': 'new_order',
              'ReferencedAttribute': 'accountid',
              'CascadeConfiguration': {...}
          }



.. py:class:: ManyToManyRelationshipMetadata

   Metadata for a many-to-many entity relationship.

   :param schema_name: Schema name for the relationship.
   :type schema_name: str
   :param entity1_logical_name: Logical name of the first entity.
   :type entity1_logical_name: str
   :param entity2_logical_name: Logical name of the second entity.
   :type entity2_logical_name: str
   :param intersect_entity_name: Name for the intersect table (defaults to schema_name if not provided).
   :type intersect_entity_name: Optional[str]
   :param additional_properties: Optional dict of additional properties to include
       in the Web API payload. Useful for setting inherited properties like
       "IsValidForAdvancedFind", "IsCustomizable", "SecurityTypes", or direct
       properties like "Entity1NavigationPropertyName". These are merged last
       and can override default values.
   :type additional_properties: Optional[Dict[str, Any]]


   .. py:attribute:: schema_name
      :type:  str


   .. py:attribute:: entity1_logical_name
      :type:  str


   .. py:attribute:: entity2_logical_name
      :type:  str


   .. py:attribute:: intersect_entity_name
      :type:  Optional[str]
      :value: None



   .. py:attribute:: additional_properties
      :type:  Optional[Dict[str, Any]]
      :value: None



   .. py:method:: to_dict() -> Dict[str, Any]

      Convert to Web API JSON format.

      Example::

          >>> rel = ManyToManyRelationshipMetadata(
          ...     schema_name="new_account_contact",
          ...     entity1_logical_name="account",
          ...     entity2_logical_name="contact"
          ... )
          >>> rel.to_dict()
          {
              '@odata.type': 'Microsoft.Dynamics.CRM.ManyToManyRelationshipMetadata',
              'SchemaName': 'new_account_contact',
              'Entity1LogicalName': 'account',
              'Entity2LogicalName': 'contact',
              'IntersectEntityName': 'new_account_contact'
          }



.. py:class:: RelationshipInfo

   Typed return model for relationship metadata.

   Returned by :meth:`~PowerPlatform.Dataverse.operations.tables.TableOperations.create_one_to_many_relationship`,
   :meth:`~PowerPlatform.Dataverse.operations.tables.TableOperations.create_many_to_many_relationship`,
   :meth:`~PowerPlatform.Dataverse.operations.tables.TableOperations.get_relationship`, and
   :meth:`~PowerPlatform.Dataverse.operations.tables.TableOperations.create_lookup_field`.

   :param relationship_id: Relationship metadata GUID.
   :type relationship_id: :class:`str` or None
   :param relationship_schema_name: Relationship schema name.
   :type relationship_schema_name: :class:`str`
   :param relationship_type: Either ``"one_to_many"`` or ``"many_to_many"``.
   :type relationship_type: :class:`str`
   :param lookup_schema_name: Lookup field schema name (one-to-many only).
   :type lookup_schema_name: :class:`str` or None
   :param referenced_entity: Parent entity logical name (one-to-many only).
   :type referenced_entity: :class:`str` or None
   :param referencing_entity: Child entity logical name (one-to-many only).
   :type referencing_entity: :class:`str` or None
   :param entity1_logical_name: First entity logical name (many-to-many only).
   :type entity1_logical_name: :class:`str` or None
   :param entity2_logical_name: Second entity logical name (many-to-many only).
   :type entity2_logical_name: :class:`str` or None

   Example::

       result = client.tables.create_one_to_many_relationship(lookup, relationship)
       print(result.relationship_schema_name)
       print(result.lookup_schema_name)


   .. py:attribute:: relationship_id
      :type:  Optional[str]
      :value: None



   .. py:attribute:: relationship_schema_name
      :type:  str
      :value: ''



   .. py:attribute:: relationship_type
      :type:  str
      :value: ''



   .. py:attribute:: lookup_schema_name
      :type:  Optional[str]
      :value: None



   .. py:attribute:: referenced_entity
      :type:  Optional[str]
      :value: None



   .. py:attribute:: referencing_entity
      :type:  Optional[str]
      :value: None



   .. py:attribute:: entity1_logical_name
      :type:  Optional[str]
      :value: None



   .. py:attribute:: entity2_logical_name
      :type:  Optional[str]
      :value: None



   .. py:method:: from_one_to_many(*, relationship_id: Optional[str], relationship_schema_name: str, lookup_schema_name: str, referenced_entity: str, referencing_entity: str) -> RelationshipInfo
      :classmethod:


      Create from a one-to-many relationship result.

      :param relationship_id: Relationship metadata GUID.
      :type relationship_id: :class:`str` or None
      :param relationship_schema_name: Relationship schema name.
      :type relationship_schema_name: :class:`str`
      :param lookup_schema_name: Lookup field schema name.
      :type lookup_schema_name: :class:`str`
      :param referenced_entity: Parent entity logical name.
      :type referenced_entity: :class:`str`
      :param referencing_entity: Child entity logical name.
      :type referencing_entity: :class:`str`
      :rtype: :class:`RelationshipInfo`



   .. py:method:: from_many_to_many(*, relationship_id: Optional[str], relationship_schema_name: str, entity1_logical_name: str, entity2_logical_name: str) -> RelationshipInfo
      :classmethod:


      Create from a many-to-many relationship result.

      :param relationship_id: Relationship metadata GUID.
      :type relationship_id: :class:`str` or None
      :param relationship_schema_name: Relationship schema name.
      :type relationship_schema_name: :class:`str`
      :param entity1_logical_name: First entity logical name.
      :type entity1_logical_name: :class:`str`
      :param entity2_logical_name: Second entity logical name.
      :type entity2_logical_name: :class:`str`
      :rtype: :class:`RelationshipInfo`



   .. py:method:: from_api_response(response_data: Dict[str, Any]) -> RelationshipInfo
      :classmethod:


      Create from a raw Dataverse Web API response.

      Detects one-to-many vs many-to-many from the ``@odata.type`` field
      in the response and maps PascalCase keys to snake_case attributes.
      Dataverse only supports these two relationship types; an unrecognized
      ``@odata.type`` raises :class:`ValueError`.

      :param response_data: Raw relationship metadata from the Web API.
      :type response_data: :class:`dict`
      :rtype: :class:`RelationshipInfo`
      :raises ValueError: If the ``@odata.type`` is not a recognized
          relationship type.



