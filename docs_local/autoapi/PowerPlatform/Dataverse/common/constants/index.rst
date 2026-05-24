PowerPlatform.Dataverse.common.constants
========================================

.. py:module:: PowerPlatform.Dataverse.common.constants

.. autoapi-nested-parse::

   Constants for Dataverse Web API metadata types.

   These constants define the OData type identifiers used in Web API payloads
   for metadata operations.



Attributes
----------

.. autoapisummary::

   PowerPlatform.Dataverse.common.constants.ODATA_TYPE_LOCALIZED_LABEL
   PowerPlatform.Dataverse.common.constants.ODATA_TYPE_LABEL
   PowerPlatform.Dataverse.common.constants.ODATA_TYPE_LOOKUP_ATTRIBUTE
   PowerPlatform.Dataverse.common.constants.ODATA_TYPE_ONE_TO_MANY_RELATIONSHIP
   PowerPlatform.Dataverse.common.constants.ODATA_TYPE_MANY_TO_MANY_RELATIONSHIP
   PowerPlatform.Dataverse.common.constants.CASCADE_BEHAVIOR_CASCADE
   PowerPlatform.Dataverse.common.constants.CASCADE_BEHAVIOR_NO_CASCADE
   PowerPlatform.Dataverse.common.constants.CASCADE_BEHAVIOR_REMOVE_LINK
   PowerPlatform.Dataverse.common.constants.CASCADE_BEHAVIOR_RESTRICT


Module Contents
---------------

.. py:data:: ODATA_TYPE_LOCALIZED_LABEL
   :value: 'Microsoft.Dynamics.CRM.LocalizedLabel'


.. py:data:: ODATA_TYPE_LABEL
   :value: 'Microsoft.Dynamics.CRM.Label'


.. py:data:: ODATA_TYPE_LOOKUP_ATTRIBUTE
   :value: 'Microsoft.Dynamics.CRM.LookupAttributeMetadata'


.. py:data:: ODATA_TYPE_ONE_TO_MANY_RELATIONSHIP
   :value: 'Microsoft.Dynamics.CRM.OneToManyRelationshipMetadata'


.. py:data:: ODATA_TYPE_MANY_TO_MANY_RELATIONSHIP
   :value: 'Microsoft.Dynamics.CRM.ManyToManyRelationshipMetadata'


.. py:data:: CASCADE_BEHAVIOR_CASCADE
   :value: 'Cascade'


   Perform the action on all referencing table records associated with the referenced table record.

.. py:data:: CASCADE_BEHAVIOR_NO_CASCADE
   :value: 'NoCascade'


   Do not apply the action to any referencing table records associated with the referenced table record.

.. py:data:: CASCADE_BEHAVIOR_REMOVE_LINK
   :value: 'RemoveLink'


   Remove the value of the referencing column for all referencing table records when the referenced record is deleted.

.. py:data:: CASCADE_BEHAVIOR_RESTRICT
   :value: 'Restrict'


   Prevent the referenced table record from being deleted when referencing table records exist.

