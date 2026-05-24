PowerPlatform.Dataverse.migration.migrate_v0_to_v1
==================================================

.. py:module:: PowerPlatform.Dataverse.migration.migrate_v0_to_v1

.. autoapi-nested-parse::

   DV-Python-SDK v0 -> v1 GA migration codemod.

   Mechanically rewrites beta (0.1.0b*) call sites to their GA (1.0) equivalents
   using LibCST (concrete syntax tree — preserves all whitespace and comments).

   Usage::

       pip install PowerPlatform-Dataverse-Client[migration]
       dataverse-migrate path/to/your/scripts/
       dataverse-migrate path/to/your/scripts/ --dry-run          # preview without writing
       dataverse-migrate path/to/your/scripts/ --client-var=svc   # if client is named 'svc'

       # Or via module for development installs:
       python -m PowerPlatform.Dataverse.migration.migrate_v0_to_v1 path/to/your/scripts/

   Transformations applied
   -----------------------
   Builder methods (.filter_*  ->  .where(col(...)...))::

       .filter_eq("col", v)               ->  .where(col("col") == v)
       .filter_ne("col", v)               ->  .where(col("col") != v)
       .filter_gt("col", v)               ->  .where(col("col") > v)
       .filter_ge("col", v)               ->  .where(col("col") >= v)
       .filter_lt("col", v)               ->  .where(col("col") < v)
       .filter_le("col", v)               ->  .where(col("col") <= v)
       .filter_contains("col", v)         ->  .where(col("col").contains(v))
       .filter_startswith("col", v)       ->  .where(col("col").startswith(v))
       .filter_endswith("col", v)         ->  .where(col("col").endswith(v))
       .filter_in("col", vals)            ->  .where(col("col").in_(vals))
       .filter_not_in("col", vals)        ->  .where(col("col").not_in(vals))
       .filter_null("col")                ->  .where(col("col").is_null())
       .filter_not_null("col")            ->  .where(col("col").is_not_null())
       .filter_between("col", lo, hi)     ->  .where(col("col").between(lo, hi))
       .filter_not_between("col", lo, hi) ->  .where(col("col").not_between(lo, hi))
       .filter_raw("expr")                ->  .where(raw("expr"))
       .filter("expr")                    ->  .where(raw("expr"))
       .execute(by_page=True)             ->  .execute_pages()
       .execute(by_page=False)            ->  .execute()  (flag removed)
       <builder_chain>.to_dataframe()     ->  <builder_chain>.execute().to_dataframe()
           Inserts .execute() when the receiver is a recognised QueryBuilder chain
           (contains .builder(), .select(), .where(), or a .filter_*() call).

   Record namespace::

       batch.records.get(t, id)     ->  batch.records.retrieve(t, id)

   Top-level shortcuts (removed at GA)::

       client.create(t, d)           ->  client.records.create(t, d)
       client.update(t, id, d)       ->  client.records.update(t, id, d)
       client.delete(t, id)          ->  client.records.delete(t, id)
       client.get(t, id)             ->  client.records.get(t, id)  [deprecated; see manual section]
       client.query_sql(sql)         ->  client.query.sql(sql)
       client.get_table_info(t)      ->  client.tables.get(t)
       client.create_table(t, …)     ->  client.tables.create(t, …)
       client.delete_table(t)        ->  client.tables.delete(t)
       client.list_tables()          ->  client.tables.list()
       client.create_columns(t, …)   ->  client.tables.add_columns(t, …)
       client.delete_columns(t, …)   ->  client.tables.remove_columns(t, …)
       client.upload_file(…)         ->  client.files.upload(…)

   Import management:
       Adds ``from PowerPlatform.Dataverse.models.filters import col`` when a
       .filter_* method is rewritten (if col is not already imported).
       Adds ``raw`` to the same import when .filter_raw or .filter is rewritten.

   NOT handled by this codemod (manual migration required):
       execute(by_page=variable)      ->  manual review required (variable argument, not literal)
       client.records.get(t, id)     ->  client.records.retrieve(t, id)
           Return type changes: beta returns Record (raises on 404); GA retrieve() returns
           Record | None. Callers that do not guard against None will fail silently.
       client.records.get(t, kw=…)  ->  client.records.list(t, kw=…)
           Return type changes: beta returns Iterable[List[Record]] (pages); GA list()
           returns QueryResult (flat iterable over Records). Any ``for page in result:
           for rec in page:`` iteration pattern breaks after a mechanical rename.
       client.dataframe.get()        ->  client.query.builder(…).execute().to_dataframe()
           Expression reconstruction requires understanding caller intent.
       client.query.sql_select()/sql_join()/sql_joins()  ->  removed (no mechanical replacement)



Functions
---------

.. autoapisummary::

   PowerPlatform.Dataverse.migration.migrate_v0_to_v1.find_manual_patterns
   PowerPlatform.Dataverse.migration.migrate_v0_to_v1.migrate_source
   PowerPlatform.Dataverse.migration.migrate_v0_to_v1.migrate_file
   PowerPlatform.Dataverse.migration.migrate_v0_to_v1.main


Module Contents
---------------

.. py:function:: find_manual_patterns(source: str, *, client_var: str = 'client') -> List[str]

   Return descriptions of patterns in *source* that require manual migration.


.. py:function:: migrate_source(source: str, *, client_var: str = 'client') -> str

   Parse *source*, apply transformations, return migrated source.


.. py:function:: migrate_file(path: pathlib.Path, *, dry_run: bool = False, client_var: str = 'client') -> Tuple[bool, List[str]]

   Migrate *path* in place. Returns (was_changed, manual_review_notes).


.. py:function:: main(argv: Optional[List[str]] = None) -> int

