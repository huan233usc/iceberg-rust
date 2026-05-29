// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashSet;
use datafusion::catalog::SchemaProvider;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use iceberg::arrow::arrow_schema_to_schema_auto_assign_ids;
use iceberg::inspect::MetadataTableType;
use iceberg::{Catalog, Error, ErrorKind, NamespaceIdent, Result, TableCreation, TableIdent};

use crate::table::IcebergTableProvider;
use crate::to_datafusion_error;

/// Represents a [`SchemaProvider`] for the Iceberg [`Catalog`], managing
/// access to table providers within a specific namespace.
///
/// Only the **set of table names** is cached at construction time.
/// [`IcebergTableProvider`]s are loaded lazily from the catalog on each
/// [`SchemaProvider::table`] call so query results always reflect the latest
/// table metadata.
///
/// The cached name set is updated by [`SchemaProvider::register_table`] and
/// [`SchemaProvider::deregister_table`]. It may still go out of sync if tables
/// are created or dropped through other means; call
/// [`IcebergSchemaProvider::try_new`] again to refresh it.
#[derive(Debug)]
pub(crate) struct IcebergSchemaProvider {
    /// Reference to the Iceberg catalog
    catalog: Arc<dyn Catalog>,
    /// The namespace this schema represents
    namespace: NamespaceIdent,
    /// Cached set of table names in this namespace, used to back the synchronous
    /// `table_names`/`table_exist` calls.
    /// Wrapped in Arc to allow sharing across async boundaries in register_table.
    table_names: Arc<DashSet<String>>,
}

impl IcebergSchemaProvider {
    /// Asynchronously tries to construct a new [`IcebergSchemaProvider`]
    /// using the given client to fetch the list of table names in the
    /// provided namespace in the Iceberg [`Catalog`].
    ///
    /// Only table names are loaded; [`IcebergTableProvider`]s are constructed
    /// lazily on demand by [`SchemaProvider::table`].
    pub(crate) async fn try_new(
        client: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
    ) -> Result<Self> {
        let table_names: DashSet<String> = client
            .list_tables(&namespace)
            .await?
            .iter()
            .map(|tbl| tbl.name().to_string())
            .collect();

        Ok(IcebergSchemaProvider {
            catalog: client,
            namespace,
            table_names: Arc::new(table_names),
        })
    }

    /// Lazily load an [`IcebergTableProvider`] for `name` from the catalog.
    ///
    /// Returns `Ok(None)` if `name` is not in the cached name set, or if the
    /// catalog reports the table no longer exists. In the latter case, the
    /// stale entry is evicted from the cache.
    async fn load_table_provider(&self, name: &str) -> DFResult<Option<IcebergTableProvider>> {
        if !self.table_names.contains(name) {
            return Ok(None);
        }

        match IcebergTableProvider::try_new(self.catalog.clone(), self.namespace.clone(), name)
            .await
        {
            Ok(provider) => Ok(Some(provider)),
            Err(err) if matches!(err.kind(), ErrorKind::TableNotFound) => {
                self.table_names.remove(name);
                Ok(None)
            }
            Err(err) => Err(to_datafusion_error(err)),
        }
    }
}

#[async_trait]
impl SchemaProvider for IcebergSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.table_names
            .iter()
            .flat_map(|entry| {
                let table_name = entry.key().clone();
                [table_name.clone()]
                    .into_iter()
                    .chain(
                        MetadataTableType::all_types().map(move |metadata_table_name| {
                            format!("{}${}", table_name, metadata_table_name.as_str())
                        }),
                    )
            })
            .collect()
    }

    fn table_exist(&self, name: &str) -> bool {
        if let Some((table_name, metadata_table_name)) = name.split_once('$') {
            self.table_names.contains(table_name)
                && MetadataTableType::try_from(metadata_table_name).is_ok()
        } else {
            self.table_names.contains(name)
        }
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        if let Some((table_name, metadata_table_name)) = name.split_once('$') {
            let metadata_table_type =
                MetadataTableType::try_from(metadata_table_name).map_err(DataFusionError::Plan)?;
            let Some(provider) = self.load_table_provider(table_name).await? else {
                return Ok(None);
            };
            let metadata_table = provider
                .metadata_table(metadata_table_type)
                .await
                .map_err(to_datafusion_error)?;
            return Ok(Some(Arc::new(metadata_table)));
        }

        Ok(self
            .load_table_provider(name)
            .await?
            .map(|p| Arc::new(p) as Arc<dyn TableProvider>))
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        // Check if table already exists
        if self.table_exist(name.as_str()) {
            return Err(DataFusionError::Execution(format!(
                "Table {name} already exists"
            )));
        }

        // Convert DataFusion schema to Iceberg schema
        // DataFusion schemas don't have field IDs, so we use the function that assigns them automatically
        let df_schema = table.schema();
        let iceberg_schema = arrow_schema_to_schema_auto_assign_ids(df_schema.as_ref())
            .map_err(to_datafusion_error)?;

        // Create the table in the Iceberg catalog
        let table_creation = TableCreation::builder()
            .name(name.clone())
            .schema(iceberg_schema)
            .build();

        let catalog = self.catalog.clone();
        let namespace = self.namespace.clone();
        let table_names = self.table_names.clone();
        let name_clone = name.clone();

        // Use tokio's spawn_blocking to handle the async work on a blocking thread pool
        let result = tokio::task::spawn_blocking(move || {
            // Create a new runtime handle to execute the async work
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async move {
                // Verify the input table is empty - CREATE TABLE only accepts schema definition
                ensure_table_is_empty(&table)
                    .await
                    .map_err(to_datafusion_error)?;

                catalog
                    .create_table(&namespace, table_creation)
                    .await
                    .map_err(to_datafusion_error)?;

                table_names.insert(name_clone);

                Ok(None)
            })
        });

        // Block on the spawned task to get the result
        // This is safe because spawn_blocking moves the blocking to a dedicated thread pool
        futures::executor::block_on(result).map_err(|e| {
            DataFusionError::Execution(format!("Failed to create Iceberg table: {e}"))
        })?
    }

    fn deregister_table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        // Check if table exists
        if !self.table_exist(name) {
            return Ok(None);
        }

        let catalog = self.catalog.clone();
        let namespace = self.namespace.clone();
        let table_names = self.table_names.clone();
        let table_name = name.to_string();

        // Use tokio's spawn_blocking to handle the async work on a blocking thread pool
        let result = tokio::task::spawn_blocking(move || {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async move {
                // Load a snapshot of the provider before dropping so we can
                // return it from this method per the SchemaProvider contract.
                // If the table has already been dropped externally we fall
                // back to `None`.
                let removed = IcebergTableProvider::try_new(
                    catalog.clone(),
                    namespace.clone(),
                    table_name.clone(),
                )
                .await
                .ok()
                .map(|p| Arc::new(p) as Arc<dyn TableProvider>);

                let table_ident = TableIdent::new(namespace, table_name.clone());

                catalog
                    .drop_table(&table_ident)
                    .await
                    .map_err(to_datafusion_error)?;

                table_names.remove(&table_name);

                Ok(removed)
            })
        });

        futures::executor::block_on(result)
            .map_err(|e| DataFusionError::Execution(format!("Failed to drop Iceberg table: {e}")))?
    }
}

/// Verifies that a table provider contains no data by scanning with LIMIT 1.
/// Returns an error if the table has any rows.
async fn ensure_table_is_empty(table: &Arc<dyn TableProvider>) -> Result<()> {
    let session_ctx = SessionContext::new();
    let exec_plan = table
        .scan(&session_ctx.state(), None, &[], Some(1))
        .await
        .map_err(|e| Error::new(ErrorKind::Unexpected, format!("Failed to scan table: {e}")))?;

    let task_ctx = Arc::new(TaskContext::default());
    let stream = exec_plan.execute(0, task_ctx).map_err(|e| {
        Error::new(
            ErrorKind::Unexpected,
            format!("Failed to execute scan: {e}"),
        )
    })?;

    let batches: Vec<_> = stream.collect().await;
    let has_data = batches
        .into_iter()
        .filter_map(|r| r.ok())
        .any(|batch| batch.num_rows() > 0);

    if has_data {
        return Err(Error::new(
            ErrorKind::Unexpected,
            "register_table does not support tables with data.",
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
    use tempfile::TempDir;

    use super::*;

    async fn create_test_schema_provider() -> (IcebergSchemaProvider, TempDir) {
        let (provider, _catalog, _ns, temp_dir) = create_test_schema_provider_with_catalog().await;
        (provider, temp_dir)
    }

    async fn create_test_schema_provider_with_catalog() -> (
        IcebergSchemaProvider,
        Arc<dyn Catalog>,
        NamespaceIdent,
        TempDir,
    ) {
        let temp_dir = TempDir::new().unwrap();
        let warehouse_path = temp_dir.path().to_str().unwrap().to_string();

        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_path.clone())]),
            )
            .await
            .unwrap();
        let catalog: Arc<dyn Catalog> = Arc::new(catalog);

        let namespace = NamespaceIdent::new("test_ns".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();

        let provider = IcebergSchemaProvider::try_new(catalog.clone(), namespace.clone())
            .await
            .unwrap();

        (provider, catalog, namespace, temp_dir)
    }

    fn iceberg_schema() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_register_table_with_data_fails() {
        let (schema_provider, _temp_dir) = create_test_schema_provider().await;

        // Create a MemTable with data
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(arrow_schema.clone(), vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ])
        .unwrap();

        let mem_table = MemTable::try_new(arrow_schema, vec![vec![batch]]).unwrap();

        // Attempt to register the table with data - should fail
        let result = schema_provider.register_table("test_table".to_string(), Arc::new(mem_table));

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("register_table does not support tables with data."),
            "Expected error about tables with data, got: {err}",
        );
    }

    #[tokio::test]
    async fn test_register_empty_table_succeeds() {
        let (schema_provider, _temp_dir) = create_test_schema_provider().await;

        // Create an empty MemTable (schema only, no data rows)
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        // Create an empty batch (0 rows) - MemTable requires at least one partition
        let empty_batch = RecordBatch::new_empty(arrow_schema.clone());
        let mem_table = MemTable::try_new(arrow_schema, vec![vec![empty_batch]]).unwrap();

        // Attempt to register the empty table - should succeed
        let result = schema_provider.register_table("empty_table".to_string(), Arc::new(mem_table));

        assert!(result.is_ok(), "Expected success, got: {result:?}");

        // Verify the table was registered
        assert!(schema_provider.table_exist("empty_table"));
    }

    #[tokio::test]
    async fn test_register_duplicate_table_fails() {
        let (schema_provider, _temp_dir) = create_test_schema_provider().await;

        // Create empty MemTables
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));

        let empty_batch1 = RecordBatch::new_empty(arrow_schema.clone());
        let empty_batch2 = RecordBatch::new_empty(arrow_schema.clone());
        let mem_table1 = MemTable::try_new(arrow_schema.clone(), vec![vec![empty_batch1]]).unwrap();
        let mem_table2 = MemTable::try_new(arrow_schema, vec![vec![empty_batch2]]).unwrap();

        // Register first table - should succeed
        let result1 = schema_provider.register_table("dup_table".to_string(), Arc::new(mem_table1));
        assert!(result1.is_ok());

        // Register second table with same name - should fail
        let result2 = schema_provider.register_table("dup_table".to_string(), Arc::new(mem_table2));
        assert!(result2.is_err());
        let err = result2.unwrap_err();
        assert!(
            err.to_string().contains("already exists"),
            "Expected error about table already existing, got: {err}",
        );
    }

    #[tokio::test]
    async fn test_deregister_table_succeeds() {
        let (schema_provider, _temp_dir) = create_test_schema_provider().await;

        // Create and register an empty table
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));

        let empty_batch = RecordBatch::new_empty(arrow_schema.clone());
        let mem_table = MemTable::try_new(arrow_schema, vec![vec![empty_batch]]).unwrap();

        // Register the table
        let result = schema_provider.register_table("drop_me".to_string(), Arc::new(mem_table));
        assert!(result.is_ok());
        assert!(schema_provider.table_exist("drop_me"));

        // Deregister the table
        let result = schema_provider.deregister_table("drop_me");
        assert!(result.is_ok());
        assert!(result.unwrap().is_some());

        // Verify the table no longer exists
        assert!(!schema_provider.table_exist("drop_me"));
    }

    #[tokio::test]
    async fn test_deregister_nonexistent_table_returns_none() {
        let (schema_provider, _temp_dir) = create_test_schema_provider().await;

        // Attempt to deregister a table that doesn't exist
        let result = schema_provider.deregister_table("nonexistent");
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_table_lazy_loads_existing_table() {
        // Create a table directly via the catalog before constructing the
        // schema provider so we can verify that `try_new` only loads the
        // table name (not the provider) and that `table()` lazy-loads on
        // demand.
        let temp_dir = TempDir::new().unwrap();
        let warehouse_path = temp_dir.path().to_str().unwrap().to_string();

        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse_path.clone())]),
            )
            .await
            .unwrap();
        let catalog: Arc<dyn Catalog> = Arc::new(catalog);

        let namespace = NamespaceIdent::new("test_ns".to_string());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .unwrap();

        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("preexisting".to_string())
                    .schema(iceberg_schema())
                    .build(),
            )
            .await
            .unwrap();

        let schema_provider = IcebergSchemaProvider::try_new(catalog.clone(), namespace.clone())
            .await
            .unwrap();

        assert!(schema_provider.table_exist("preexisting"));

        let provider = schema_provider.table("preexisting").await.unwrap();
        assert!(provider.is_some(), "expected lazy-loaded table provider");
        assert_eq!(
            provider.unwrap().schema().fields().len(),
            1,
            "lazy-loaded provider should expose the table schema",
        );
    }

    #[tokio::test]
    async fn test_table_returns_none_for_unknown_table() {
        let (schema_provider, _temp_dir) = create_test_schema_provider().await;

        let result = schema_provider.table("does_not_exist").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_table_evicts_externally_dropped_entry() {
        // When a table is removed from the catalog out-of-band, the next
        // `table()` call should report it as missing and evict the stale
        // entry from the local name cache.
        let (schema_provider, catalog, namespace, _temp_dir) =
            create_test_schema_provider_with_catalog().await;

        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("ghost".to_string())
                    .schema(iceberg_schema())
                    .build(),
            )
            .await
            .unwrap();

        // Manually seed the cache, simulating a previous `try_new` snapshot
        // that observed this table.
        schema_provider.table_names.insert("ghost".to_string());
        assert!(schema_provider.table_exist("ghost"));

        catalog
            .drop_table(&TableIdent::new(namespace.clone(), "ghost".to_string()))
            .await
            .unwrap();

        let result = schema_provider.table("ghost").await.unwrap();
        assert!(result.is_none(), "stale entry should resolve to None");
        assert!(
            !schema_provider.table_exist("ghost"),
            "stale entry should be evicted after a missing lookup",
        );
    }
}
