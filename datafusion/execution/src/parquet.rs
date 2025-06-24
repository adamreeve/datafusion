use arrow::datatypes::SchemaRef;
use dashmap::DashMap;
use datafusion_common::DataFusionError;
use object_store::path::Path;
use parquet::encryption::decrypt::FileDecryptionProperties;
use parquet::encryption::encrypt::FileEncryptionProperties;
use std::sync::Arc;
use datafusion_common::config::Extensions;

/// Trait for types that generate file encryption and decryption properties to
/// write and read encrypted Parquet files.
/// This allows flexibility in how encryption keys are managed, for example, to
/// integrate with a user's key management service (KMS).
pub trait EncryptionFactory: Send + Sync + std::fmt::Debug + 'static {
    /// Generate file encryption properties to use when writing a Parquet file.
    fn get_file_encryption_properties(
        &self,
        extensions: &Extensions,
        schema: &SchemaRef,
        file_path: &Path,
    ) -> datafusion_common::Result<Option<FileEncryptionProperties>>;

    /// Generate file decryption properties to use when reading a Parquet file.
    fn get_file_decryption_properties(
        &self,
        extensions: &Extensions,
        file_path: &Path,
    ) -> datafusion_common::Result<Option<FileDecryptionProperties>>;
}

#[derive(Clone, Debug, Default)]
pub struct EncryptionFactoryRegistry {
    factories: DashMap<String, Arc<dyn EncryptionFactory>>,
}

impl EncryptionFactoryRegistry {
    pub fn register_factory(
        &self,
        id: &str,
        factory: Arc<dyn EncryptionFactory>,
    ) -> Option<Arc<dyn EncryptionFactory>> {
        self.factories.insert(id.to_owned(), factory)
    }

    pub fn get_factory(
        &self,
        id: &str,
    ) -> datafusion_common::Result<Arc<dyn EncryptionFactory>> {
        self.factories
            .get(id)
            .map(|f| f.clone())
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "No Parquet encryption factory found for id '{id}'"
                ))
            })
    }
}
