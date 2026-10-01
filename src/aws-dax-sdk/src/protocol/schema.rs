//! Bounded preloaded schema state for Phase 3 protocol codecs.

use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    sync::{Arc, Mutex},
};

use aws_sdk_dynamodb::types::AttributeDefinition;
use tokio::sync::Notify;

use crate::Error;

const KEY_SCHEMA_CAPACITY: usize = 100;
const ATTRIBUTE_LIST_CAPACITY: usize = 1_000;
const EMPTY_ATTRIBUTE_LIST_ID: i64 = 1;

/// Cache-state failures that require a later control request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SchemaError {
    /// No preloaded key schema is available for this table.
    MissingKeySchema(String),
    /// No preloaded attribute-list ID is available for these sorted names.
    MissingAttributeListId(Vec<String>),
    /// No preloaded attribute list is available for this ID.
    MissingAttributeList(i64),
    /// Attribute-list names were not in canonical sorted order.
    UnsortedAttributeNames,
    /// Attribute list ID one is reserved for the empty attribute list.
    ReservedEmptyAttributeListId,
    /// Another schema-registry user panicked while holding its lock.
    Poisoned,
}

/// Concurrency-safe, bounded protocol schema state.
///
/// Cache misses are loaded by [`super::control::ControlResolver`] through the
/// control operation executor. This registry owns only explicit, validated
/// state and single-flight coordination for those loads.
#[derive(Clone, Debug, Default)]
pub(crate) struct SchemaRegistry {
    inner: Arc<Mutex<RegistryState>>,
}

#[derive(Debug, Default)]
struct RegistryState {
    key_schemas: BoundedMap<String, Vec<AttributeDefinition>>,
    names_to_id: BoundedMap<Vec<String>, i64>,
    id_to_names: BoundedMap<i64, Vec<String>>,
    loads: HashMap<LoadKey, Arc<LoadState>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum LoadKey {
    KeySchema(String),
    AttributeListId(Vec<String>),
    AttributeList(i64),
}

#[derive(Debug, Clone)]
enum LoadedValue {
    KeySchema(Vec<AttributeDefinition>),
    AttributeListId(i64),
    AttributeList(Vec<String>),
}

#[derive(Debug, Default)]
struct LoadState {
    result: Mutex<Option<Result<LoadedValue, Error>>>,
    completed: Notify,
}

struct LoadLeaderGuard {
    registry: SchemaRegistry,
    key: LoadKey,
    flight: Arc<LoadState>,
    completed: bool,
}

impl LoadLeaderGuard {
    fn new(registry: SchemaRegistry, key: LoadKey, flight: Arc<LoadState>) -> Self {
        Self {
            registry,
            key,
            flight,
            completed: false,
        }
    }

    fn complete(&mut self, result: Result<LoadedValue, Error>) -> Result<(), Error> {
        let mut state = self.registry.state().map_err(schema_error)?;
        state.loads.remove(&self.key);
        drop(state);

        let mut shared = self.flight.result.lock().map_err(|_| schema_poisoned())?;
        *shared = Some(result);
        self.completed = true;
        drop(shared);
        self.flight.completed.notify_waiters();
        Ok(())
    }
}

impl Drop for LoadLeaderGuard {
    fn drop(&mut self) {
        if self.completed {
            return;
        }

        let mut state = self
            .registry
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state
            .loads
            .get(&self.key)
            .is_some_and(|flight| Arc::ptr_eq(flight, &self.flight))
        {
            state.loads.remove(&self.key);
        }
        drop(state);

        let mut shared = self
            .flight
            .result
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if shared.is_none() {
            *shared = Some(Err(Error::Validation {
                message: "schema cache load was cancelled".into(),
            }));
        }
        drop(shared);
        self.flight.completed.notify_waiters();
    }
}

impl SchemaRegistry {
    /// Adds or replaces a table key schema.
    pub(crate) fn insert_key_schema(
        &self,
        table: String,
        schema: Vec<AttributeDefinition>,
    ) -> Result<(), SchemaError> {
        self.state()?
            .key_schemas
            .insert(table, schema, KEY_SCHEMA_CAPACITY);
        Ok(())
    }

    /// Returns the preloaded key schema for a table.
    pub(crate) fn key_schema(&self, table: &str) -> Result<Vec<AttributeDefinition>, SchemaError> {
        self.state()?
            .key_schemas
            .get(&table.to_owned())
            .cloned()
            .ok_or_else(|| SchemaError::MissingKeySchema(table.into()))
    }

    /// Adds a bidirectional non-empty attribute-list mapping.
    pub(crate) fn insert_attribute_list(
        &self,
        id: i64,
        names: Vec<String>,
    ) -> Result<(), SchemaError> {
        if id == EMPTY_ATTRIBUTE_LIST_ID {
            return Err(SchemaError::ReservedEmptyAttributeListId);
        }
        if !is_sorted(&names) {
            return Err(SchemaError::UnsortedAttributeNames);
        }
        let mut state = self.state()?;
        state
            .names_to_id
            .insert(names.clone(), id, ATTRIBUTE_LIST_CAPACITY);
        state.id_to_names.insert(id, names, ATTRIBUTE_LIST_CAPACITY);
        Ok(())
    }

    /// Resolves a sorted attribute-name list to a DAX schema ID.
    pub(crate) fn attribute_list_id(&self, names: &[String]) -> Result<i64, SchemaError> {
        if names.is_empty() {
            return Ok(EMPTY_ATTRIBUTE_LIST_ID);
        }
        if !is_sorted(names) {
            return Err(SchemaError::UnsortedAttributeNames);
        }
        self.state()?
            .names_to_id
            .get(&names.to_vec())
            .copied()
            .ok_or_else(|| SchemaError::MissingAttributeListId(names.to_vec()))
    }

    /// Resolves a DAX schema ID to a sorted attribute-name list.
    pub(crate) fn attribute_list(&self, id: i64) -> Result<Vec<String>, SchemaError> {
        if id == EMPTY_ATTRIBUTE_LIST_ID {
            return Ok(Vec::new());
        }
        self.state()?
            .id_to_names
            .get(&id)
            .cloned()
            .ok_or(SchemaError::MissingAttributeList(id))
    }

    /// Resolves a table schema from cache or performs one shared cache-miss load.
    pub(crate) async fn load_key_schema<Loader, LoadFuture>(
        &self,
        table: &str,
        loader: Loader,
    ) -> Result<Vec<AttributeDefinition>, Error>
    where
        Loader: FnOnce() -> LoadFuture,
        LoadFuture: Future<Output = Result<Vec<AttributeDefinition>, Error>>,
    {
        let table = table.to_owned();
        let table_for_store = table.clone();
        self.load(
            LoadKey::KeySchema(table.clone()),
            |state| {
                state
                    .key_schemas
                    .get(&table)
                    .cloned()
                    .map(LoadedValue::KeySchema)
            },
            |state, value| match value {
                LoadedValue::KeySchema(schema) => {
                    state
                        .key_schemas
                        .insert(table_for_store, schema, KEY_SCHEMA_CAPACITY);
                    Ok(())
                }
                _ => unreachable!("key-schema loader returns a key schema"),
            },
            || async move { loader().await.map(LoadedValue::KeySchema) },
        )
        .await
        .map(|value| match value {
            LoadedValue::KeySchema(schema) => schema,
            _ => unreachable!("key-schema cache contains a key schema"),
        })
    }

    /// Resolves a canonical attribute-name list ID through one shared miss load.
    pub(crate) async fn load_attribute_list_id<Loader, LoadFuture>(
        &self,
        names: &[String],
        loader: Loader,
    ) -> Result<i64, Error>
    where
        Loader: FnOnce() -> LoadFuture,
        LoadFuture: Future<Output = Result<i64, Error>>,
    {
        if names.is_empty() {
            return Ok(EMPTY_ATTRIBUTE_LIST_ID);
        }
        if !is_sorted(names) {
            return Err(schema_validation("attribute-list names must be sorted"));
        }
        let names = names.to_vec();
        let names_for_store = names.clone();
        self.load(
            LoadKey::AttributeListId(names.clone()),
            |state| {
                state
                    .names_to_id
                    .get(&names)
                    .copied()
                    .map(LoadedValue::AttributeListId)
            },
            |state, value| match value {
                LoadedValue::AttributeListId(id) if id != EMPTY_ATTRIBUTE_LIST_ID => {
                    state
                        .names_to_id
                        .insert(names_for_store, id, ATTRIBUTE_LIST_CAPACITY);
                    Ok(())
                }
                LoadedValue::AttributeListId(_) => Err(schema_validation(
                    "non-empty attribute list cannot use reserved ID 1",
                )),
                _ => unreachable!("attribute-list-ID loader returns an ID"),
            },
            || async move { loader().await.map(LoadedValue::AttributeListId) },
        )
        .await
        .map(|value| match value {
            LoadedValue::AttributeListId(id) => id,
            _ => unreachable!("attribute-list-ID cache contains an ID"),
        })
    }

    /// Resolves an attribute-name list through one shared cache-miss load.
    pub(crate) async fn load_attribute_list<Loader, LoadFuture>(
        &self,
        id: i64,
        loader: Loader,
    ) -> Result<Vec<String>, Error>
    where
        Loader: FnOnce() -> LoadFuture,
        LoadFuture: Future<Output = Result<Vec<String>, Error>>,
    {
        if id == EMPTY_ATTRIBUTE_LIST_ID {
            return Ok(Vec::new());
        }
        self.load(
            LoadKey::AttributeList(id),
            |state| {
                state
                    .id_to_names
                    .get(&id)
                    .cloned()
                    .map(LoadedValue::AttributeList)
            },
            |state, value| match value {
                LoadedValue::AttributeList(names) if is_sorted(&names) => {
                    state.id_to_names.insert(id, names, ATTRIBUTE_LIST_CAPACITY);
                    Ok(())
                }
                LoadedValue::AttributeList(_) => {
                    Err(schema_validation("attribute-list names must be sorted"))
                }
                _ => unreachable!("attribute-list loader returns names"),
            },
            || async move { loader().await.map(LoadedValue::AttributeList) },
        )
        .await
        .map(|value| match value {
            LoadedValue::AttributeList(names) => names,
            _ => unreachable!("attribute-list cache contains names"),
        })
    }

    async fn load<Loader, LoadFuture>(
        &self,
        key: LoadKey,
        cached: impl FnOnce(&RegistryState) -> Option<LoadedValue>,
        store: impl FnOnce(&mut RegistryState, LoadedValue) -> Result<(), Error>,
        loader: Loader,
    ) -> Result<LoadedValue, Error>
    where
        Loader: FnOnce() -> LoadFuture,
        LoadFuture: Future<Output = Result<LoadedValue, Error>>,
    {
        let (flight, leader) = {
            let mut state = self.state().map_err(schema_error)?;
            if let Some(value) = cached(&state) {
                return Ok(value);
            }
            if let Some(flight) = state.loads.get(&key) {
                (flight.clone(), false)
            } else {
                let flight = Arc::new(LoadState::default());
                state.loads.insert(key.clone(), flight.clone());
                (flight, true)
            }
        };

        if !leader {
            loop {
                let notified = flight.completed.notified();
                if let Some(result) = flight.result.lock().map_err(|_| schema_poisoned())?.clone() {
                    return result;
                }
                notified.await;
            }
        }

        let mut leader = LoadLeaderGuard::new(self.clone(), key, flight);
        let result = loader().await.and_then(|value| {
            let mut state = self.state().map_err(schema_error)?;
            store(&mut state, value.clone())?;
            Ok(value)
        });
        leader.complete(result.clone())?;
        result
    }

    fn state(&self) -> Result<std::sync::MutexGuard<'_, RegistryState>, SchemaError> {
        self.inner.lock().map_err(|_| SchemaError::Poisoned)
    }
}

fn schema_error(error: SchemaError) -> Error {
    Error::Validation {
        message: error.to_string(),
    }
}

fn schema_validation(message: &str) -> Error {
    Error::Validation {
        message: message.into(),
    }
}

fn schema_poisoned() -> Error {
    schema_error(SchemaError::Poisoned)
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingKeySchema(table) => {
                write!(formatter, "key schema is not loaded for `{table}`")
            }
            Self::MissingAttributeListId(names) => {
                write!(formatter, "attribute-list ID is not loaded for {names:?}")
            }
            Self::MissingAttributeList(id) => {
                write!(formatter, "attribute-list `{id}` is not loaded")
            }
            Self::UnsortedAttributeNames => {
                write!(formatter, "attribute-list names must be sorted")
            }
            Self::ReservedEmptyAttributeListId => {
                write!(
                    formatter,
                    "attribute-list ID 1 is reserved for the empty list"
                )
            }
            Self::Poisoned => write!(formatter, "schema registry lock is poisoned"),
        }
    }
}

#[derive(Debug, Default)]
struct BoundedMap<Key, Value> {
    values: HashMap<Key, Value>,
    insertion_order: VecDeque<Key>,
}

impl<Key, Value> BoundedMap<Key, Value>
where
    Key: Clone + Eq + std::hash::Hash,
{
    fn insert(&mut self, key: Key, value: Value, capacity: usize) {
        if !self.values.contains_key(&key) {
            self.insertion_order.push_back(key.clone());
        }
        self.values.insert(key, value);
        if self.values.len() > capacity {
            let oldest = self
                .insertion_order
                .pop_front()
                .expect("insertion order exists for every cache entry");
            self.values.remove(&oldest);
        }
    }

    fn get(&self, key: &Key) -> Option<&Value> {
        self.values.get(key)
    }
}

fn is_sorted(names: &[String]) -> bool {
    names.windows(2).all(|pair| pair[0] <= pair[1])
}

#[cfg(test)]
mod tests {
    use aws_sdk_dynamodb::types::{AttributeDefinition, ScalarAttributeType};

    use super::{SchemaError, SchemaRegistry};

    #[test]
    fn reserves_empty_attribute_list_and_requires_sorted_names() {
        let registry = SchemaRegistry::default();
        assert_eq!(registry.attribute_list_id(&[]).unwrap(), 1);
        assert_eq!(registry.attribute_list(1).unwrap(), Vec::<String>::new());
        assert_eq!(
            registry.insert_attribute_list(2, vec!["z".into(), "a".into()]),
            Err(SchemaError::UnsortedAttributeNames)
        );
        assert_eq!(
            registry.insert_attribute_list(1, vec!["a".into()]),
            Err(SchemaError::ReservedEmptyAttributeListId)
        );
    }

    #[test]
    fn resolves_preloaded_schema_state_bidirectionally() {
        let registry = SchemaRegistry::default();
        let schema = vec![
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("schema is complete"),
        ];
        registry
            .insert_key_schema("Table".into(), schema.clone())
            .unwrap();
        registry
            .insert_attribute_list(9, vec!["a".into(), "z".into()])
            .unwrap();

        assert_eq!(registry.key_schema("Table").unwrap(), schema);
        assert_eq!(
            registry
                .attribute_list_id(&["a".into(), "z".into()])
                .unwrap(),
            9
        );
        assert_eq!(
            registry.attribute_list(9).unwrap(),
            vec!["a".to_owned(), "z".to_owned()]
        );
        assert_eq!(
            registry.key_schema("Missing").unwrap_err(),
            SchemaError::MissingKeySchema("Missing".into())
        );
    }

    #[test]
    fn evicts_the_oldest_key_schema_at_the_reference_capacity() {
        let registry = SchemaRegistry::default();
        let schema = vec![
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("schema is complete"),
        ];
        for number in 0..101 {
            registry
                .insert_key_schema(format!("Table{number}"), schema.clone())
                .unwrap();
        }

        assert_eq!(
            registry.key_schema("Table0").unwrap_err(),
            SchemaError::MissingKeySchema("Table0".into())
        );
        assert_eq!(registry.key_schema("Table100").unwrap(), schema);
    }
}
