//! DAX request stream encoders for the initial Phase 3 operation slice.

use aws_sdk_dynamodb::{
    operation::{
        batch_get_item::BatchGetItemInput, batch_write_item::BatchWriteItemInput,
        delete_item::DeleteItemInput, get_item::GetItemInput, put_item::PutItemInput,
        query::QueryInput, scan::ScanInput, transact_get_items::TransactGetItemsInput,
        transact_write_items::TransactWriteItemsInput, update_item::UpdateItemInput,
    },
    types::{
        AttributeDefinition, ReturnConsumedCapacity, ReturnItemCollectionMetrics, ReturnValue,
        Select,
    },
};

use super::cbor::{
    CborError, encode_attribute_value, encode_item_key, encode_item_non_key_attributes,
    write_bytes, write_type,
};
use super::schema::{SchemaError, SchemaRegistry};

const DAX_SERVICE_ID: i64 = 1;
const GET_ITEM_METHOD_ID: i64 = 263_244_906;
const PUT_ITEM_METHOD_ID: i64 = -2_106_490_455;
const DELETE_ITEM_METHOD_ID: i64 = 1_013_539_361;
const UPDATE_ITEM_METHOD_ID: i64 = 1_425_579_023;
const BATCH_WRITE_ITEM_METHOD_ID: i64 = 116_217_951;
const BATCH_GET_ITEM_METHOD_ID: i64 = -697_851_100;
const TRANSACT_GET_ITEMS_METHOD_ID: i64 = 1_866_287_579;
const TRANSACT_WRITE_ITEMS_METHOD_ID: i64 = -1_160_037_738;
pub(crate) const ENDPOINTS_METHOD_ID: i64 = 455_855_874;
const SCAN_METHOD_ID: i64 = -1_875_390_620;
const QUERY_METHOD_ID: i64 = -931_250_863;
const DEFINE_KEY_SCHEMA_METHOD_ID: i64 = -742_646_399;
const DEFINE_ATTRIBUTE_LIST_ID_METHOD_ID: i64 = -1_230_579_644;
const DEFINE_ATTRIBUTE_LIST_METHOD_ID: i64 = 670_678_385;

const MAJOR_UNSIGNED: u8 = 0x00;
const MAJOR_NEGATIVE: u8 = 0x20;
const MAJOR_ARRAY: u8 = 0x80;
const MAJOR_TEXT: u8 = 0x60;
const MAJOR_MAP: u8 = 0xa0;
const MAJOR_TAG: u8 = 0xc0;
const DOCUMENT_PATH_LIST_INDEX_TAG: u64 = 3324;

/// An invalid or not-yet-supported DAX request encoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RequestError {
    /// A required AWS input field was absent.
    MissingRequiredField(&'static str),
    /// The operation needs an expression parser that has not been ported yet.
    UnsupportedExpression(&'static str),
    /// Attribute-value or item-key encoding failed.
    Cbor(CborError),
    /// Required schema state has not been loaded by a control operation.
    Schema(SchemaError),
}

#[derive(Clone, Copy)]
enum UpdateAction<'a> {
    SetValue {
        attribute: &'a str,
        placeholder: &'a str,
    },
    SetArithmetic {
        attribute: &'a str,
        operator: i64,
        placeholder: &'a str,
    },
    SetIfNotExists {
        attribute: &'a str,
        placeholder: &'a str,
    },
    SetListAppend {
        attribute: &'a str,
        placeholder: &'a str,
    },
    Add {
        attribute: &'a str,
        placeholder: &'a str,
    },
    Delete {
        attribute: &'a str,
        placeholder: &'a str,
    },
    Remove {
        attribute: &'a str,
    },
}

impl UpdateAction<'_> {
    fn placeholder(&self) -> Option<&str> {
        match self {
            Self::SetValue { placeholder, .. }
            | Self::SetArithmetic { placeholder, .. }
            | Self::SetIfNotExists { placeholder, .. }
            | Self::SetListAppend { placeholder, .. }
            | Self::Add { placeholder, .. }
            | Self::Delete { placeholder, .. } => Some(placeholder),
            Self::Remove { .. } => None,
        }
    }
}

fn query_attribute_value_placeholders(expression: &str) -> std::collections::HashSet<String> {
    let mut used = std::collections::HashSet::new();
    let mut characters = expression.chars().peekable();
    while let Some(character) = characters.next() {
        if character != ':' {
            continue;
        }
        let mut placeholder = String::from(":");
        while let Some(character) =
            characters.next_if(|character| character.is_ascii_alphanumeric() || *character == '_')
        {
            placeholder.push(character);
        }
        if placeholder.len() > 1 {
            used.insert(placeholder);
        }
    }
    used
}

fn contains_top_level_comma(expression: &str) -> bool {
    let mut depth = 0usize;
    for character in expression.chars() {
        match character {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => return true,
            _ => {}
        }
    }
    false
}

fn split_update_clauses(expression: &str) -> Vec<&str> {
    let mut clauses = Vec::new();
    let mut start = 0;
    let mut depth: usize = 0;
    for (index, character) in expression.char_indices() {
        match character {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                clauses.push(expression[start..index].trim());
                start = index + character.len_utf8();
            }
            _ => {}
        }
    }
    clauses.push(expression[start..].trim());
    clauses
}

fn update_section_at(expression: &str, index: usize) -> Option<&'static str> {
    for (section, keyword) in [
        ("SET", "SET"),
        ("REMOVE", "REMOVE"),
        ("ADD", "ADD"),
        ("DELETE", "DELETE"),
    ] {
        let end = index + keyword.len();
        if expression.get(index..end)?.eq_ignore_ascii_case(keyword)
            && expression
                .as_bytes()
                .get(end)
                .is_some_and(u8::is_ascii_whitespace)
        {
            return Some(section);
        }
    }
    None
}

fn parse_update_actions(expression: &str) -> Result<Vec<UpdateAction<'_>>, RequestError> {
    let expression = expression.trim();
    let mut sections = Vec::new();
    let mut section_start = None;
    let mut section_name = None;
    let mut section_order = 0usize;
    let mut seen_sections = std::collections::HashSet::new();
    let mut depth: usize = 0;
    for (index, character) in expression.char_indices() {
        match character {
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            _ if depth == 0
                && (index == 0 || expression.as_bytes()[index - 1].is_ascii_whitespace()) =>
            {
                if let Some(section) = update_section_at(expression, index) {
                    let section_rank = match section {
                        "SET" => 0,
                        "REMOVE" => 1,
                        "ADD" => 2,
                        "DELETE" => 3,
                        _ => unreachable!("update section is validated above"),
                    };
                    if section_rank < section_order || !seen_sections.insert(section) {
                        return Err(RequestError::UnsupportedExpression(
                            "UpdateExpression section ordering",
                        ));
                    }
                    section_order = section_rank + 1;
                    if let (Some(start), Some(name)) = (section_start, section_name) {
                        sections.push((name, &expression[start..index]));
                    }
                    section_start = Some(index + section.len());
                    section_name = Some(section);
                }
            }
            _ => {}
        }
    }
    if let (Some(start), Some(name)) = (section_start, section_name) {
        sections.push((name, &expression[start..]));
    }
    if sections.is_empty() {
        return Err(RequestError::UnsupportedExpression(
            "UpdateExpression section",
        ));
    }

    let mut actions = Vec::new();
    for (section, body) in sections {
        for clause in split_update_clauses(body) {
            if clause.is_empty() {
                return Err(RequestError::UnsupportedExpression(
                    "UpdateExpression action",
                ));
            }
            actions.push(parse_update_action_clause(section, clause)?);
        }
    }
    Ok(actions)
}

fn parse_update_action_clause<'a>(
    section: &str,
    clause: &'a str,
) -> Result<UpdateAction<'a>, RequestError> {
    match section {
        "SET" => {
            let (attribute, value) =
                clause
                    .split_once('=')
                    .ok_or(RequestError::UnsupportedExpression(
                        "UpdateExpression SET action",
                    ))?;
            let attribute = attribute.trim();
            let value = value.trim();
            if !valid_document_path(attribute) {
                return Err(RequestError::UnsupportedExpression(
                    "UpdateExpression attribute",
                ));
            }
            if valid_query_placeholder(value) {
                return Ok(UpdateAction::SetValue {
                    attribute,
                    placeholder: value,
                });
            }
            if let Some((source, placeholder, operator)) = value
                .split_once('+')
                .map(|(source, placeholder)| (source, placeholder, 25))
                .or_else(|| {
                    value
                        .split_once('-')
                        .map(|(source, placeholder)| (source, placeholder, 26))
                })
            {
                let source = source.trim();
                let placeholder = placeholder.trim();
                if source == attribute
                    && valid_document_path(source)
                    && valid_query_placeholder(placeholder)
                {
                    return Ok(UpdateAction::SetArithmetic {
                        attribute,
                        operator,
                        placeholder,
                    });
                }
            }
            for (name, action) in [
                ("if_not_exists(", UpdateActionKind::IfNotExists),
                ("IF_NOT_EXISTS(", UpdateActionKind::IfNotExists),
                ("list_append(", UpdateActionKind::ListAppend),
                ("LIST_APPEND(", UpdateActionKind::ListAppend),
            ] {
                if let Some(arguments) = value
                    .strip_prefix(name)
                    .and_then(|arguments| arguments.strip_suffix(')'))
                {
                    let (source, placeholder) =
                        arguments
                            .split_once(',')
                            .ok_or(RequestError::UnsupportedExpression(
                                "UpdateExpression function",
                            ))?;
                    let source = source.trim();
                    let placeholder = placeholder.trim();
                    if source == attribute
                        && valid_document_path(source)
                        && valid_query_placeholder(placeholder)
                    {
                        return Ok(match action {
                            UpdateActionKind::IfNotExists => UpdateAction::SetIfNotExists {
                                attribute,
                                placeholder,
                            },
                            UpdateActionKind::ListAppend => UpdateAction::SetListAppend {
                                attribute,
                                placeholder,
                            },
                        });
                    }
                }
            }
        }
        "ADD" | "DELETE" => {
            let (attribute, placeholder) = clause.split_once(is_expression_whitespace).ok_or(
                RequestError::UnsupportedExpression("UpdateExpression action"),
            )?;
            let attribute = attribute.trim();
            let placeholder = placeholder.trim();
            if valid_document_path(attribute) && valid_query_placeholder(placeholder) {
                return Ok(if section == "ADD" {
                    UpdateAction::Add {
                        attribute,
                        placeholder,
                    }
                } else {
                    UpdateAction::Delete {
                        attribute,
                        placeholder,
                    }
                });
            }
        }
        "REMOVE" => {
            let attribute = clause.trim();
            if valid_document_path(attribute) {
                return Ok(UpdateAction::Remove { attribute });
            }
        }
        _ => {}
    }
    Err(RequestError::UnsupportedExpression(
        "UpdateExpression action",
    ))
}

#[derive(Clone, Copy)]
enum UpdateActionKind {
    IfNotExists,
    ListAppend,
}

fn parse_update_action(expression: &str) -> Result<UpdateAction<'_>, RequestError> {
    let expression = expression.trim();
    if contains_top_level_comma(expression) {
        return Err(RequestError::UnsupportedExpression(
            "UpdateExpression multi-action lists",
        ));
    }
    expression
        .strip_prefix("SET ")
        .or_else(|| expression.strip_prefix("set "))
        .and_then(|assignment| assignment.split_once('='))
        .and_then(|(attribute, value)| {
            let attribute = attribute.trim();
            let value = value.trim();
            if valid_query_attribute(attribute) && valid_query_placeholder(value) {
                return Some(UpdateAction::SetValue {
                    attribute,
                    placeholder: value,
                });
            }
            value
                .split_once('+')
                .map(|(source, placeholder)| (source, placeholder, 25))
                .or_else(|| {
                    value
                        .split_once('-')
                        .map(|(source, placeholder)| (source, placeholder, 26))
                })
                .and_then(|(source, placeholder, operator)| {
                    let source = source.trim();
                    let placeholder = placeholder.trim();
                    (source == attribute
                        && valid_query_attribute(source)
                        && valid_query_placeholder(placeholder))
                    .then_some(UpdateAction::SetArithmetic {
                        attribute,
                        operator,
                        placeholder,
                    })
                })
                .or_else(|| {
                    value
                        .strip_prefix("if_not_exists(")
                        .or_else(|| value.strip_prefix("IF_NOT_EXISTS("))
                        .and_then(|arguments| arguments.strip_suffix(')'))
                        .and_then(|arguments| arguments.split_once(','))
                        .and_then(|(source, placeholder)| {
                            let source = source.trim();
                            let placeholder = placeholder.trim();
                            (source == attribute
                                && valid_query_attribute(source)
                                && valid_query_placeholder(placeholder))
                            .then_some(UpdateAction::SetIfNotExists {
                                attribute,
                                placeholder,
                            })
                        })
                })
                .or_else(|| {
                    value
                        .strip_prefix("list_append(")
                        .or_else(|| value.strip_prefix("LIST_APPEND("))
                        .and_then(|arguments| arguments.strip_suffix(')'))
                        .and_then(|arguments| arguments.split_once(','))
                        .and_then(|(source, placeholder)| {
                            let source = source.trim();
                            let placeholder = placeholder.trim();
                            (source == attribute
                                && valid_query_attribute(source)
                                && valid_query_placeholder(placeholder))
                            .then_some(UpdateAction::SetListAppend {
                                attribute,
                                placeholder,
                            })
                        })
                })
        })
        .or_else(|| {
            expression
                .strip_prefix("ADD ")
                .or_else(|| expression.strip_prefix("add "))
                .and_then(|arguments| arguments.split_once(' '))
                .map(|(attribute, placeholder)| (attribute.trim(), placeholder.trim(), 20))
                .or_else(|| {
                    expression
                        .strip_prefix("DELETE ")
                        .or_else(|| expression.strip_prefix("delete "))
                        .and_then(|arguments| arguments.split_once(' '))
                        .map(|(attribute, placeholder)| (attribute.trim(), placeholder.trim(), 21))
                })
                .and_then(|(attribute, placeholder, opcode)| {
                    (valid_query_attribute(attribute) && valid_query_placeholder(placeholder))
                        .then_some(if opcode == 20 {
                            UpdateAction::Add {
                                attribute,
                                placeholder,
                            }
                        } else {
                            UpdateAction::Delete {
                                attribute,
                                placeholder,
                            }
                        })
                })
        })
        .or_else(|| {
            expression
                .strip_prefix("REMOVE ")
                .or_else(|| expression.strip_prefix("remove "))
                .map(str::trim)
                .filter(|attribute| valid_query_attribute(attribute))
                .map(|attribute| UpdateAction::Remove { attribute })
        })
        .ok_or(RequestError::UnsupportedExpression(
            "UpdateExpression (only bounded SET, ADD, DELETE, or REMOVE actions are supported)",
        ))
}

fn encode_update_action_body(
    action: UpdateAction<'_>,
    value: Option<&aws_sdk_dynamodb::types::AttributeValue>,
    variable_id: usize,
) -> Result<Vec<u8>, RequestError> {
    let mut update = Vec::new();
    write_type(
        &mut update,
        MAJOR_ARRAY,
        if value.is_some() { 3 } else { 2 },
    );
    write_i64(
        &mut update,
        match action {
            UpdateAction::SetValue { .. }
            | UpdateAction::SetArithmetic { .. }
            | UpdateAction::SetIfNotExists { .. }
            | UpdateAction::SetListAppend { .. } => 19,
            UpdateAction::Add { .. } => 20,
            UpdateAction::Delete { .. } => 21,
            UpdateAction::Remove { .. } => 22,
        },
    );
    let attribute = match action {
        UpdateAction::SetValue { attribute, .. }
        | UpdateAction::SetArithmetic { attribute, .. }
        | UpdateAction::SetIfNotExists { attribute, .. }
        | UpdateAction::SetListAppend { attribute, .. }
        | UpdateAction::Add { attribute, .. }
        | UpdateAction::Delete { attribute, .. }
        | UpdateAction::Remove { attribute } => attribute,
    };
    let path = parse_document_path(attribute)?;
    write_type(&mut update, MAJOR_ARRAY, (path.len() + 1) as u64);
    write_i64(&mut update, 18);
    for segment in &path {
        match segment {
            DocumentPathSegment::Attribute(attribute) => write_text(&mut update, attribute),
            DocumentPathSegment::ListIndex(index) => {
                write_type(&mut update, MAJOR_TAG, DOCUMENT_PATH_LIST_INDEX_TAG);
                write_i64(&mut update, *index);
            }
        }
    }
    if value.is_some() {
        match action {
            UpdateAction::SetValue { .. } => {
                write_type(&mut update, MAJOR_ARRAY, 2);
                write_i64(&mut update, 17);
                write_i64(&mut update, variable_id as i64);
            }
            UpdateAction::SetArithmetic { operator, .. } => {
                write_type(&mut update, MAJOR_ARRAY, 3);
                write_i64(&mut update, operator);
                write_type(&mut update, MAJOR_ARRAY, 2);
                write_i64(&mut update, 18);
                for segment in &path {
                    match segment {
                        DocumentPathSegment::Attribute(attribute) => {
                            write_text(&mut update, attribute)
                        }
                        DocumentPathSegment::ListIndex(index) => {
                            write_type(&mut update, MAJOR_TAG, DOCUMENT_PATH_LIST_INDEX_TAG);
                            write_i64(&mut update, *index);
                        }
                    }
                }
                write_type(&mut update, MAJOR_ARRAY, 2);
                write_i64(&mut update, 17);
                write_i64(&mut update, variable_id as i64);
            }
            UpdateAction::SetIfNotExists { .. } => {
                write_type(&mut update, MAJOR_ARRAY, 3);
                write_i64(&mut update, 23);
                write_type(&mut update, MAJOR_ARRAY, 2);
                write_i64(&mut update, 18);
                for segment in &path {
                    match segment {
                        DocumentPathSegment::Attribute(attribute) => {
                            write_text(&mut update, attribute)
                        }
                        DocumentPathSegment::ListIndex(index) => {
                            write_type(&mut update, MAJOR_TAG, DOCUMENT_PATH_LIST_INDEX_TAG);
                            write_i64(&mut update, *index);
                        }
                    }
                }
                write_type(&mut update, MAJOR_ARRAY, 2);
                write_i64(&mut update, 17);
                write_i64(&mut update, variable_id as i64);
            }
            UpdateAction::SetListAppend { .. } => {
                write_type(&mut update, MAJOR_ARRAY, 3);
                write_i64(&mut update, 24);
                write_type(&mut update, MAJOR_ARRAY, 2);
                write_i64(&mut update, 18);
                for segment in &path {
                    match segment {
                        DocumentPathSegment::Attribute(attribute) => {
                            write_text(&mut update, attribute)
                        }
                        DocumentPathSegment::ListIndex(index) => {
                            write_type(&mut update, MAJOR_TAG, DOCUMENT_PATH_LIST_INDEX_TAG);
                            write_i64(&mut update, *index);
                        }
                    }
                }
                write_type(&mut update, MAJOR_ARRAY, 2);
                write_i64(&mut update, 17);
                write_i64(&mut update, variable_id as i64);
            }
            UpdateAction::Add { .. } | UpdateAction::Delete { .. } => {
                write_type(&mut update, MAJOR_ARRAY, 2);
                write_i64(&mut update, 17);
                write_i64(&mut update, variable_id as i64);
            }
            UpdateAction::Remove { .. } => unreachable!("REMOVE has no value"),
        }
    }
    Ok(update)
}

fn encode_update_action(
    action: UpdateAction<'_>,
    value: Option<&aws_sdk_dynamodb::types::AttributeValue>,
) -> Result<Vec<u8>, RequestError> {
    let mut update = Vec::new();
    write_type(&mut update, MAJOR_ARRAY, 3);
    write_i64(&mut update, 1);
    write_type(&mut update, MAJOR_ARRAY, 1);
    update.extend(encode_update_action_body(action, value, 0)?);
    write_type(&mut update, MAJOR_ARRAY, u64::from(value.is_some()));
    if let Some(value) = value {
        update.extend(encode_attribute_value(value)?);
    }
    Ok(update)
}

fn encode_update_actions(
    actions: &[UpdateAction<'_>],
    values: &[&aws_sdk_dynamodb::types::AttributeValue],
) -> Result<Vec<u8>, RequestError> {
    let mut update = Vec::new();
    write_type(&mut update, MAJOR_ARRAY, 3);
    write_i64(&mut update, 1);
    write_type(&mut update, MAJOR_ARRAY, actions.len() as u64);
    let mut value_index = 0;
    let mut placeholders = Vec::new();
    for action in actions {
        let action_value_index = action.placeholder().map_or(value_index, |placeholder| {
            placeholders
                .iter()
                .position(|seen| *seen == placeholder)
                .unwrap_or(value_index)
        });
        let value = action
            .placeholder()
            .and_then(|_| values.get(action_value_index).copied());
        if action.placeholder().is_some() && value.is_none() {
            return Err(RequestError::UnsupportedExpression(
                "UpdateExpression placeholder",
            ));
        }
        update.extend(encode_update_action_body(
            *action,
            value,
            action_value_index,
        )?);
        if let Some(placeholder) = action.placeholder() {
            if action_value_index == value_index {
                placeholders.push(placeholder);
                value_index += 1;
            }
        } else if value.is_some() {
            value_index += 1;
        }
    }
    write_type(&mut update, MAJOR_ARRAY, values.len() as u64);
    for value in values {
        update.extend(encode_attribute_value(value)?);
    }
    Ok(update)
}

fn resolve_query_attribute_names<'a>(
    expression: &'a str,
    names: Option<&std::collections::HashMap<String, String>>,
) -> Result<std::borrow::Cow<'a, str>, RequestError> {
    let Some(names) = names else {
        if expression.contains('#') {
            return Err(RequestError::UnsupportedExpression(
                "KeyConditionExpression attribute-name alias",
            ));
        }
        return Ok(std::borrow::Cow::Borrowed(expression));
    };
    let mut resolved = String::with_capacity(expression.len());
    let mut characters = expression.chars().peekable();
    while let Some(character) = characters.next() {
        if character != '#' {
            resolved.push(character);
            continue;
        }
        let mut alias = String::from("#");
        while let Some(character) =
            characters.next_if(|character| character.is_ascii_alphanumeric() || *character == '_')
        {
            alias.push(character);
        }
        if alias.len() == 1 {
            return Err(RequestError::UnsupportedExpression(
                "KeyConditionExpression attribute-name alias",
            ));
        }
        let name = names
            .get(&alias)
            .ok_or(RequestError::UnsupportedExpression(
                "KeyConditionExpression attribute-name alias",
            ))?;
        resolved.push_str(name);
    }
    Ok(std::borrow::Cow::Owned(resolved))
}

fn resolve_projection_attribute_names<'a>(
    expression: &'a str,
    names: Option<&std::collections::HashMap<String, String>>,
) -> Result<std::borrow::Cow<'a, str>, RequestError> {
    resolve_query_attribute_names(expression, names).map_err(|error| match error {
        RequestError::UnsupportedExpression("KeyConditionExpression attribute-name alias") => {
            RequestError::UnsupportedExpression("ProjectionExpression attribute-name alias")
        }
        error => error,
    })
}

fn validate_query_attribute_names(input: &QueryInput) -> Result<(), RequestError> {
    let Some(names) = input.expression_attribute_names() else {
        return Ok(());
    };
    let mut used = std::collections::HashSet::new();
    for expression in [
        input.projection_expression(),
        input.key_condition_expression(),
        input.filter_expression(),
    ]
    .into_iter()
    .flatten()
    {
        let mut characters = expression.chars().peekable();
        while let Some(character) = characters.next() {
            if character != '#' {
                continue;
            }
            let mut alias = String::from("#");
            while let Some(character) = characters
                .next_if(|character| character.is_ascii_alphanumeric() || *character == '_')
            {
                alias.push(character);
            }
            if alias.len() > 1 {
                used.insert(alias);
            }
        }
    }
    if used.len() != names.len() {
        return Err(RequestError::UnsupportedExpression(
            "Query unused attribute-name alias",
        ));
    }
    Ok(())
}

impl From<CborError> for RequestError {
    fn from(value: CborError) -> Self {
        Self::Cbor(value)
    }
}

impl From<SchemaError> for RequestError {
    fn from(value: SchemaError) -> Self {
        Self::Schema(value)
    }
}

/// Encodes the DAX control request that retrieves a table key schema.
pub(crate) fn encode_define_key_schema(table_name: &str) -> Vec<u8> {
    let mut output = service_and_method(DEFINE_KEY_SCHEMA_METHOD_ID);
    write_bytes(&mut output, table_name.as_bytes());
    output
}

/// Encodes the DAX control request that resolves a sorted attribute list to an ID.
pub(crate) fn encode_define_attribute_list_id(names: &[String]) -> Vec<u8> {
    let mut output = service_and_method(DEFINE_ATTRIBUTE_LIST_ID_METHOD_ID);
    write_type(&mut output, MAJOR_ARRAY, names.len() as u64);
    for name in names {
        write_text(&mut output, name);
    }
    output
}

/// Encodes the DAX control request that resolves an attribute-list ID to names.
pub(crate) fn encode_define_attribute_list(id: i64) -> Vec<u8> {
    let mut output = service_and_method(DEFINE_ATTRIBUTE_LIST_METHOD_ID);
    write_i64(&mut output, id);
    output
}

/// Encodes the supported GetItem subset with bounded projections.
pub(crate) fn encode_get_item(
    input: &GetItemInput,
    key_schema: &[AttributeDefinition],
) -> Result<Vec<u8>, RequestError> {
    reject_get_options(input)?;
    let projection = encode_projection_expression(
        input.projection_expression(),
        input.expression_attribute_names(),
    )?;
    let table_name = input
        .table_name()
        .ok_or(RequestError::MissingRequiredField("TableName"))?;
    let key = input
        .key()
        .ok_or(RequestError::MissingRequiredField("Key"))?;

    let mut output = service_and_method(GET_ITEM_METHOD_ID);
    write_bytes(&mut output, table_name.as_bytes());
    output.extend(encode_item_key(key, key_schema)?);
    output.push(MAJOR_MAP | 31);
    if let Some(projection) = projection {
        write_i64(&mut output, 0);
        write_bytes(&mut output, &projection);
    }
    if let Some(consistent_read) = input.consistent_read() {
        write_i64(&mut output, 2);
        output.push(if consistent_read { 0xf5 } else { 0xf4 });
    }
    if let Some(return_consumed_capacity) = input
        .return_consumed_capacity()
        .and_then(return_consumed_capacity_code)
    {
        write_i64(&mut output, 3);
        write_i64(&mut output, return_consumed_capacity);
    }
    output.push(0xff);
    Ok(output)
}

/// Encodes GetItem using preloaded, client-owned table schema state.
pub(crate) fn encode_get_item_with_registry(
    input: &GetItemInput,
    schemas: &SchemaRegistry,
) -> Result<Vec<u8>, RequestError> {
    let table_name = input
        .table_name()
        .ok_or(RequestError::MissingRequiredField("TableName"))?;
    encode_get_item(input, &schemas.key_schema(table_name)?)
}

/// Encodes the bounded PutItem request subset with condition expressions.
pub(crate) fn encode_put_item(
    input: &PutItemInput,
    key_schema: &[AttributeDefinition],
    attribute_list_id: i64,
) -> Result<Vec<u8>, RequestError> {
    reject_put_options(input)?;
    let condition = encode_filter_expression(
        input.condition_expression(),
        input.expression_attribute_names(),
        input.expression_attribute_values(),
    )?;
    let table_name = input
        .table_name()
        .ok_or(RequestError::MissingRequiredField("TableName"))?;
    let item = input
        .item()
        .ok_or(RequestError::MissingRequiredField("Item"))?;

    let mut output = service_and_method(PUT_ITEM_METHOD_ID);
    write_bytes(&mut output, table_name.as_bytes());
    output.extend(encode_item_key(item, key_schema)?);
    write_bytes(
        &mut output,
        &encode_item_non_key_attributes(item, key_schema, attribute_list_id)?,
    );
    output.push(MAJOR_MAP | 31);
    if let Some(return_consumed_capacity) = input
        .return_consumed_capacity()
        .and_then(return_consumed_capacity_code)
    {
        write_i64(&mut output, 3);
        write_i64(&mut output, return_consumed_capacity);
    }
    if let Some(return_item_collection_metrics) = input
        .return_item_collection_metrics()
        .and_then(return_item_collection_metrics_code)
    {
        write_i64(&mut output, 6);
        write_i64(&mut output, return_item_collection_metrics);
    }
    if let Some(return_values) = input.return_values().and_then(return_values_code) {
        write_i64(&mut output, 7);
        write_i64(&mut output, return_values);
    }
    if let Some(condition) = condition {
        write_i64(&mut output, 4);
        write_bytes(&mut output, &condition);
    }
    output.push(0xff);
    Ok(output)
}

/// Encodes the bounded DeleteItem request subset with condition expressions.
pub(crate) fn encode_delete_item(
    input: &DeleteItemInput,
    key_schema: &[AttributeDefinition],
) -> Result<Vec<u8>, RequestError> {
    reject_delete_options(input)?;
    let condition = encode_filter_expression(
        input.condition_expression(),
        input.expression_attribute_names(),
        input.expression_attribute_values(),
    )?;
    let table_name = input
        .table_name()
        .ok_or(RequestError::MissingRequiredField("TableName"))?;
    let key = input
        .key()
        .ok_or(RequestError::MissingRequiredField("Key"))?;

    let mut output = service_and_method(DELETE_ITEM_METHOD_ID);
    write_bytes(&mut output, table_name.as_bytes());
    output.extend(encode_item_key(key, key_schema)?);
    output.push(MAJOR_MAP | 31);
    if let Some(return_consumed_capacity) = input
        .return_consumed_capacity()
        .and_then(return_consumed_capacity_code)
    {
        write_i64(&mut output, 3);
        write_i64(&mut output, return_consumed_capacity);
    }
    if let Some(return_item_collection_metrics) = input
        .return_item_collection_metrics()
        .and_then(return_item_collection_metrics_code)
    {
        write_i64(&mut output, 6);
        write_i64(&mut output, return_item_collection_metrics);
    }
    if let Some(return_values) = input.return_values().and_then(return_values_code) {
        write_i64(&mut output, 7);
        write_i64(&mut output, return_values);
    }
    if let Some(condition) = condition {
        write_i64(&mut output, 4);
        write_bytes(&mut output, &condition);
    }
    output.push(0xff);
    Ok(output)
}

pub(crate) fn encode_update_item(
    input: &UpdateItemInput,
    key_schema: &[AttributeDefinition],
) -> Result<Vec<u8>, RequestError> {
    if input.return_values_on_condition_check_failure().is_some()
        || input.expected().is_some()
        || input.conditional_operator().is_some()
    {
        return Err(RequestError::UnsupportedExpression(
            "UpdateItem legacy condition fields",
        ));
    }
    let expression = input
        .update_expression()
        .ok_or(RequestError::MissingRequiredField("UpdateExpression"))?;
    if contains_non_ascii_expression_whitespace(expression) {
        return Err(RequestError::UnsupportedExpression("UpdateExpression"));
    }
    let expression = resolve_query_attribute_names(expression, input.expression_attribute_names())?;
    if let Some(names) = input.expression_attribute_names() {
        let mut used = query_attribute_name_aliases(input.update_expression().unwrap());
        if let Some(condition) = input.condition_expression() {
            used.extend(query_attribute_name_aliases(condition));
        }
        if used.len() != names.len() {
            return Err(RequestError::UnsupportedExpression(
                "UpdateItem unused attribute-name alias",
            ));
        }
    }
    let actions = parse_update_actions(expression.trim())?;
    let condition = encode_filter_expression(
        input.condition_expression(),
        input.expression_attribute_names(),
        input.expression_attribute_values(),
    )?;
    if let Some(values) = input.expression_attribute_values() {
        let mut used = std::collections::HashSet::new();
        if let Some(update) = input.update_expression() {
            used.extend(query_attribute_value_placeholders(update));
        }
        if let Some(condition_expression) = input.condition_expression() {
            let condition = resolve_query_attribute_names(
                condition_expression,
                input.expression_attribute_names(),
            )?;
            used.extend(
                parse_query_filter(&condition)?
                    .conditions
                    .into_iter()
                    .flat_map(|condition| {
                        condition
                            .placeholders()
                            .into_iter()
                            .map(str::to_owned)
                            .collect::<Vec<_>>()
                    }),
            );
        }
        if used.len() != values.len() {
            return Err(RequestError::UnsupportedExpression(
                "UpdateItem unused expression attribute value",
            ));
        }
    }
    let mut seen_update_values = std::collections::HashSet::new();
    let update_values = actions
        .iter()
        .filter_map(UpdateAction::placeholder)
        .filter(|placeholder| seen_update_values.insert(*placeholder))
        .map(|placeholder| {
            input
                .expression_attribute_values()
                .ok_or(RequestError::MissingRequiredField(
                    "ExpressionAttributeValues",
                ))?
                .get(placeholder)
                .ok_or(RequestError::UnsupportedExpression(
                    "UpdateExpression placeholder",
                ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let table_name = input
        .table_name()
        .ok_or(RequestError::MissingRequiredField("TableName"))?;
    let key = input
        .key()
        .ok_or(RequestError::MissingRequiredField("Key"))?;
    let update = encode_update_actions(&actions, &update_values)?;

    let mut output = service_and_method(UPDATE_ITEM_METHOD_ID);
    write_bytes(&mut output, table_name.as_bytes());
    output.extend(encode_item_key(key, key_schema)?);
    output.push(MAJOR_MAP | 31);
    if let Some(condition) = condition {
        write_i64(&mut output, 4);
        write_bytes(&mut output, &condition);
    }
    if let Some(return_values) = input.return_values().and_then(return_values_code) {
        write_i64(&mut output, 7);
        write_i64(&mut output, return_values);
    }
    write_i64(&mut output, 8);
    write_bytes(&mut output, &update);
    output.push(0xff);
    Ok(output)
}

pub(crate) fn encode_batch_write_item(
    input: &BatchWriteItemInput,
    schemas: &std::collections::HashMap<String, Vec<AttributeDefinition>>,
    attribute_list_ids: &std::collections::HashMap<String, i64>,
) -> Result<Vec<u8>, RequestError> {
    let requests = input
        .request_items()
        .ok_or(RequestError::MissingRequiredField("RequestItems"))?;
    if requests.is_empty() {
        return Err(RequestError::UnsupportedExpression(
            "BatchWriteItem empty RequestItems",
        ));
    }

    let total = requests.values().map(Vec::len).sum::<usize>();
    if total == 0
        || total > 25
        || requests
            .values()
            .any(|items| items.is_empty() || items.len() > 25)
    {
        return Err(RequestError::UnsupportedExpression(
            "BatchWriteItem request count",
        ));
    }

    let mut tables = requests.keys().collect::<Vec<_>>();
    tables.sort_unstable();
    let mut output = service_and_method(BATCH_WRITE_ITEM_METHOD_ID);
    write_type(&mut output, MAJOR_MAP, tables.len() as u64);
    for table in tables {
        let schema = schemas
            .get(table)
            .ok_or_else(|| RequestError::Schema(SchemaError::MissingKeySchema((*table).clone())))?;
        let attribute_list_id = *attribute_list_ids.get(table).ok_or_else(|| {
            RequestError::Schema(SchemaError::MissingAttributeListId(
                non_key_attribute_names_from_requests(&requests[table], schema),
            ))
        })?;
        let items = &requests[table];
        let mut seen_keys = std::collections::HashSet::new();
        for request in items {
            let key = match (request.put_request(), request.delete_request()) {
                (Some(put), None) => &put.item,
                (None, Some(delete)) => &delete.key,
                _ => {
                    return Err(RequestError::UnsupportedExpression(
                        "BatchWriteItem write request shape",
                    ));
                }
            };
            let encoded_key = encode_item_key(key, schema)?;
            if !seen_keys.insert(encoded_key) {
                return Err(RequestError::UnsupportedExpression(
                    "BatchWriteItem duplicate item key",
                ));
            }
        }
        write_text(&mut output, table);
        write_type(&mut output, MAJOR_ARRAY, (items.len() * 2) as u64);
        for request in items {
            match (request.put_request(), request.delete_request()) {
                (Some(put), None) => {
                    output.extend(encode_item_key(&put.item, schema)?);
                    let attributes =
                        encode_item_non_key_attributes(&put.item, schema, attribute_list_id)?;
                    write_bytes(&mut output, &attributes);
                }
                (None, Some(delete)) => {
                    output.extend(encode_item_key(&delete.key, schema)?);
                    output.push(0xf6);
                }
                _ => {
                    return Err(RequestError::UnsupportedExpression(
                        "BatchWriteItem write request shape",
                    ));
                }
            }
        }
    }
    write_optional_map(
        &mut output,
        None,
        input.return_consumed_capacity(),
        input.return_item_collection_metrics(),
        None,
    );
    Ok(output)
}

fn non_key_attribute_names_from_requests(
    requests: &[aws_sdk_dynamodb::types::WriteRequest],
    schema: &[AttributeDefinition],
) -> Vec<String> {
    let mut names = std::collections::HashSet::new();
    for request in requests {
        if let Some(put) = request.put_request() {
            names.extend(non_key_attribute_names(&put.item, schema));
        }
    }
    let mut names = names.into_iter().collect::<Vec<_>>();
    names.sort_unstable();
    names
}

pub(crate) fn encode_batch_get_item(
    input: &BatchGetItemInput,
    schemas: &std::collections::HashMap<String, Vec<AttributeDefinition>>,
) -> Result<Vec<u8>, RequestError> {
    let requests = input
        .request_items()
        .ok_or(RequestError::MissingRequiredField("RequestItems"))?;
    if requests.is_empty() || requests.values().any(|item| item.keys().is_empty()) {
        return Err(RequestError::UnsupportedExpression(
            "BatchGetItem request items",
        ));
    }
    let total = requests
        .values()
        .map(|item| item.keys().len())
        .sum::<usize>();
    if total > 100 {
        return Err(RequestError::UnsupportedExpression(
            "BatchGetItem request count",
        ));
    }
    let mut tables = requests.keys().collect::<Vec<_>>();
    tables.sort_unstable();
    let mut output = service_and_method(BATCH_GET_ITEM_METHOD_ID);
    write_type(&mut output, MAJOR_MAP, tables.len() as u64);
    for table in tables {
        let item = &requests[table];
        let schema = schemas
            .get(table)
            .ok_or_else(|| RequestError::Schema(SchemaError::MissingKeySchema(table.clone())))?;
        let mut seen = std::collections::HashSet::new();
        for key in item.keys() {
            if !seen.insert(encode_item_key(key, schema)?) {
                return Err(RequestError::UnsupportedExpression(
                    "BatchGetItem duplicate item key",
                ));
            }
        }
        write_text(&mut output, table);
        write_type(&mut output, MAJOR_ARRAY, 3);
        output.push(if item.consistent_read().unwrap_or(false) {
            0xf5
        } else {
            0xf4
        });
        if let Some(projection) = encode_projection_expression(
            item.projection_expression(),
            item.expression_attribute_names(),
        )? {
            write_bytes(&mut output, &projection);
        } else {
            output.push(0xf6);
        }
        write_type(&mut output, MAJOR_ARRAY, item.keys().len() as u64);
        for key in item.keys() {
            output.extend(encode_item_key(key, schema)?);
        }
    }
    write_optional_map(
        &mut output,
        None,
        input.return_consumed_capacity(),
        None,
        None,
    );
    Ok(output)
}

pub(crate) fn encode_transact_get_items(
    input: &TransactGetItemsInput,
    schemas: &std::collections::HashMap<String, Vec<AttributeDefinition>>,
) -> Result<Vec<u8>, RequestError> {
    let items = input.transact_items();
    if items.is_empty() {
        return Err(RequestError::MissingRequiredField("TransactItems"));
    }
    if items.is_empty() || items.len() > 100 {
        return Err(RequestError::UnsupportedExpression(
            "TransactGetItems request count",
        ));
    }

    let mut table_names = Vec::new();
    let mut keys = Vec::new();
    let mut projections = Vec::new();
    write_type(&mut table_names, MAJOR_ARRAY, items.len() as u64);
    write_type(&mut keys, MAJOR_ARRAY, items.len() as u64);
    write_type(&mut projections, MAJOR_ARRAY, items.len() as u64);
    let mut seen = std::collections::HashSet::new();
    for item in items {
        let get = item.get().ok_or(RequestError::UnsupportedExpression(
            "TransactGetItems item must contain Get",
        ))?;
        let table = get.table_name();
        let key = get.key();
        let schema = schemas
            .get(table)
            .ok_or_else(|| RequestError::Schema(SchemaError::MissingKeySchema(table.to_owned())))?;
        let encoded_key = encode_item_key(key, schema)?;
        if !seen.insert((table.to_owned(), encoded_key.clone())) {
            return Err(RequestError::UnsupportedExpression(
                "TransactGetItems duplicate item key",
            ));
        }
        write_bytes(&mut table_names, table.as_bytes());
        keys.extend(encoded_key);
        let projection = encode_projection_expression(
            get.projection_expression(),
            get.expression_attribute_names(),
        )?;
        match projection {
            Some(projection) => write_bytes(&mut projections, &projection),
            None => projections.push(0xf6),
        }
    }

    let mut output = service_and_method(TRANSACT_GET_ITEMS_METHOD_ID);
    output.extend(table_names);
    output.extend(keys);
    output.extend(projections);
    write_optional_map(
        &mut output,
        None,
        input.return_consumed_capacity(),
        None,
        None,
    );
    Ok(output)
}

pub(crate) fn encode_transact_write_items(
    input: &TransactWriteItemsInput,
    schemas: &std::collections::HashMap<String, Vec<AttributeDefinition>>,
    attribute_list_ids: &std::collections::HashMap<String, i64>,
) -> Result<Vec<u8>, RequestError> {
    let items = input.transact_items();
    if items.is_empty() {
        return Err(RequestError::MissingRequiredField("TransactItems"));
    }
    if items.len() > 100 {
        return Err(RequestError::UnsupportedExpression(
            "TransactWriteItems request count",
        ));
    }
    let mut operations = Vec::new();
    let mut tables = Vec::new();
    let mut keys = Vec::new();
    let mut values = Vec::new();
    let mut return_values = Vec::new();
    let mut conditions = Vec::new();
    let mut updates = Vec::new();
    for section in [
        &mut operations,
        &mut tables,
        &mut keys,
        &mut values,
        &mut conditions,
        &mut updates,
    ] {
        write_type(section, MAJOR_ARRAY, items.len() as u64);
    }
    let mut seen = std::collections::HashSet::new();
    for item in items {
        let present = [
            item.condition_check().is_some(),
            item.put().is_some(),
            item.delete().is_some(),
            item.update().is_some(),
        ];
        if present.iter().filter(|value| **value).count() != 1 {
            return Err(RequestError::UnsupportedExpression(
                "TransactWriteItems item shape",
            ));
        }
        let (
            operation,
            table,
            key,
            put_item,
            condition,
            update_expression,
            names,
            values_map,
            return_mode,
        ) = if let Some(check) = item.condition_check() {
            (
                12,
                check.table_name(),
                check.key(),
                None,
                Some(check.condition_expression()),
                None,
                check.expression_attribute_names(),
                check.expression_attribute_values(),
                check.return_values_on_condition_check_failure(),
            )
        } else if let Some(put) = item.put() {
            (
                2,
                put.table_name(),
                put.item(),
                Some(put.item()),
                put.condition_expression(),
                None,
                put.expression_attribute_names(),
                put.expression_attribute_values(),
                put.return_values_on_condition_check_failure(),
            )
        } else if let Some(delete) = item.delete() {
            (
                7,
                delete.table_name(),
                delete.key(),
                None,
                delete.condition_expression(),
                None,
                delete.expression_attribute_names(),
                delete.expression_attribute_values(),
                delete.return_values_on_condition_check_failure(),
            )
        } else {
            let update = item.update().expect("validated update variant");
            (
                9,
                update.table_name(),
                update.key(),
                None,
                update.condition_expression(),
                Some(update.update_expression()),
                update.expression_attribute_names(),
                update.expression_attribute_values(),
                update.return_values_on_condition_check_failure(),
            )
        };
        let schema = schemas
            .get(table)
            .ok_or_else(|| RequestError::Schema(SchemaError::MissingKeySchema(table.to_owned())))?;
        let key_bytes = encode_item_key(key, schema)?;
        if !seen.insert((table.to_owned(), key_bytes.clone())) {
            return Err(RequestError::UnsupportedExpression(
                "TransactWriteItems duplicate item key",
            ));
        }
        write_i64(&mut operations, operation);
        write_bytes(&mut tables, table.as_bytes());
        write_bytes(&mut keys, &key_bytes);
        if let Some(item) = put_item {
            let id = *attribute_list_ids.get(table).ok_or_else(|| {
                RequestError::Schema(SchemaError::MissingAttributeListId(
                    non_key_attribute_names(item, schema),
                ))
            })?;
            values.extend(encode_item_non_key_attributes(item, schema, id)?);
        } else {
            values.push(0xf6);
        }
        if let Some(condition) = condition {
            let encoded = encode_filter_expression(Some(condition), names, values_map)?
                .ok_or(RequestError::UnsupportedExpression("transaction condition"))?;
            write_bytes(&mut conditions, &encoded);
        } else {
            conditions.push(0xf6);
        }
        if let Some(expression) = update_expression {
            let expression = resolve_query_attribute_names(expression, names)?;
            let actions = parse_update_actions(expression.trim())?;
            let update_values = actions
                .iter()
                .filter_map(UpdateAction::placeholder)
                .map(|placeholder| {
                    values_map.and_then(|values| values.get(placeholder)).ok_or(
                        RequestError::UnsupportedExpression(
                            "TransactWriteItems update placeholder",
                        ),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            let encoded = encode_update_actions(&actions, &update_values)?;
            write_bytes(&mut updates, &encoded);
        } else {
            updates.push(0xf6);
        }
        write_i64(
            &mut return_values,
            if return_mode
                .map(|mode| mode.as_str() == "ALL_OLD")
                .unwrap_or(false)
            {
                2
            } else {
                1
            },
        );
    }
    let mut output = service_and_method(TRANSACT_WRITE_ITEMS_METHOD_ID);
    for section in [&operations, &tables, &keys, &values] {
        output.extend(section);
    }
    output.push(0xf6);
    let mut return_array = Vec::new();
    write_type(&mut return_array, MAJOR_ARRAY, items.len() as u64);
    return_array.extend(return_values);
    output.extend(return_array);
    let mut condition_array = Vec::new();
    write_type(&mut condition_array, MAJOR_ARRAY, items.len() as u64);
    condition_array.extend(conditions);
    output.extend(condition_array);
    let mut update_array = Vec::new();
    write_type(&mut update_array, MAJOR_ARRAY, items.len() as u64);
    update_array.extend(updates);
    output.extend(update_array);
    write_optional_map_with_token(
        &mut output,
        input.return_consumed_capacity(),
        input.return_item_collection_metrics(),
        input.client_request_token(),
    );
    Ok(output)
}

/// Encodes the bounded Scan subset with table-key pagination and filter expressions.
pub(crate) fn encode_scan(
    input: &ScanInput,
    key_schema: &[AttributeDefinition],
) -> Result<Vec<u8>, RequestError> {
    reject_scan_options(input)?;
    validate_scan_expressions(input)?;
    let projection = encode_projection_expression(
        input.projection_expression(),
        input.expression_attribute_names(),
    )?;
    let filter = encode_filter_expression(
        input.filter_expression(),
        input.expression_attribute_names(),
        input.expression_attribute_values(),
    )?;
    let table_name = input
        .table_name()
        .ok_or(RequestError::MissingRequiredField("TableName"))?;

    let mut output = service_and_method(SCAN_METHOD_ID);
    write_bytes(&mut output, table_name.as_bytes());
    output.push(MAJOR_MAP | 31);
    if let Some(index_name) = input.index_name() {
        write_i64(&mut output, 11);
        write_bytes(&mut output, index_name.as_bytes());
    }
    if let Some(projection) = projection {
        write_i64(&mut output, 0);
        write_bytes(&mut output, &projection);
    }
    write_select(&mut output, input.select())?;
    if let Some(return_consumed_capacity) = input
        .return_consumed_capacity()
        .and_then(return_consumed_capacity_code)
    {
        write_i64(&mut output, 3);
        write_i64(&mut output, return_consumed_capacity);
    }
    if let Some(consistent_read) = input.consistent_read() {
        write_i64(&mut output, 2);
        write_i64(&mut output, i64::from(consistent_read));
    }
    if let Some(key) = input.exclusive_start_key() {
        write_i64(&mut output, 9);
        output.extend(encode_item_key(key, key_schema)?);
    }
    if let Some(limit) = input.limit() {
        write_i64(&mut output, 13);
        write_i64(&mut output, i64::from(limit));
    }
    if let Some(filter) = filter {
        write_i64(&mut output, 10);
        write_bytes(&mut output, &filter);
    }
    output.push(0xff);
    Ok(output)
}

/// Encodes the bounded Query subset with a partition-key equality condition
/// and an optional sort-key comparison.
pub(crate) fn encode_query(
    input: &QueryInput,
    key_schema: &[AttributeDefinition],
) -> Result<Vec<u8>, RequestError> {
    validate_query_input(input)?;
    let table_name = input
        .table_name()
        .ok_or(RequestError::MissingRequiredField("TableName"))?;
    let key_condition = encode_query_key_condition(input)?;

    let mut output = service_and_method(QUERY_METHOD_ID);
    write_bytes(&mut output, table_name.as_bytes());
    write_bytes(&mut output, &key_condition);
    let projection = encode_projection_expression(
        input.projection_expression(),
        input.expression_attribute_names(),
    )?;
    let filter = encode_filter_expression(
        input.filter_expression(),
        input.expression_attribute_names(),
        input.expression_attribute_values(),
    )?;
    output.push(MAJOR_MAP | 31);
    if let Some(index_name) = input.index_name() {
        write_i64(&mut output, 11);
        write_bytes(&mut output, index_name.as_bytes());
    }
    if let Some(projection) = projection {
        write_i64(&mut output, 0);
        write_bytes(&mut output, &projection);
    }
    write_select(&mut output, input.select())?;
    if let Some(return_consumed_capacity) = input
        .return_consumed_capacity()
        .and_then(return_consumed_capacity_code)
    {
        write_i64(&mut output, 3);
        write_i64(&mut output, return_consumed_capacity);
    }

    if let Some(consistent_read) = input.consistent_read() {
        write_i64(&mut output, 2);
        write_i64(&mut output, i64::from(consistent_read));
    }
    if let Some(key) = input.exclusive_start_key() {
        write_i64(&mut output, 9);
        output.extend(encode_item_key(key, key_schema)?);
    }
    if let Some(limit) = input.limit() {
        write_i64(&mut output, 13);
        write_i64(&mut output, i64::from(limit));
    }
    if let Some(scan_index_forward) = input.scan_index_forward() {
        write_i64(&mut output, 14);
        write_i64(&mut output, i64::from(scan_index_forward));
    }
    if let Some(filter) = filter {
        write_i64(&mut output, 10);
        write_bytes(&mut output, &filter);
    }
    output.push(0xff);
    Ok(output)
}

/// Validates the Query fields supported before schema-dependent request encoding.
pub(crate) fn validate_query_input(input: &QueryInput) -> Result<(), RequestError> {
    reject_query_options(input)?;
    validate_query_attribute_names(input)?;
    encode_query_key_condition(input)?;
    encode_projection_expression(
        input.projection_expression(),
        input.expression_attribute_names(),
    )?;
    encode_filter_expression(
        input.filter_expression(),
        input.expression_attribute_names(),
        input.expression_attribute_values(),
    )?;
    validate_query_attribute_values(input)?;
    Ok(())
}

/// Encodes PutItem using preloaded schema and attribute-list state.
pub(crate) fn encode_put_item_with_registry(
    input: &PutItemInput,
    schemas: &SchemaRegistry,
) -> Result<Vec<u8>, RequestError> {
    let table_name = input
        .table_name()
        .ok_or(RequestError::MissingRequiredField("TableName"))?;
    let item = input
        .item()
        .ok_or(RequestError::MissingRequiredField("Item"))?;
    let key_schema = schemas.key_schema(table_name)?;
    let attribute_list_id =
        schemas.attribute_list_id(&non_key_attribute_names(item, &key_schema))?;
    encode_put_item(input, &key_schema, attribute_list_id)
}

pub(crate) fn non_key_attribute_names(
    item: &std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>,
    key_schema: &[AttributeDefinition],
) -> Vec<String> {
    let mut names = item
        .keys()
        .filter(|name| {
            key_schema
                .iter()
                .all(|definition| name.as_str() != definition.attribute_name())
        })
        .cloned()
        .collect::<Vec<_>>();
    names.sort_unstable();
    names
}

fn reject_get_options(input: &GetItemInput) -> Result<(), RequestError> {
    if !input.attributes_to_get().is_empty() {
        return Err(RequestError::UnsupportedExpression("ProjectionExpression"));
    }
    let Some(projection) = input.projection_expression() else {
        if input.expression_attribute_names().is_some() {
            return Err(RequestError::UnsupportedExpression(
                "GetItem unused attribute-name alias",
            ));
        }
        return Ok(());
    };
    if let Some(names) = input.expression_attribute_names() {
        if query_attribute_name_aliases(projection).len() != names.len() {
            return Err(RequestError::UnsupportedExpression(
                "GetItem unused attribute-name alias",
            ));
        }
    }
    Ok(())
}

fn reject_put_options(input: &PutItemInput) -> Result<(), RequestError> {
    if input.expected().is_some()
        || input.conditional_operator().is_some()
        || input.return_values_on_condition_check_failure().is_some()
    {
        return Err(RequestError::UnsupportedExpression("ConditionExpression"));
    }
    validate_condition_expression_attributes(
        input.condition_expression(),
        input.expression_attribute_names(),
        input.expression_attribute_values(),
    )?;
    Ok(())
}

fn reject_delete_options(input: &DeleteItemInput) -> Result<(), RequestError> {
    if input.expected().is_some()
        || input.conditional_operator().is_some()
        || input.return_values_on_condition_check_failure().is_some()
    {
        return Err(RequestError::UnsupportedExpression("ConditionExpression"));
    }
    validate_condition_expression_attributes(
        input.condition_expression(),
        input.expression_attribute_names(),
        input.expression_attribute_values(),
    )?;
    Ok(())
}

fn validate_condition_expression_attributes(
    condition: Option<&str>,
    names: Option<&std::collections::HashMap<String, String>>,
    values: Option<&std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>>,
) -> Result<(), RequestError> {
    let Some(condition) = condition else {
        if names.is_some() || values.is_some() {
            return Err(RequestError::UnsupportedExpression(
                "ConditionExpression unused expression attributes",
            ));
        }
        return Ok(());
    };
    if let Some(names) = names {
        if query_attribute_name_aliases(condition).len() != names.len() {
            return Err(RequestError::UnsupportedExpression(
                "ConditionExpression unused attribute-name alias",
            ));
        }
    }
    let resolved = resolve_query_attribute_names(condition, names)?;
    let filter = parse_query_filter(&resolved).map_err(|error| match error {
        RequestError::UnsupportedExpression(
            "FilterExpression (only up to two conditions joined by `AND` or `OR` are supported)",
        ) => RequestError::UnsupportedExpression(
            "ConditionExpression (only up to two conditions joined by `AND` or `OR` are supported)",
        ),
        error => error,
    })?;
    let placeholders = filter
        .conditions
        .iter()
        .flat_map(|condition| condition.placeholders())
        .collect::<std::collections::HashSet<_>>();
    if placeholders.is_empty() {
        if values.is_some_and(|values| !values.is_empty()) {
            return Err(RequestError::UnsupportedExpression(
                "ConditionExpression unused expression attribute value",
            ));
        }
    } else {
        let values = values.ok_or(RequestError::MissingRequiredField(
            "ExpressionAttributeValues",
        ))?;
        if placeholders.len() != values.len() {
            return Err(RequestError::UnsupportedExpression(
                "ConditionExpression unused expression attribute value",
            ));
        }
    }
    Ok(())
}

fn reject_scan_options(input: &ScanInput) -> Result<(), RequestError> {
    if input.segment().is_some()
        || input.total_segments().is_some()
        || !input.attributes_to_get().is_empty()
    {
        return Err(RequestError::UnsupportedExpression("Scan options"));
    }
    Ok(())
}

fn validate_scan_expressions(input: &ScanInput) -> Result<(), RequestError> {
    let projection = input.projection_expression();
    let filter = input.filter_expression();
    if filter.is_none() && input.expression_attribute_values().is_some() {
        return Err(RequestError::UnsupportedExpression(
            "Scan unused expression attribute value",
        ));
    }
    if projection.is_none() && filter.is_none() {
        if input.expression_attribute_names().is_some() {
            return Err(RequestError::UnsupportedExpression(
                "Scan unused expression attributes",
            ));
        }
        return Ok(());
    }
    if let Some(names) = input.expression_attribute_names() {
        let used = [projection, filter]
            .into_iter()
            .flatten()
            .flat_map(query_attribute_name_aliases)
            .collect::<std::collections::HashSet<_>>();
        if used.len() != names.len() {
            return Err(RequestError::UnsupportedExpression(
                "Scan unused attribute-name alias",
            ));
        }
    }
    let Some(filter) = filter else {
        return Ok(());
    };
    let filter = resolve_query_attribute_names(filter, input.expression_attribute_names())?;
    let filter = parse_query_filter(&filter)?;
    let placeholders = filter
        .conditions
        .iter()
        .flat_map(|condition| condition.placeholders())
        .collect::<std::collections::HashSet<_>>();
    let values = input
        .expression_attribute_values()
        .ok_or(RequestError::MissingRequiredField(
            "ExpressionAttributeValues",
        ))?;
    if placeholders.len() != values.len() {
        return Err(RequestError::UnsupportedExpression(
            "Scan unused expression attribute value",
        ));
    }
    Ok(())
}

fn query_attribute_name_aliases(expression: &str) -> std::collections::HashSet<String> {
    let mut used = std::collections::HashSet::new();
    let mut characters = expression.chars().peekable();
    while let Some(character) = characters.next() {
        if character != '#' {
            continue;
        }
        let mut alias = String::from("#");
        while let Some(character) =
            characters.next_if(|character| character.is_ascii_alphanumeric() || *character == '_')
        {
            alias.push(character);
        }
        if alias.len() > 1 {
            used.insert(alias);
        }
    }
    used
}

fn reject_query_options(input: &QueryInput) -> Result<(), RequestError> {
    if input.key_conditions().is_some()
        || input.query_filter().is_some()
        || input.conditional_operator().is_some()
        || !input.attributes_to_get().is_empty()
    {
        return Err(RequestError::UnsupportedExpression(
            "Query expressions or legacy options",
        ));
    }

    Ok(())
}

fn encode_projection_expression(
    projection: Option<&str>,
    names: Option<&std::collections::HashMap<String, String>>,
) -> Result<Option<Vec<u8>>, RequestError> {
    let Some(projection) = projection else {
        return Ok(None);
    };
    if contains_non_ascii_expression_whitespace(projection) {
        return Err(RequestError::UnsupportedExpression("ProjectionExpression"));
    }
    let paths = projection
        .split(',')
        .map(|path| parse_projection_path(path, names))
        .collect::<Result<Vec<_>, _>>()?;
    let mut encoded = Vec::new();
    write_type(&mut encoded, MAJOR_ARRAY, 2);
    write_i64(&mut encoded, 1);
    write_type(&mut encoded, MAJOR_ARRAY, paths.len() as u64);
    for path in paths {
        write_type(&mut encoded, MAJOR_ARRAY, (path.len() + 1) as u64);
        write_i64(&mut encoded, 18);
        for segment in path {
            match segment {
                ProjectionPathSegment::Attribute(attribute) => write_text(&mut encoded, &attribute),
                ProjectionPathSegment::ListIndex(index) => {
                    write_type(&mut encoded, MAJOR_TAG, DOCUMENT_PATH_LIST_INDEX_TAG);
                    write_i64(&mut encoded, index);
                }
            }
        }
    }

    enum ProjectionPathSegment {
        Attribute(String),
        ListIndex(i64),
    }

    fn parse_projection_path(
        path: &str,
        names: Option<&std::collections::HashMap<String, String>>,
    ) -> Result<Vec<ProjectionPathSegment>, RequestError> {
        let mut segments = Vec::new();
        for component in path.trim().split('.') {
            let component = component.trim();
            let attribute_end = component.find('[').unwrap_or(component.len());
            let attribute = component[..attribute_end].trim();
            let is_alias = attribute.starts_with('#');
            let resolved_attribute = resolve_projection_attribute_names(attribute, names)?;
            let attribute = resolved_attribute.as_ref();
            if attribute.is_empty() || (!is_alias && !valid_query_attribute(attribute)) {
                return Err(RequestError::UnsupportedExpression(
                    "ProjectionExpression (only attribute document paths are supported)",
                ));
            }
            segments.push(ProjectionPathSegment::Attribute(
                resolved_attribute.into_owned(),
            ));
            let mut indices = &component[attribute_end..];
            while !indices.is_empty() {
                let index = indices
                    .strip_prefix('[')
                    .and_then(|indices| indices.split_once(']'))
                    .and_then(|(index, remainder)| {
                        parse_nonnegative_list_index(index.trim()).map(|index| (index, remainder))
                    })
                    .ok_or(RequestError::UnsupportedExpression(
                        "ProjectionExpression (only non-negative list indexes are supported)",
                    ))?;
                segments.push(ProjectionPathSegment::ListIndex(index.0));
                indices = index.1.trim();
            }
        }
        Ok(segments)
    }
    Ok(Some(encoded))
}

fn parse_nonnegative_list_index(index: &str) -> Option<i64> {
    if index.is_empty() || !index.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    // The Go encoder ignores strconv.ParseInt errors, which normalizes an
    // oversized digit-only index to zero.
    Some(index.parse::<i64>().unwrap_or_default())
}

fn write_select(output: &mut Vec<u8>, select: Option<&Select>) -> Result<(), RequestError> {
    match select.map(Select::as_str) {
        None | Some("ALL_ATTRIBUTES") => Ok(()),
        Some("COUNT") => {
            write_i64(output, 15);
            write_i64(output, 3);
            Ok(())
        }
        _ => Err(RequestError::UnsupportedExpression("Select")),
    }
}

fn encode_query_key_condition(input: &QueryInput) -> Result<Vec<u8>, RequestError> {
    let expression = input
        .key_condition_expression()
        .ok_or(RequestError::MissingRequiredField("KeyConditionExpression"))?;
    if contains_non_ascii_expression_whitespace(expression) {
        return Err(RequestError::UnsupportedExpression(
            "KeyConditionExpression",
        ));
    }
    if expression.contains('[') || expression.contains(']') {
        return Err(RequestError::UnsupportedExpression(
            "KeyConditionExpression document path",
        ));
    }
    let expression = resolve_query_attribute_names(expression, input.expression_attribute_names())?;
    let conditions = split_query_equalities(&expression)?;
    if conditions
        .iter()
        .any(|condition| !valid_query_attribute(condition.attribute))
    {
        return Err(RequestError::UnsupportedExpression(
            "KeyConditionExpression document path",
        ));
    }
    let values = input
        .expression_attribute_values()
        .ok_or(RequestError::MissingRequiredField(
            "ExpressionAttributeValues",
        ))?;
    let value_count: usize = conditions
        .iter()
        .map(|condition| condition.placeholders.len())
        .sum();
    let mut expression = Vec::new();
    write_type(&mut expression, MAJOR_ARRAY, 3);
    write_i64(&mut expression, 1);
    let mut variable_id = 0_i64;
    if conditions.len() == 1 {
        encode_query_comparison(&mut expression, &conditions[0], variable_id)?;
    } else {
        write_type(&mut expression, MAJOR_ARRAY, 3);
        write_i64(&mut expression, 6);
        encode_query_comparison(&mut expression, &conditions[0], variable_id)?;
        variable_id += conditions[0].placeholders.len() as i64;
        encode_query_comparison(&mut expression, &conditions[1], variable_id)?;
    }

    write_type(&mut expression, MAJOR_ARRAY, value_count as u64);
    for condition in conditions {
        for placeholder in condition.placeholders {
            expression.extend(encode_attribute_value(values.get(placeholder).ok_or(
                RequestError::UnsupportedExpression("KeyConditionExpression placeholder"),
            )?)?);
        }
    }
    Ok(expression)
}

fn encode_filter_expression(
    filter: Option<&str>,
    names: Option<&std::collections::HashMap<String, String>>,
    values: Option<&std::collections::HashMap<String, aws_sdk_dynamodb::types::AttributeValue>>,
) -> Result<Option<Vec<u8>>, RequestError> {
    let Some(filter) = filter else {
        return Ok(None);
    };
    if contains_non_ascii_expression_whitespace(filter) {
        return Err(RequestError::UnsupportedExpression("FilterExpression"));
    }
    let filter = resolve_query_attribute_names(filter, names)?;
    let filter = parse_query_filter(&filter)?;
    let mut encoded = Vec::new();
    write_type(&mut encoded, MAJOR_ARRAY, 3);
    write_i64(&mut encoded, 1);
    if filter.negated {
        write_type(&mut encoded, MAJOR_ARRAY, 2);
        write_i64(&mut encoded, 8);
        encode_query_filter_condition(&mut encoded, &filter.conditions[0], 0)?;
    } else if filter.conditions.len() == 1 {
        encode_query_filter_condition(&mut encoded, &filter.conditions[0], 0)?;
    } else {
        write_type(&mut encoded, MAJOR_ARRAY, 3);
        write_i64(
            &mut encoded,
            filter.operator.expect("two conditions have an operator"),
        );
        encode_query_filter_condition(&mut encoded, &filter.conditions[0], 0)?;
        encode_query_filter_condition(
            &mut encoded,
            &filter.conditions[1],
            filter.conditions[0].placeholders().len() as i64,
        )?;
    }
    let placeholders = filter
        .conditions
        .iter()
        .flat_map(|condition| condition.placeholders())
        .collect::<Vec<_>>();
    write_type(&mut encoded, MAJOR_ARRAY, placeholders.len() as u64);
    for placeholder in placeholders {
        let values = values.ok_or(RequestError::MissingRequiredField(
            "ExpressionAttributeValues",
        ))?;
        encoded.extend(encode_attribute_value(values.get(placeholder).ok_or(
            RequestError::UnsupportedExpression("FilterExpression placeholder"),
        )?)?);
    }
    Ok(Some(encoded))
}

fn validate_query_attribute_values(input: &QueryInput) -> Result<(), RequestError> {
    let values = input
        .expression_attribute_values()
        .ok_or(RequestError::MissingRequiredField(
            "ExpressionAttributeValues",
        ))?;
    let key_condition = input
        .key_condition_expression()
        .ok_or(RequestError::MissingRequiredField("KeyConditionExpression"))?;
    let key_condition =
        resolve_query_attribute_names(key_condition, input.expression_attribute_names())?;
    let mut used = split_query_equalities(&key_condition)?
        .into_iter()
        .flat_map(|condition| condition.placeholders)
        .map(str::to_owned)
        .collect::<std::collections::HashSet<_>>();
    if let Some(filter) = input.filter_expression() {
        let filter = resolve_query_attribute_names(filter, input.expression_attribute_names())?;
        used.extend(
            parse_query_filter(&filter)?
                .conditions
                .into_iter()
                .flat_map(|condition| {
                    condition
                        .placeholders()
                        .into_iter()
                        .map(str::to_owned)
                        .collect::<Vec<_>>()
                }),
        );
    }
    if used.len() != values.len() {
        return Err(RequestError::UnsupportedExpression(
            "Query unused expression attribute value",
        ));
    }
    Ok(())
}

struct QueryFilter<'a> {
    conditions: Vec<QueryFilterCondition<'a>>,
    operator: Option<i64>,
    negated: bool,
}

enum QueryFilterCondition<'a> {
    Comparison(QueryComparison<'a>),
    AttributeComparison {
        left: &'a str,
        right: &'a str,
        operator: QueryComparisonOperator,
    },
    AttributeExists {
        attribute: &'a str,
        exists: bool,
    },
    AttributeType {
        attribute: &'a str,
        argument: AttributeTypeArgument<'a>,
    },
    Contains {
        attribute: &'a str,
        placeholder: &'a str,
    },
    BeginsWith {
        attribute: &'a str,
        argument: AttributeTypeArgument<'a>,
    },
    In {
        attribute: &'a str,
        arguments: Vec<AttributeTypeArgument<'a>>,
    },
    SizeComparison {
        attribute: &'a str,
        sized_attribute: &'a str,
        operator: QueryComparisonOperator,
    },
}

impl QueryFilterCondition<'_> {
    fn placeholders(&self) -> Vec<&str> {
        match self {
            Self::Comparison(condition) => condition.placeholders.clone(),
            Self::AttributeComparison { .. } => Vec::new(),
            Self::AttributeExists { .. } => Vec::new(),
            Self::AttributeType { argument, .. } => match argument {
                AttributeTypeArgument::Literal(_) => Vec::new(),
                AttributeTypeArgument::Placeholder(placeholder) => vec![*placeholder],
            },
            Self::Contains { placeholder, .. } => vec![*placeholder],
            Self::BeginsWith { argument, .. } => match argument {
                AttributeTypeArgument::Literal(_) => Vec::new(),
                AttributeTypeArgument::Placeholder(placeholder) => vec![*placeholder],
            },
            Self::In { arguments, .. } => arguments
                .iter()
                .filter_map(|argument| match argument {
                    AttributeTypeArgument::Literal(_) => None,
                    AttributeTypeArgument::Placeholder(placeholder) => Some(*placeholder),
                })
                .collect(),
            Self::SizeComparison { .. } => Vec::new(),
        }
    }
}

enum AttributeTypeArgument<'a> {
    Literal(&'a str),
    Placeholder(&'a str),
}

fn parse_query_filter(expression: &str) -> Result<QueryFilter<'_>, RequestError> {
    if let Some(expression) = negated_query_filter_expression(expression) {
        let condition = parse_query_filter_condition(expression)?;
        validate_query_filter_condition(&condition)?;
        return Ok(QueryFilter {
            conditions: vec![condition],
            operator: None,
            negated: true,
        });
    }
    if let Ok(condition) = parse_query_comparison(expression) {
        let condition = QueryFilterCondition::Comparison(condition);
        validate_query_filter_condition(&condition)?;
        return Ok(QueryFilter {
            conditions: vec![condition],
            operator: None,
            negated: false,
        });
    }
    if let Some(condition) = parse_query_attribute_exists(expression) {
        validate_query_filter_condition(&condition)?;
        return Ok(QueryFilter {
            conditions: vec![condition],
            operator: None,
            negated: false,
        });
    }
    if let Some(condition) = parse_query_attribute_type(expression) {
        validate_query_filter_condition(&condition)?;
        return Ok(QueryFilter {
            conditions: vec![condition],
            operator: None,
            negated: false,
        });
    }
    if let Some(condition) = parse_query_contains(expression) {
        validate_query_filter_condition(&condition)?;
        return Ok(QueryFilter {
            conditions: vec![condition],
            operator: None,
            negated: false,
        });
    }
    if let Some(condition) = parse_query_attribute_comparison(expression) {
        validate_query_filter_condition(&condition)?;
        return Ok(QueryFilter {
            conditions: vec![condition],
            operator: None,
            negated: false,
        });
    }
    if let Some(condition) = parse_query_begins_with(expression) {
        validate_query_filter_condition(&condition)?;
        return Ok(QueryFilter {
            conditions: vec![condition],
            operator: None,
            negated: false,
        });
    }
    if let Some(condition) = parse_query_in(expression) {
        validate_query_filter_condition(&condition)?;
        return Ok(QueryFilter {
            conditions: vec![condition],
            operator: None,
            negated: false,
        });
    }
    if let Some(condition) = parse_query_size_comparison(expression) {
        validate_query_filter_condition(&condition)?;
        return Ok(QueryFilter {
            conditions: vec![condition],
            operator: None,
            negated: false,
        });
    }
    let (first, second, operator) = split_query_filter_conditions(expression)
        .map(|(first, second)| (first, second, 6))
        .or_else(|| split_query_keyword(expression, "OR").map(|(first, second)| (first, second, 7)))
        .ok_or(RequestError::UnsupportedExpression(
            "FilterExpression (only up to two conditions joined by `AND` or `OR` are supported)",
        ))?;
    let conditions = vec![
        parse_query_filter_condition(first)?,
        parse_query_filter_condition(second)?,
    ];
    for condition in &conditions {
        validate_query_filter_condition(condition)?;
    }
    Ok(QueryFilter {
        conditions,
        operator: Some(operator),
        negated: false,
    })
}

fn parse_query_filter_condition(
    expression: &str,
) -> Result<QueryFilterCondition<'_>, RequestError> {
    if let Some(condition) = parse_query_attribute_comparison(expression) {
        return Ok(condition);
    }
    if let Some(condition) = parse_query_attribute_exists(expression) {
        return Ok(condition);
    }

    if let Some(condition) = parse_query_attribute_type(expression) {
        return Ok(condition);
    }
    if let Some(condition) = parse_query_contains(expression) {
        return Ok(condition);
    }
    if let Some(condition) = parse_query_begins_with(expression) {
        return Ok(condition);
    }
    if let Some(condition) = parse_query_in(expression) {
        return Ok(condition);
    }
    if let Some(condition) = parse_query_size_comparison(expression) {
        return Ok(condition);
    }
    Ok(QueryFilterCondition::Comparison(parse_query_comparison(
        expression,
    )?))
}

fn parse_query_attribute_comparison(expression: &str) -> Option<QueryFilterCondition<'_>> {
    let (left, right, operator) =
        ["<>", "<=", ">=", "=", "<", ">"]
            .iter()
            .find_map(|operator| {
                expression
                    .split_once(operator)
                    .map(|(left, right)| (left.trim(), right.trim(), *operator))
            })?;
    let operator = match operator {
        "=" => QueryComparisonOperator::Equal,
        "<>" => QueryComparisonOperator::NotEqual,
        "<" => QueryComparisonOperator::LessThan,
        "<=" => QueryComparisonOperator::LessThanOrEqual,
        ">" => QueryComparisonOperator::GreaterThan,
        ">=" => QueryComparisonOperator::GreaterThanOrEqual,
        _ => unreachable!(),
    };
    (valid_document_path(left) && valid_document_path(right)).then_some(
        QueryFilterCondition::AttributeComparison {
            left,
            right,
            operator,
        },
    )
}

fn parse_query_attribute_exists(expression: &str) -> Option<QueryFilterCondition<'_>> {
    let (attribute, exists) = function_arguments(expression, "attribute_exists")
        .map(|attribute| (attribute, true))
        .or_else(|| {
            function_arguments(expression, "attribute_not_exists")
                .map(|attribute| (attribute, false))
        })?;
    valid_document_path(attribute)
        .then_some(QueryFilterCondition::AttributeExists { attribute, exists })
}

fn parse_query_attribute_type(expression: &str) -> Option<QueryFilterCondition<'_>> {
    let arguments = function_arguments(expression, "attribute_type")?;
    let (attribute, attribute_type) = arguments.split_once(',')?;
    let attribute = attribute.trim();
    let argument = attribute_type.trim();
    let argument = if valid_dynamodb_attribute_type(argument) {
        AttributeTypeArgument::Literal(argument)
    } else if valid_query_placeholder(argument) {
        AttributeTypeArgument::Placeholder(argument)
    } else {
        return None;
    };
    valid_document_path(attribute).then_some(QueryFilterCondition::AttributeType {
        attribute,
        argument,
    })
}

fn valid_dynamodb_attribute_type(attribute_type: &str) -> bool {
    matches!(
        attribute_type,
        "S" | "N" | "B" | "BOOL" | "NULL" | "L" | "M" | "SS" | "NS" | "BS"
    )
}

fn parse_query_contains(expression: &str) -> Option<QueryFilterCondition<'_>> {
    let arguments = function_arguments(expression, "contains")?;
    let (attribute, placeholder) = arguments.split_once(',')?;
    let attribute = attribute.trim();
    let placeholder = placeholder.trim();
    (valid_document_path(attribute) && valid_query_placeholder(placeholder)).then_some(
        QueryFilterCondition::Contains {
            attribute,
            placeholder,
        },
    )
}

fn parse_query_begins_with(expression: &str) -> Option<QueryFilterCondition<'_>> {
    let arguments = function_arguments(expression, "begins_with")?;
    let (attribute, argument) = arguments.split_once(',')?;
    let attribute = attribute.trim();
    let argument = argument.trim();
    let argument = if valid_query_placeholder(argument) {
        AttributeTypeArgument::Placeholder(argument)
    } else if !argument.is_empty() && !argument.contains(is_expression_whitespace) {
        AttributeTypeArgument::Literal(argument)
    } else {
        return None;
    };
    valid_document_path(attribute).then_some(QueryFilterCondition::BeginsWith {
        attribute,
        argument,
    })
}

fn negated_query_filter_expression(expression: &str) -> Option<&str> {
    let expression = expression.trim();
    let (keyword, remainder) = expression.split_once(is_expression_whitespace)?;
    if !keyword.eq_ignore_ascii_case("NOT") {
        return None;
    }
    let expression = remainder.trim_start();
    Some(
        expression
            .strip_prefix('(')
            .and_then(|expression| expression.strip_suffix(')'))
            .unwrap_or(expression),
    )
}

fn split_query_filter_conditions(expression: &str) -> Option<(&str, &str)> {
    let (first, remainder) = split_query_keyword(expression, "AND")?;
    if split_query_keyword(first, "BETWEEN").is_none() {
        return Some((first, remainder));
    }
    let (upper_bound, second) = split_query_keyword(remainder, "AND")?;
    let between_end = expression.len() - remainder.len() + upper_bound.len();
    Some((&expression[..between_end], second))
}

fn function_arguments<'a>(expression: &'a str, name: &str) -> Option<&'a str> {
    let (function, arguments) = expression.trim().split_once('(')?;
    function
        .trim()
        .eq_ignore_ascii_case(name)
        .then(|| arguments.strip_suffix(')').map(str::trim))?
}

fn split_query_keyword<'a>(expression: &'a str, keyword: &str) -> Option<(&'a str, &'a str)> {
    let mut whitespace_start = None;
    for (index, character) in expression.char_indices() {
        if is_expression_whitespace(character) {
            whitespace_start.get_or_insert(index);
            continue;
        }
        let Some(start) = whitespace_start.take() else {
            continue;
        };
        let Some(candidate) = expression.get(index..index + keyword.len()) else {
            continue;
        };
        let after_keyword = index + keyword.len();
        if candidate.eq_ignore_ascii_case(keyword)
            && expression[after_keyword..]
                .chars()
                .next()
                .is_some_and(is_expression_whitespace)
        {
            return Some((&expression[..start], &expression[after_keyword..]));
        }
    }
    None
}

fn is_expression_whitespace(character: char) -> bool {
    matches!(character, ' ' | '\t' | '\r' | '\n')
}

fn contains_non_ascii_expression_whitespace(expression: &str) -> bool {
    expression
        .chars()
        .any(|character| character.is_whitespace() && !is_expression_whitespace(character))
}

fn parse_query_in(expression: &str) -> Option<QueryFilterCondition<'_>> {
    let (attribute, arguments) = split_query_keyword(expression.trim(), "IN")?;
    let arguments = arguments
        .trim()
        .strip_prefix('(')?
        .strip_suffix(')')?
        .split(',')
        .map(str::trim)
        .map(|argument| {
            if valid_query_placeholder(argument) {
                AttributeTypeArgument::Placeholder(argument)
            } else {
                AttributeTypeArgument::Literal(argument)
            }
        })
        .collect::<Vec<_>>();
    (valid_document_path(attribute.trim())
        && (2..=100).contains(&arguments.len())
        && arguments.iter().all(|argument| match argument {
            AttributeTypeArgument::Literal(value) => !value.is_empty(),
            AttributeTypeArgument::Placeholder(_) => true,
        }))
    .then_some(QueryFilterCondition::In {
        attribute: attribute.trim(),
        arguments,
    })
}

fn parse_query_size_comparison(expression: &str) -> Option<QueryFilterCondition<'_>> {
    let (attribute, sized_expression, operator) = ["<>", "<=", ">=", "=", "<", ">"]
        .iter()
        .find_map(|operator| {
            expression
                .split_once(operator)
                .map(|(attribute, value)| (attribute.trim(), value.trim(), *operator))
        })?;
    let sized_attribute = function_arguments(sized_expression, "size")?;
    let operator = match operator {
        "=" => QueryComparisonOperator::Equal,
        "<>" => QueryComparisonOperator::NotEqual,
        "<" => QueryComparisonOperator::LessThan,
        "<=" => QueryComparisonOperator::LessThanOrEqual,
        ">" => QueryComparisonOperator::GreaterThan,
        ">=" => QueryComparisonOperator::GreaterThanOrEqual,
        _ => unreachable!(),
    };
    (valid_document_path(attribute) && valid_document_path(sized_attribute)).then_some(
        QueryFilterCondition::SizeComparison {
            attribute,
            sized_attribute,
            operator,
        },
    )
}

fn validate_query_filter_condition(
    condition: &QueryFilterCondition<'_>,
) -> Result<(), RequestError> {
    let QueryFilterCondition::Comparison(condition) = condition else {
        return Ok(());
    };
    if !matches!(
        condition.operator,
        QueryComparisonOperator::Equal
            | QueryComparisonOperator::NotEqual
            | QueryComparisonOperator::LessThan
            | QueryComparisonOperator::LessThanOrEqual
            | QueryComparisonOperator::GreaterThan
            | QueryComparisonOperator::GreaterThanOrEqual
            | QueryComparisonOperator::BeginsWith
            | QueryComparisonOperator::Between
    ) {
        return Err(RequestError::UnsupportedExpression(
            "FilterExpression (only comparisons, `begins_with`, or `BETWEEN` conditions are supported)",
        ));
    }
    Ok(())
}

fn encode_query_filter_condition(
    output: &mut Vec<u8>,
    condition: &QueryFilterCondition<'_>,
    variable_id: i64,
) -> Result<(), RequestError> {
    match condition {
        QueryFilterCondition::Comparison(condition) => {
            encode_query_comparison(output, condition, variable_id)?;
        }
        QueryFilterCondition::AttributeComparison {
            left,
            right,
            operator,
        } => {
            write_type(output, MAJOR_ARRAY, 3);
            write_i64(output, operator.dax_code());
            write_document_path(output, left)?;
            write_type(output, MAJOR_ARRAY, 2);
            write_i64(output, 18);
            let path = parse_document_path(right)?;
            for segment in path {
                match segment {
                    DocumentPathSegment::Attribute(attribute) => write_text(output, attribute),
                    DocumentPathSegment::ListIndex(index) => {
                        write_type(output, MAJOR_TAG, DOCUMENT_PATH_LIST_INDEX_TAG);
                        write_i64(output, index);
                    }
                }
            }
        }
        QueryFilterCondition::AttributeExists { attribute, exists } => {
            write_type(output, MAJOR_ARRAY, 2);
            write_i64(output, if *exists { 11 } else { 12 });
            write_document_path(output, attribute)?;
        }
        QueryFilterCondition::AttributeType {
            attribute,
            argument,
        } => {
            write_type(output, MAJOR_ARRAY, 3);
            write_i64(output, 13);
            write_document_path(output, attribute)?;
            write_type(output, MAJOR_ARRAY, 2);
            match argument {
                AttributeTypeArgument::Literal(attribute_type) => {
                    write_i64(output, 18);
                    write_text(output, attribute_type);
                }
                AttributeTypeArgument::Placeholder(_) => {
                    write_i64(output, 17);
                    write_i64(output, variable_id);
                }
            }
        }
        QueryFilterCondition::Contains {
            attribute,
            placeholder: _,
        } => {
            write_type(output, MAJOR_ARRAY, 3);
            write_i64(output, 15);
            write_document_path(output, attribute)?;
            write_type(output, MAJOR_ARRAY, 2);
            write_i64(output, 17);
            write_i64(output, variable_id);
        }
        QueryFilterCondition::BeginsWith {
            attribute,
            argument,
        } => {
            write_type(output, MAJOR_ARRAY, 3);
            write_i64(output, 14);
            write_document_path(output, attribute)?;
            write_type(output, MAJOR_ARRAY, 2);
            match argument {
                AttributeTypeArgument::Literal(value) => {
                    write_i64(output, 18);
                    write_text(output, value);
                }
                AttributeTypeArgument::Placeholder(_) => {
                    write_i64(output, 17);
                    write_i64(output, variable_id);
                }
            }
        }
        QueryFilterCondition::In {
            attribute,
            arguments,
        } => {
            write_type(output, MAJOR_ARRAY, 3);
            write_i64(output, 10);
            write_document_path(output, attribute)?;
            write_type(output, MAJOR_ARRAY, arguments.len() as u64);
            let mut next_variable_id = variable_id;
            for argument in arguments {
                write_type(output, MAJOR_ARRAY, 2);
                match argument {
                    AttributeTypeArgument::Literal(value) => {
                        write_i64(output, 18);
                        write_text(output, value);
                    }
                    AttributeTypeArgument::Placeholder(_) => {
                        write_i64(output, 17);
                        write_i64(output, next_variable_id);
                        next_variable_id += 1;
                    }
                }
            }
        }
        QueryFilterCondition::SizeComparison {
            attribute,
            sized_attribute,
            operator,
        } => {
            write_type(output, MAJOR_ARRAY, 3);
            write_i64(output, operator.dax_code());
            write_document_path(output, attribute)?;
            write_type(output, MAJOR_ARRAY, 2);
            write_i64(output, 16);
            write_document_path(output, sized_attribute)?;
        }
    }
    Ok(())
}

fn split_query_equalities(expression: &str) -> Result<Vec<QueryComparison<'_>>, RequestError> {
    let (partition, sort) = split_query_terms(expression);
    let mut conditions = vec![parse_query_comparison(partition)?];
    if let Some(sort) = sort {
        conditions.push(parse_query_comparison(sort)?);
    }
    if !(1..=2).contains(&conditions.len()) {
        return Err(RequestError::UnsupportedExpression(
            "KeyConditionExpression (only a partition-key equality and optional sort-key comparison are supported)",
        ));
    }
    if conditions[0].operator != QueryComparisonOperator::Equal {
        return Err(RequestError::UnsupportedExpression(
            "KeyConditionExpression (the first term must use `=`)",
        ));
    }
    if conditions.get(1).is_some_and(|condition| {
        !matches!(
            condition.operator,
            QueryComparisonOperator::Equal
                | QueryComparisonOperator::LessThan
                | QueryComparisonOperator::LessThanOrEqual
                | QueryComparisonOperator::GreaterThan
                | QueryComparisonOperator::GreaterThanOrEqual
                | QueryComparisonOperator::BeginsWith
                | QueryComparisonOperator::Between
        )
    }) {
        return Err(RequestError::UnsupportedExpression(
            "KeyConditionExpression (the sort key does not support this comparison)",
        ));
    }
    Ok(conditions)
}

fn split_query_terms(expression: &str) -> (&str, Option<&str>) {
    split_query_keyword(expression, "AND").map_or((expression, None), |(partition, sort)| {
        (partition, Some(sort))
    })
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum QueryComparisonOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    BeginsWith,
    Between,
}

impl QueryComparisonOperator {
    const fn dax_code(self) -> i64 {
        match self {
            Self::Equal => 0,
            Self::NotEqual => 1,
            Self::LessThan => 2,
            Self::LessThanOrEqual => 5,
            Self::GreaterThan => 4,
            Self::GreaterThanOrEqual => 3,
            Self::BeginsWith => 14,
            Self::Between => 9,
        }
    }
}

struct QueryComparison<'a> {
    attribute: &'a str,
    placeholders: Vec<&'a str>,
    operator: QueryComparisonOperator,
}

fn parse_query_comparison(expression: &str) -> Result<QueryComparison<'_>, RequestError> {
    if let Some((attribute, bounds)) = split_query_keyword(expression.trim(), "BETWEEN") {
        let (lower, upper) = split_query_keyword(bounds, "AND")
            .map(|(lower, upper)| (lower.trim(), upper.trim()))
            .ok_or(RequestError::UnsupportedExpression(
                "KeyConditionExpression (BETWEEN requires two placeholders)",
            ))?;
        return query_comparison(
            attribute.trim(),
            vec![lower, upper],
            QueryComparisonOperator::Between,
        );
    }
    if let Some(arguments) = function_arguments(expression, "begins_with") {
        let (attribute, placeholder) = arguments
            .split_once(',')
            .map(|(attribute, placeholder)| (attribute.trim(), placeholder.trim()))
            .ok_or(RequestError::UnsupportedExpression(
                "KeyConditionExpression (begins_with requires an attribute and placeholder)",
            ))?;
        return query_comparison(
            attribute,
            vec![placeholder],
            QueryComparisonOperator::BeginsWith,
        );
    }

    let (attribute, placeholder, operator) = ["<>", "<=", ">=", "=", "<", ">"]
        .iter()
        .find_map(|operator| {
            expression
                .split_once(operator)
                .map(|(attribute, placeholder)| (attribute.trim(), placeholder.trim(), *operator))
        })
        .ok_or(RequestError::UnsupportedExpression(
            "KeyConditionExpression (only a simple key comparison is supported)",
        ))?;
    let operator = match operator {
        "=" => QueryComparisonOperator::Equal,
        "<>" => QueryComparisonOperator::NotEqual,
        "<" => QueryComparisonOperator::LessThan,
        "<=" => QueryComparisonOperator::LessThanOrEqual,
        ">" => QueryComparisonOperator::GreaterThan,
        ">=" => QueryComparisonOperator::GreaterThanOrEqual,
        _ => unreachable!(),
    };
    query_comparison(attribute, vec![placeholder], operator)
}

fn query_comparison<'a>(
    attribute: &'a str,
    placeholders: Vec<&'a str>,
    operator: QueryComparisonOperator,
) -> Result<QueryComparison<'a>, RequestError> {
    let valid_placeholders = placeholders
        .iter()
        .all(|placeholder| valid_query_placeholder(placeholder));
    if !valid_document_path(attribute) || !valid_placeholders {
        return Err(RequestError::UnsupportedExpression(
            "KeyConditionExpression (only `attribute operator :value` is supported)",
        ));
    }

    Ok(QueryComparison {
        attribute,
        placeholders,
        operator,
    })
}

fn valid_query_attribute(attribute: &str) -> bool {
    !attribute.is_empty()
        && attribute
            .bytes()
            .enumerate()
            .all(|(index, byte)| byte.is_ascii_alphanumeric() || (byte == b'_' && index > 0))
}

#[derive(Clone, Copy)]
enum DocumentPathSegment<'a> {
    Attribute(&'a str),
    ListIndex(i64),
}

fn parse_document_path(path: &str) -> Result<Vec<DocumentPathSegment<'_>>, RequestError> {
    let mut segments = Vec::new();
    for component in path.trim().split('.') {
        let attribute_end = component.find('[').unwrap_or(component.len());
        let attribute = component[..attribute_end].trim();
        if !valid_query_attribute(attribute) {
            return Err(RequestError::UnsupportedExpression(
                "UpdateExpression document path",
            ));
        }
        segments.push(DocumentPathSegment::Attribute(attribute));
        let mut indices = &component[attribute_end..];
        while !indices.is_empty() {
            let (index, remainder) = indices
                .strip_prefix('[')
                .and_then(|indices| indices.split_once(']'))
                .and_then(|(index, remainder)| {
                    parse_nonnegative_list_index(index.trim()).map(|index| (index, remainder))
                })
                .ok_or(RequestError::UnsupportedExpression(
                    "UpdateExpression document path index",
                ))?;
            segments.push(DocumentPathSegment::ListIndex(index));
            indices = remainder.trim();
        }
    }
    Ok(segments)
}

fn valid_document_path(path: &str) -> bool {
    parse_document_path(path).is_ok()
}

fn write_document_path(output: &mut Vec<u8>, path: &str) -> Result<(), RequestError> {
    let path = parse_document_path(path)?;
    write_type(output, MAJOR_ARRAY, (path.len() + 1) as u64);
    write_i64(output, 18);
    for segment in path {
        match segment {
            DocumentPathSegment::Attribute(attribute) => write_text(output, attribute),
            DocumentPathSegment::ListIndex(index) => {
                write_type(output, MAJOR_TAG, DOCUMENT_PATH_LIST_INDEX_TAG);
                write_i64(output, index);
            }
        }
    }
    Ok(())
}

fn valid_query_placeholder(placeholder: &str) -> bool {
    placeholder.starts_with(':')
        && placeholder.len() > 1
        && placeholder[1..]
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn encode_query_comparison(
    output: &mut Vec<u8>,
    condition: &QueryComparison<'_>,
    variable_id: i64,
) -> Result<(), RequestError> {
    let is_between = condition.operator == QueryComparisonOperator::Between;
    write_type(output, MAJOR_ARRAY, if is_between { 4 } else { 3 });
    write_i64(output, condition.operator.dax_code());
    write_document_path(output, condition.attribute)?;
    write_type(output, MAJOR_ARRAY, 2);
    write_i64(output, 17);
    write_i64(output, variable_id);
    if is_between {
        write_type(output, MAJOR_ARRAY, 2);
        write_i64(output, 17);
        write_i64(output, variable_id + 1);
    }
    Ok(())
}

fn service_and_method(method: i64) -> Vec<u8> {
    let mut output = Vec::new();
    write_i64(&mut output, DAX_SERVICE_ID);
    write_i64(&mut output, method);
    output
}

pub(crate) fn encode_endpoints() -> Vec<u8> {
    service_and_method(ENDPOINTS_METHOD_ID)
}

fn write_optional_map(
    output: &mut Vec<u8>,
    consistent_read: Option<bool>,
    return_consumed_capacity: Option<&ReturnConsumedCapacity>,
    return_item_collection_metrics: Option<&ReturnItemCollectionMetrics>,
    return_values: Option<&ReturnValue>,
) {
    output.push(MAJOR_MAP | 31);
    if let Some(value) = consistent_read {
        write_i64(output, 2);
        output.push(if value { 0xf5 } else { 0xf4 });
    }
    if let Some(value) = return_consumed_capacity.and_then(return_consumed_capacity_code) {
        write_i64(output, 3);
        write_i64(output, value);
    }
    if let Some(value) =
        return_item_collection_metrics.and_then(return_item_collection_metrics_code)
    {
        write_i64(output, 6);
        write_i64(output, value);
    }
    if let Some(value) = return_values.and_then(return_values_code) {
        write_i64(output, 7);
        write_i64(output, value);
    }
    output.push(0xff);
}

fn write_optional_map_with_token(
    output: &mut Vec<u8>,
    return_consumed_capacity: Option<&ReturnConsumedCapacity>,
    return_item_collection_metrics: Option<&ReturnItemCollectionMetrics>,
    client_request_token: Option<&str>,
) {
    output.push(MAJOR_MAP | 31);
    if let Some(value) = return_consumed_capacity.and_then(return_consumed_capacity_code) {
        write_i64(output, 3);
        write_i64(output, value);
    }
    if let Some(value) =
        return_item_collection_metrics.and_then(return_item_collection_metrics_code)
    {
        write_i64(output, 6);
        write_i64(output, value);
    }
    if let Some(token) = client_request_token {
        write_i64(output, 19);
        write_text(output, token);
    }
    output.push(0xff);
}

fn return_consumed_capacity_code(value: &ReturnConsumedCapacity) -> Option<i64> {
    match value.as_str() {
        "TOTAL" => Some(1),
        "INDEXES" => Some(2),
        _ => None,
    }
}

fn return_item_collection_metrics_code(value: &ReturnItemCollectionMetrics) -> Option<i64> {
    (value.as_str() == "SIZE").then_some(1)
}

fn return_values_code(value: &ReturnValue) -> Option<i64> {
    match value.as_str() {
        "ALL_OLD" => Some(2),
        "UPDATED_OLD" => Some(3),
        "ALL_NEW" => Some(4),
        "UPDATED_NEW" => Some(5),
        _ => None,
    }
}

fn write_i64(output: &mut Vec<u8>, value: i64) {
    if value >= 0 {
        write_type(output, MAJOR_UNSIGNED, value as u64);
    } else {
        write_type(output, MAJOR_NEGATIVE, (-value - 1) as u64);
    }
}

fn write_text(output: &mut Vec<u8>, value: &str) {
    write_type(output, MAJOR_TEXT, value.len() as u64);
    output.extend_from_slice(value.as_bytes());
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use aws_sdk_dynamodb::{
        operation::{
            batch_get_item::BatchGetItemInput, batch_write_item::BatchWriteItemInput,
            delete_item::DeleteItemInput, get_item::GetItemInput, put_item::PutItemInput,
            query::QueryInput, scan::ScanInput, transact_get_items::TransactGetItemsInput,
            transact_write_items::TransactWriteItemsInput, update_item::UpdateItemInput,
        },
        types::{
            AttributeDefinition, AttributeValue, KeysAndAttributes, ReturnConsumedCapacity,
            ReturnItemCollectionMetrics, ReturnValue, ReturnValuesOnConditionCheckFailure,
            ScalarAttributeType, Select, WriteRequest,
        },
    };

    use super::{
        RequestError, UpdateAction, encode_batch_get_item, encode_batch_write_item,
        encode_define_attribute_list, encode_define_attribute_list_id, encode_define_key_schema,
        encode_delete_item, encode_endpoints, encode_filter_expression, encode_get_item,
        encode_get_item_with_registry, encode_projection_expression, encode_put_item,
        encode_put_item_with_registry, encode_query, encode_query_key_condition, encode_scan,
        encode_transact_get_items, encode_transact_write_items, encode_update_action,
        encode_update_actions, encode_update_item, parse_update_action, parse_update_actions,
    };
    use crate::protocol::schema::{SchemaError, SchemaRegistry};

    fn key_schema() -> Vec<AttributeDefinition> {
        vec![
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("key definition is complete"),
        ]
    }

    #[test]
    fn encodes_endpoints_request() {
        assert_eq!(encode_endpoints(), [0x01, 0x1a, 0x1b, 0x2b, 0xcf, 0x02]);
    }

    #[test]
    fn encodes_batch_get_item_with_consistency_and_key() {
        let mut key = HashMap::new();
        key.insert("pk".to_owned(), AttributeValue::S("a".to_owned()));
        let attributes = KeysAndAttributes::builder()
            .keys(key)
            .consistent_read(true)
            .build()
            .expect("keys and attributes are complete");
        let input = BatchGetItemInput::builder()
            .request_items("Table", attributes)
            .build()
            .expect("batch get request is complete");
        let mut schemas = HashMap::new();
        schemas.insert("Table".to_owned(), key_schema());
        let encoded = encode_batch_get_item(&input, &schemas).expect("encodes");
        assert_eq!(
            encoded
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            "013a29985cdba1655461626c6583f5f6814161bfff"
        );
    }

    #[test]
    fn encodes_transact_get_items_parallel_arrays() {
        let mut key = HashMap::new();
        key.insert("pk".to_owned(), AttributeValue::S("a".to_owned()));
        let get = aws_sdk_dynamodb::types::Get::builder()
            .table_name("Table")
            .set_key(Some(key))
            .build()
            .expect("get is complete");
        let item = aws_sdk_dynamodb::types::TransactGetItem::builder()
            .get(get)
            .build();
        let input = TransactGetItemsInput::builder()
            .transact_items(item)
            .build()
            .expect("transaction request is complete");
        let mut schemas = HashMap::new();
        schemas.insert("Table".to_owned(), key_schema());
        let encoded = encode_transact_get_items(&input, &schemas).expect("encodes");
        assert_eq!(
            encoded
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            "011a6f3d49db81455461626c6581416181f6bfff"
        );
    }

    #[test]
    fn encodes_transact_write_put_parallel_arrays() {
        let mut item = HashMap::new();
        item.insert("pk".to_owned(), AttributeValue::S("a".to_owned()));
        item.insert("value".to_owned(), AttributeValue::N("1".to_owned()));
        let put = aws_sdk_dynamodb::types::Put::builder()
            .set_item(Some(item))
            .table_name("Table")
            .build()
            .expect("put is complete");
        let transaction_item = aws_sdk_dynamodb::types::TransactWriteItem::builder()
            .put(put)
            .build();
        let input = TransactWriteItemsInput::builder()
            .transact_items(transaction_item)
            .client_request_token("token")
            .build()
            .expect("transaction request is complete");
        let mut schemas = HashMap::new();
        schemas.insert("Table".to_owned(), key_schema());
        let mut ids = HashMap::new();
        ids.insert("Table".to_owned(), 9);
        let encoded = encode_transact_write_items(&input, &schemas, &ids).expect("encodes");
        assert_eq!(
            encoded
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            "013a4524c569810281455461626c6581424161810901f681018181f68181f6bf1365746f6b656eff"
        );
    }

    #[test]
    fn encodes_transact_write_update_action() {
        let mut key = HashMap::new();
        key.insert("pk".to_owned(), AttributeValue::S("a".to_owned()));
        let mut values = HashMap::new();
        values.insert(":v".to_owned(), AttributeValue::N("2".to_owned()));
        let update = aws_sdk_dynamodb::types::Update::builder()
            .table_name("Table")
            .set_key(Some(key))
            .update_expression("SET value = :v")
            .set_expression_attribute_values(Some(values))
            .build()
            .expect("update is complete");
        let transaction_item = aws_sdk_dynamodb::types::TransactWriteItem::builder()
            .update(update)
            .build();
        let input = TransactWriteItemsInput::builder()
            .transact_items(transaction_item)
            .client_request_token("token")
            .build()
            .expect("transaction request is complete");
        let mut schemas = HashMap::new();
        schemas.insert("Table".to_owned(), key_schema());
        let encoded =
            encode_transact_write_items(&input, &schemas, &HashMap::new()).expect("encodes");
        assert_eq!(
            encoded
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            "013a4524c569810981455461626c658142416181f6f681018181f6818152830181831382126576616c75658211008102bf1365746f6b656eff"
        );
    }

    #[test]
    fn encodes_batch_write_item_with_put_and_delete_entries() {
        let mut put_item = HashMap::new();
        put_item.insert("pk".to_owned(), AttributeValue::S("a".to_owned()));
        put_item.insert("value".to_owned(), AttributeValue::N("1".to_owned()));
        let put = WriteRequest::builder()
            .put_request(
                aws_sdk_dynamodb::types::PutRequest::builder()
                    .set_item(Some(put_item))
                    .build()
                    .expect("put request is complete"),
            )
            .build();

        let mut delete_key = HashMap::new();
        delete_key.insert("pk".to_owned(), AttributeValue::S("b".to_owned()));
        let delete = WriteRequest::builder()
            .delete_request(
                aws_sdk_dynamodb::types::DeleteRequest::builder()
                    .set_key(Some(delete_key))
                    .build()
                    .expect("delete request is complete"),
            )
            .build();

        let mut requests = HashMap::new();
        requests.insert("Table".to_owned(), vec![put, delete]);
        let input = BatchWriteItemInput::builder()
            .set_request_items(Some(requests))
            .build()
            .expect("batch request is complete");

        let mut schemas = HashMap::new();
        schemas.insert("Table".to_owned(), key_schema());
        let mut attribute_list_ids = HashMap::new();
        attribute_list_ids.insert("Table".to_owned(), 9);
        let encoded =
            encode_batch_write_item(&input, &schemas, &attribute_list_ids).expect("encodes");
        assert_eq!(
            encoded
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            "011a06ed585fa1655461626c658441614209014162f6bfff"
        );
    }

    #[test]
    fn encodes_go_control_request_streams() {
        assert_eq!(
            encode_define_key_schema("Table"),
            hex("013a2c43e27e455461626c65")
        );
        assert_eq!(
            encode_define_attribute_list_id(&["a".into(), "z".into()]),
            hex("013a495927bb826161617a")
        );
        assert_eq!(encode_define_attribute_list(7), hex("011a27f9bd7107"));
    }

    #[test]
    fn encodes_default_and_optional_get_item_streams() {
        let input = GetItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("val".into()))
            .build()
            .expect("GetItem input is complete");
        assert_eq!(
            encode_get_item(&input, &key_schema()).unwrap(),
            hex("011a0fb0cc6a455461626c654376616cbfff")
        );

        let optional = GetItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("val".into()))
            .consistent_read(true)
            .return_consumed_capacity(ReturnConsumedCapacity::Total)
            .build()
            .expect("GetItem input is complete");
        assert_eq!(
            encode_get_item(&optional, &key_schema()).unwrap(),
            hex("011a0fb0cc6a455461626c654376616cbf02f50301ff")
        );

        let projected = GetItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("val".into()))
            .projection_expression("#profile.email, #status")
            .expression_attribute_names("#profile", "profile")
            .expression_attribute_names("#status", "status")
            .build()
            .expect("GetItem input is complete");
        assert_eq!(
            encode_get_item(&projected, &key_schema()).unwrap(),
            hex(
                "011a0fb0cc6a455461626c654376616cbf00581c82018283126770726f66696c6565656d61696c821266737461747573ff"
            )
        );
    }

    #[test]
    fn encodes_default_and_optional_put_item_streams() {
        let item = HashMap::from([
            ("pk".into(), AttributeValue::S("val".into())),
            ("z".into(), AttributeValue::N("1".into())),
            ("a".into(), AttributeValue::S("x".into())),
        ]);
        let input = PutItemInput::builder()
            .table_name("Table")
            .set_item(Some(item.clone()))
            .build()
            .expect("PutItem input is complete");
        assert_eq!(
            encode_put_item(&input, &key_schema(), 9).unwrap(),
            hex("013a7d8e7e56455461626c654376616c4409617801bfff")
        );

        let optional = PutItemInput::builder()
            .table_name("Table")
            .set_item(Some(item.clone()))
            .return_consumed_capacity(ReturnConsumedCapacity::Total)
            .return_item_collection_metrics(ReturnItemCollectionMetrics::Size)
            .return_values(ReturnValue::AllOld)
            .build()
            .expect("PutItem input is complete");
        assert_eq!(
            encode_put_item(&optional, &key_schema(), 9).unwrap(),
            hex("013a7d8e7e56455461626c654376616c4409617801bf030106010702ff")
        );

        let conditional = PutItemInput::builder()
            .table_name("Table")
            .set_item(Some(item))
            .condition_expression("#status = :status")
            .expression_attribute_names("#status", "status")
            .expression_attribute_values(":status", AttributeValue::S("open".into()))
            .build()
            .expect("PutItem input is complete");
        assert_eq!(
            encode_put_item(&conditional, &key_schema(), 9).unwrap(),
            hex(
                "013a7d8e7e56455461626c654376616c4409617801bf04568301830082126673746174757382110081646f70656eff"
            )
        );
    }

    #[test]
    fn encodes_set_update_item_stream() {
        let input = UpdateItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("key".into()))
            .update_expression("SET status = :status")
            .expression_attribute_values(":status", AttributeValue::S("open".into()))
            .build()
            .expect("UpdateItem input is complete");
        assert_eq!(
            encode_update_item(&input, &key_schema()).unwrap(),
            hex(
                "011a54f89c0f455461626c65436b6579bf0857830181831382126673746174757382110081646f70656eff"
            )
        );
    }

    #[test]
    fn encodes_update_item_condition_with_update_value() {
        let input = UpdateItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("key".into()))
            .update_expression("SET status = :status")
            .condition_expression("status = :expected")
            .expression_attribute_values(":status", AttributeValue::S("open".into()))
            .expression_attribute_values(":expected", AttributeValue::S("old".into()))
            .build()
            .expect("UpdateItem input is complete");
        assert_eq!(
            encode_update_item(&input, &key_schema()).unwrap(),
            hex(
                "011a54f89c0f455461626c65436b6579bf04558301830082126673746174757382110081636f6c640857830181831382126673746174757382110081646f70656eff"
            )
        );
    }

    #[test]
    fn validates_update_item_aliases_across_update_and_condition() {
        let input = UpdateItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("key".into()))
            .update_expression("SET #status = :status")
            .condition_expression("#status = :expected")
            .expression_attribute_names("#status", "status")
            .expression_attribute_values(":status", AttributeValue::S("open".into()))
            .expression_attribute_values(":expected", AttributeValue::S("old".into()))
            .build()
            .expect("UpdateItem input is complete");
        assert!(encode_update_item(&input, &key_schema()).is_ok());
    }

    #[test]
    fn rejects_update_item_alias_unused_across_both_expressions() {
        let input = UpdateItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("key".into()))
            .update_expression("SET status = :status")
            .condition_expression("status = :expected")
            .expression_attribute_names("#unused", "unused")
            .expression_attribute_values(":status", AttributeValue::S("open".into()))
            .expression_attribute_values(":expected", AttributeValue::S("old".into()))
            .build()
            .expect("UpdateItem input is complete");
        assert_eq!(
            encode_update_item(&input, &key_schema()),
            Err(RequestError::UnsupportedExpression(
                "UpdateItem unused attribute-name alias"
            ))
        );
    }

    #[test]
    fn rejects_update_item_value_unused_across_both_expressions() {
        let input = UpdateItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("key".into()))
            .update_expression("REMOVE status")
            .condition_expression("status = :expected")
            .expression_attribute_values(":expected", AttributeValue::S("old".into()))
            .expression_attribute_values(":unused", AttributeValue::S("unused".into()))
            .build()
            .expect("UpdateItem input is complete");
        assert_eq!(
            encode_update_item(&input, &key_schema()),
            Err(RequestError::UnsupportedExpression(
                "UpdateItem unused expression attribute value"
            ))
        );
    }

    #[test]
    fn encodes_remove_update_item_stream() {
        let input = UpdateItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("key".into()))
            .update_expression("REMOVE #status")
            .expression_attribute_names("#status", "status")
            .build()
            .expect("UpdateItem input is complete");
        assert_eq!(
            encode_update_item(&input, &key_schema()).unwrap(),
            hex("011a54f89c0f455461626c65436b6579bf084f830181821682126673746174757380ff")
        );
    }

    #[test]
    fn encodes_arithmetic_update_item_stream() {
        let input = UpdateItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("key".into()))
            .update_expression("SET #count = #count + :increment")
            .expression_attribute_names("#count", "count")
            .expression_attribute_values(":increment", AttributeValue::S("one".into()))
            .build()
            .expect("UpdateItem input is complete");
        assert_eq!(
            encode_update_item(&input, &key_schema()).unwrap(),
            hex(
                "011a54f89c0f455461626c65436b6579bf0858208301818313821265636f756e74831819821265636f756e7482110081636f6e65ff"
            )
        );
    }

    #[test]
    fn encodes_subtraction_update_item_stream() {
        let input = UpdateItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("key".into()))
            .update_expression("SET count = count - :decrement")
            .expression_attribute_values(":decrement", AttributeValue::S("one".into()))
            .build()
            .expect("UpdateItem input is complete");
        assert_eq!(
            encode_update_item(&input, &key_schema()).unwrap(),
            hex(
                "011a54f89c0f455461626c65436b6579bf0858208301818313821265636f756e7483181a821265636f756e7482110081636f6e65ff"
            )
        );
    }

    #[test]
    fn encodes_go_numeric_subtraction_update_vector() {
        let input = UpdateItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("key".into()))
            .update_expression("SET Price = Price - :p")
            .expression_attribute_values(":p", AttributeValue::N("5".into()))
            .build()
            .expect("UpdateItem input is complete");
        assert_eq!(
            encode_update_item(&input, &key_schema()).unwrap(),
            hex(
                "011a54f89c0f455461626c65436b6579bf08581d8301818313821265507269636583181a82126550726963658211008105ff"
            )
        );
    }

    #[test]
    fn encodes_go_numeric_subtraction_update_payload() {
        let value = AttributeValue::N("5".into());
        let actions = vec![UpdateAction::SetArithmetic {
            attribute: "Price",
            operator: 26,
            placeholder: ":p",
        }];
        assert_eq!(
            encode_update_actions(&actions, &[&value]).expect("subtraction payload encodes"),
            hex("8301818313821265507269636583181a82126550726963658211008105")
        );
    }

    #[test]
    fn encodes_if_not_exists_update_item_stream() {
        let input = UpdateItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("key".into()))
            .update_expression("SET #count = if_not_exists(#count, :initial)")
            .expression_attribute_names("#count", "count")
            .expression_attribute_values(":initial", AttributeValue::S("one".into()))
            .build()
            .expect("UpdateItem input is complete");
        assert_eq!(
            encode_update_item(&input, &key_schema()).unwrap(),
            hex(
                "011a54f89c0f455461626c65436b6579bf08581f8301818313821265636f756e748317821265636f756e7482110081636f6e65ff"
            )
        );
    }

    #[test]
    fn encodes_list_append_update_item_stream() {
        let input = UpdateItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("key".into()))
            .update_expression("SET #items = list_append(#items, :values)")
            .expression_attribute_names("#items", "items")
            .expression_attribute_values(
                ":values",
                AttributeValue::L(vec![AttributeValue::S("next".into())]),
            )
            .build()
            .expect("UpdateItem input is complete");
        assert_eq!(
            encode_update_item(&input, &key_schema()).unwrap(),
            hex(
                "011a54f89c0f455461626c65436b6579bf08582283018183138212656974656d738318188212656974656d738211008181646e657874ff"
            )
        );
    }

    #[test]
    fn encodes_go_if_not_exists_numeric_update_vector() {
        let input = UpdateItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("key".into()))
            .update_expression("SET Price = if_not_exists(Price, :p)")
            .expression_attribute_values(":p", AttributeValue::N("10".into()))
            .build()
            .expect("UpdateItem input is complete");
        assert_eq!(
            encode_update_item(&input, &key_schema()).unwrap(),
            hex(
                "011a54f89c0f455461626c65436b6579bf08581c8301818313821265507269636583178212655072696365821100810aff"
            )
        );
    }

    #[test]
    fn encodes_go_if_not_exists_numeric_update_payload() {
        let value = AttributeValue::N("10".into());
        let actions = vec![UpdateAction::SetIfNotExists {
            attribute: "Price",
            placeholder: ":p",
        }];
        assert_eq!(
            encode_update_actions(&actions, &[&value]).expect("if_not_exists payload encodes"),
            hex("8301818313821265507269636583178212655072696365821100810a")
        );
    }

    #[test]
    fn encodes_go_list_append_numeric_update_vector() {
        let input = UpdateItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("key".into()))
            .update_expression("SET #ri = list_append(#ri, :vals)")
            .expression_attribute_names("#ri", "RelatedItems")
            .expression_attribute_values(":vals", AttributeValue::N("5".into()))
            .build()
            .expect("UpdateItem input is complete");
        assert_eq!(
            encode_update_item(&input, &key_schema()).unwrap(),
            hex(
                "011a54f89c0f455461626c65436b6579bf08582b830181831382126c52656c617465644974656d7383181882126c52656c617465644974656d738211008105ff"
            )
        );
    }

    #[test]
    fn encodes_go_list_append_numeric_update_payload() {
        let value = AttributeValue::N("5".into());
        let actions = vec![UpdateAction::SetListAppend {
            attribute: "RelatedItems",
            placeholder: ":vals",
        }];
        assert_eq!(
            encode_update_actions(&actions, &[&value]).expect("list_append payload encodes"),
            hex(
                "830181831382126c52656c617465644974656d7383181882126c52656c617465644974656d738211008105"
            )
        );
    }

    #[test]
    fn encodes_go_nested_alias_and_list_index_update_vector() {
        let input = UpdateItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("key".into()))
            .update_expression("SET #pr.#5star[1] = :r5, #pr.#3star = :r3")
            .expression_attribute_names("#pr", "a1")
            .expression_attribute_names("#5star", "k5")
            .expression_attribute_names("#3star", "k3")
            .expression_attribute_values(":r3", AttributeValue::N("3".into()))
            .expression_attribute_values(":r5", AttributeValue::N("5".into()))
            .build()
            .expect("UpdateItem input is complete");
        let encoded = encode_update_item(&input, &key_schema()).expect("update encodes");
        let expected_update =
            hex("83018283138412626131626b35d90cfc0182110083138312626131626b33821101820503");
        assert!(
            encoded
                .windows(expected_update.len())
                .any(|window| window == expected_update)
        );
    }

    #[test]
    fn encodes_go_nested_alias_and_list_index_update_payload() {
        let r5 = AttributeValue::N("5".into());
        let r3 = AttributeValue::N("3".into());
        let actions = vec![
            UpdateAction::SetValue {
                attribute: "a1.k5[1]",
                placeholder: ":r5",
            },
            UpdateAction::SetValue {
                attribute: "a1.k3",
                placeholder: ":r3",
            },
        ];
        assert_eq!(
            encode_update_actions(&actions, &[&r5, &r3]).expect("update payload encodes"),
            hex("83018283138412626131626b35d90cfc0182110083138312626131626b33821101820503")
        );
    }

    #[test]
    fn encodes_go_remove_indexed_document_paths_vector() {
        let input = UpdateItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("key".into()))
            .update_expression("REMOVE RelatedItems[1], RelatedItems[2]")
            .build()
            .expect("UpdateItem input is complete");
        let encoded = encode_update_item(&input, &key_schema()).expect("update encodes");
        let expected_update = hex(
            "830182821683126c52656c617465644974656d73d90cfc01821683126c52656c617465644974656d73d90cfc0280",
        );
        assert!(
            encoded
                .windows(expected_update.len())
                .any(|window| window == expected_update)
        );
    }

    #[test]
    fn encodes_go_remove_indexed_document_paths_payload() {
        let actions = vec![
            UpdateAction::Remove {
                attribute: "RelatedItems[1]",
            },
            UpdateAction::Remove {
                attribute: "RelatedItems[2]",
            },
        ];
        assert_eq!(
            encode_update_actions(&actions, &[]).expect("remove payload encodes"),
            hex(
                "830182821683126c52656c617465644974656d73d90cfc01821683126c52656c617465644974656d73d90cfc0280"
            )
        );
    }

    #[test]
    fn encodes_add_update_item_stream() {
        let input = UpdateItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("key".into()))
            .update_expression("ADD count :increment")
            .expression_attribute_values(":increment", AttributeValue::N("1".into()))
            .build()
            .expect("UpdateItem input is complete");
        assert_eq!(
            encode_update_item(&input, &key_schema()).unwrap(),
            hex("011a54f89c0f455461626c65436b6579bf08528301818314821265636f756e748211008101ff")
        );
    }

    #[test]
    fn encodes_go_numeric_add_update_vector() {
        let input = UpdateItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("key".into()))
            .update_expression("ADD QuantityOnHand :q")
            .expression_attribute_values(":q", AttributeValue::N("5".into()))
            .build()
            .expect("UpdateItem input is complete");
        let encoded = encode_update_item(&input, &key_schema()).expect("update encodes");
        let expected_update = hex("830181831482126e5175616e746974794f6e48616e648211008105");
        assert!(
            encoded
                .windows(expected_update.len())
                .any(|window| window == expected_update)
        );
    }

    #[test]
    fn encodes_go_numeric_add_update_payload() {
        let value = AttributeValue::N("5".into());
        let actions = vec![UpdateAction::Add {
            attribute: "QuantityOnHand",
            placeholder: ":q",
        }];
        assert_eq!(
            encode_update_actions(&actions, &[&value]).expect("add payload encodes"),
            hex("830181831482126e5175616e746974794f6e48616e648211008105")
        );
    }

    #[test]
    fn encodes_delete_update_item_stream() {
        let input = UpdateItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("key".into()))
            .update_expression("DELETE tags :tag")
            .expression_attribute_values(":tag", AttributeValue::Ss(vec!["old".into()]))
            .build()
            .expect("UpdateItem input is complete");
        assert_eq!(
            encode_update_item(&input, &key_schema()).unwrap(),
            hex(
                "011a54f89c0f455461626c65436b6579bf08581883018183158212647461677382110081d90cf981636f6c64ff"
            )
        );
    }

    #[test]
    fn encodes_go_numeric_delete_update_vector() {
        let input = UpdateItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("key".into()))
            .update_expression("DELETE Color :p")
            .expression_attribute_values(":p", AttributeValue::N("5".into()))
            .build()
            .expect("UpdateItem input is complete");
        let encoded = encode_update_item(&input, &key_schema()).expect("update encodes");
        let expected_update = hex("8301818315821265436f6c6f728211008105");
        assert!(
            encoded
                .windows(expected_update.len())
                .any(|window| window == expected_update)
        );
    }

    #[test]
    fn encodes_go_numeric_delete_update_payload() {
        let value = AttributeValue::N("5".into());
        let actions = vec![UpdateAction::Delete {
            attribute: "Color",
            placeholder: ":p",
        }];
        assert_eq!(
            encode_update_actions(&actions, &[&value]).expect("delete payload encodes"),
            hex("8301818315821265436f6c6f728211008105")
        );
    }

    #[test]
    fn encodes_go_multiple_numeric_delete_update_vector() {
        let input = UpdateItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("key".into()))
            .update_expression("DELETE Color :p, Color_2 :p")
            .expression_attribute_values(":p", AttributeValue::N("5".into()))
            .build()
            .expect("UpdateItem input is complete");
        let encoded = encode_update_item(&input, &key_schema()).expect("update encodes");
        assert_eq!(
            encoded,
            hex(
                "011a54f89c0f455461626c65436b6579bf0858218301828315821265436f6c6f728211008315821267436f6c6f725f328211008105ff"
            )
        );
    }

    #[test]
    fn encodes_go_multiple_numeric_delete_update_payload() {
        let value = AttributeValue::N("5".into());
        let actions = vec![
            UpdateAction::Delete {
                attribute: "Color",
                placeholder: ":p",
            },
            UpdateAction::Delete {
                attribute: "Color_2",
                placeholder: ":p",
            },
        ];
        assert_eq!(
            encode_update_actions(&actions, &[&value]).expect("delete payload encodes"),
            hex("8301828315821265436f6c6f728211008315821267436f6c6f725f328211008105")
        );
    }

    #[test]
    fn reuses_update_placeholder_ids_in_first_use_order() {
        let input = UpdateItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("key".into()))
            .update_expression("DELETE Color :p, Color_2 :q, Color_3 :p")
            .expression_attribute_values(":p", AttributeValue::N("5".into()))
            .expression_attribute_values(":q", AttributeValue::N("7".into()))
            .build()
            .expect("UpdateItem input is complete");
        assert_eq!(
            encode_update_item(&input, &key_schema()).unwrap(),
            hex(
                "011a54f89c0f455461626c65436b6579bf0858318301838315821265436f6c6f728211008315821267436f6c6f725f328211018315821267436f6c6f725f33821100820507ff"
            )
        );
    }

    #[test]
    fn reuses_update_placeholder_ids_across_action_sections() {
        let input = UpdateItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("key".into()))
            .update_expression("SET count = :v ADD total :v")
            .expression_attribute_values(":v", AttributeValue::N("5".into()))
            .build()
            .expect("UpdateItem input is complete");
        assert_eq!(
            encode_update_item(&input, &key_schema()).unwrap(),
            hex(
                "011a54f89c0f455461626c65436b6579bf08581f8301828313821265636f756e748211008314821265746f74616c8211008105ff"
            )
        );
    }

    #[test]
    fn rejects_values_for_remove_update_item() {
        let input = UpdateItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("key".into()))
            .update_expression("REMOVE status")
            .expression_attribute_values(":unused", AttributeValue::S("value".into()))
            .build()
            .expect("UpdateItem input is complete");
        assert_eq!(
            encode_update_item(&input, &key_schema()),
            Err(RequestError::UnsupportedExpression(
                "UpdateItem unused expression attribute value"
            ))
        );
    }

    #[test]
    fn encodes_multi_action_update_item_expression() {
        let input = UpdateItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("key".into()))
            .update_expression("SET status = :status, count = :count")
            .expression_attribute_values(":status", AttributeValue::S("open".into()))
            .expression_attribute_values(":count", AttributeValue::N("1".into()))
            .build()
            .expect("UpdateItem input is complete");
        assert_eq!(
            encode_update_item(&input, &key_schema()),
            Ok(hex(
                "011a54f89c0f455461626c65436b6579bf08582583018283138212667374617475738211008313821265636f756e7482110182646f70656e01ff"
            ))
        );
    }

    #[test]
    fn parses_bounded_update_action_model() {
        let add = parse_update_action("ADD count :increment").expect("ADD parses");
        assert!(matches!(
            add,
            UpdateAction::Add {
                attribute: "count",
                placeholder: ":increment"
            }
        ));
        assert_eq!(add.placeholder(), Some(":increment"));

        let append =
            parse_update_action("SET items = list_append(items, :values)").expect("SET parses");
        assert!(matches!(
            append,
            UpdateAction::SetListAppend {
                attribute: "items",
                placeholder: ":values"
            }
        ));
        assert_eq!(append.placeholder(), Some(":values"));

        let remove = parse_update_action("REMOVE status").expect("REMOVE parses");
        assert_eq!(remove.placeholder(), None);
    }

    #[test]
    fn parses_update_action_sections_in_reference_order() {
        let actions = parse_update_actions(
            "SET status = :status, count = count + :delta REMOVE obsolete ADD score :score",
        )
        .expect("mixed sections parse");
        assert_eq!(actions.len(), 4);
        assert!(matches!(actions[0], UpdateAction::SetValue { .. }));
        assert!(matches!(actions[1], UpdateAction::SetArithmetic { .. }));
        assert!(matches!(actions[2], UpdateAction::Remove { .. }));
        assert!(matches!(actions[3], UpdateAction::Add { .. }));
    }

    #[test]
    fn rejects_duplicate_or_out_of_order_update_sections() {
        for expression in [
            "SET status = :status SET count = :count",
            "REMOVE obsolete SET status = :status",
            "ADD score :score REMOVE obsolete",
        ] {
            assert!(matches!(
                parse_update_actions(expression),
                Err(RequestError::UnsupportedExpression(
                    "UpdateExpression section ordering"
                ))
            ));
        }
    }

    #[test]
    fn encodes_update_action_model_without_request_framing() {
        let value = AttributeValue::N("5".into());
        let add = parse_update_action("ADD count :increment").expect("ADD parses");
        assert_eq!(
            encode_update_action(add, Some(&value)).expect("ADD encodes"),
            hex("8301818314821265636f756e748211008105")
        );

        let arithmetic =
            parse_update_action("SET Price = Price - :delta").expect("arithmetic SET parses");
        assert_eq!(
            encode_update_action(arithmetic, Some(&value)).expect("arithmetic SET encodes"),
            hex("8301818313821265507269636583181a82126550726963658211008105")
        );

        let remove = parse_update_action("REMOVE status").expect("REMOVE parses");
        assert_eq!(
            encode_update_action(remove, None).expect("REMOVE encodes"),
            hex("830181821682126673746174757380")
        );
    }

    #[test]
    fn encodes_update_document_paths_and_list_indexes() {
        let value = AttributeValue::S("open".into());
        let nested = parse_update_actions("SET profile.email = :email").expect("nested SET parses");
        assert_eq!(
            encode_update_actions(&nested, &[&value]).expect("nested SET encodes"),
            hex("830181831383126770726f66696c6565656d61696c82110081646f70656e")
        );

        let indexed =
            parse_update_actions("REMOVE items[1].status").expect("indexed REMOVE parses");
        assert_eq!(
            encode_update_actions(&indexed, &[]).expect("indexed REMOVE encodes"),
            hex("83018182168412656974656d73d90cfc016673746174757380")
        );

        let spaced_indexed =
            parse_update_actions("REMOVE items [ 1 ] . status").expect("spaced REMOVE parses");
        assert_eq!(
            encode_update_actions(&spaced_indexed, &[]).expect("spaced REMOVE encodes"),
            hex("83018182168412656974656d73d90cfc016673746174757380")
        );

        for expression in ["REMOVE items[+1]", "REMOVE items[-1]", "REMOVE items[one]"] {
            assert!(
                parse_update_actions(expression).is_err(),
                "update expression {expression:?} should reject a non-digit index"
            );
        }

        let oversized = parse_update_actions("REMOVE items[9223372036854775808]")
            .expect("Go-compatible oversized index parsing");
        assert_eq!(
            encode_update_actions(&oversized, &[]).expect("oversized index encodes"),
            hex("83018182168312656974656d73d90cfc0080")
        );
    }

    #[test]
    fn encodes_document_paths_in_filter_conditions() {
        let values = HashMap::from([(":email".into(), AttributeValue::S("open".into()))]);
        let encoded = encode_filter_expression(Some("profile.email = :email"), None, Some(&values))
            .expect("filter encodes")
            .expect("filter is present");
        assert_eq!(
            encoded,
            hex("8301830083126770726f66696c6565656d61696c82110081646f70656e")
        );

        let spaced =
            encode_filter_expression(Some("profile . email = :email"), None, Some(&values))
                .expect("spaced filter encodes")
                .expect("spaced filter is present");
        assert_eq!(spaced, encoded);

        let tabbed = encode_filter_expression(
            Some("profile.email = :email\tAND\nprofile.email = :email"),
            None,
            Some(&values),
        )
        .expect("tabbed filter encodes")
        .expect("tabbed filter is present");
        let compact = encode_filter_expression(
            Some("profile.email = :email AND profile.email = :email"),
            None,
            Some(&values),
        )
        .expect("compact filter encodes")
        .expect("compact filter is present");
        assert_eq!(tabbed, compact);

        let negated_comparison = encode_filter_expression(Some("not (a1 <> a2)"), None, None)
            .expect("negated comparison encodes")
            .expect("negated comparison is present");
        assert_eq!(
            negated_comparison,
            hex("8301820883018212626131821262613280")
        );

        let attribute_comparison = encode_filter_expression(Some("a1 = a2"), None, None)
            .expect("attribute comparison encodes")
            .expect("attribute comparison is present");
        assert_eq!(attribute_comparison, hex("830183008212626131821262613280"));

        let placeholder_comparison = encode_filter_expression(
            Some("a1 <> :v1"),
            None,
            Some(&HashMap::from([(
                ":v1".into(),
                AttributeValue::N("5".into()),
            )])),
        )
        .expect("placeholder comparison encodes")
        .expect("placeholder comparison is present");
        assert_eq!(placeholder_comparison, hex("8301830182126261318211008105"));

        let compound_comparison = encode_filter_expression(
            Some("a1 < :v1 AND a2 >= :v2"),
            None,
            Some(&HashMap::from([
                (":v1".into(), AttributeValue::N("5".into())),
                (":v2".into(), AttributeValue::N("10".into())),
            ])),
        )
        .expect("compound comparison encodes")
        .expect("compound comparison is present");
        assert_eq!(
            compound_comparison,
            hex("83018306830282126261318211008303821262613282110182050a")
        );

        let disjunction = encode_filter_expression(
            Some("a1 > :v1 OR a2 <= :v2"),
            None,
            Some(&HashMap::from([
                (":v1".into(), AttributeValue::N("5".into())),
                (":v2".into(), AttributeValue::N("10".into())),
            ])),
        )
        .expect("disjunction encodes")
        .expect("disjunction is present");
        assert_eq!(
            disjunction,
            hex("83018307830482126261318211008305821262613282110182050a")
        );
    }

    #[test]
    fn encodes_document_paths_in_filter_functions() {
        let exists = encode_filter_expression(Some("attribute_exists(profile.email)"), None, None)
            .expect("exists filter encodes")
            .expect("exists filter is present");
        assert_eq!(exists, hex("8301820b83126770726f66696c6565656d61696c80"));

        let aliased_exists = encode_filter_expression(
            Some("attribute_not_exists(#a.k1)"),
            Some(&HashMap::from([("#a".into(), "a1".into())])),
            None,
        )
        .expect("aliased not-exists filter encodes")
        .expect("aliased not-exists filter is present");
        assert_eq!(aliased_exists, hex("8301820c8312626131626b3180"));

        let values = HashMap::from([(":kind".into(), AttributeValue::S("open".into()))]);
        let contains = encode_filter_expression(
            Some("contains(items[0].status, :kind)"),
            None,
            Some(&values),
        )
        .expect("contains filter encodes")
        .expect("contains filter is present");
        assert_eq!(
            contains,
            hex("8301830f8412656974656d73d90cfc006673746174757382110081646f70656e")
        );

        let numeric_values = HashMap::from([(":v".into(), AttributeValue::N("5".into()))]);
        let numeric_contains =
            encode_filter_expression(Some("CONTAINS(a, :v)"), None, Some(&numeric_values))
                .expect("numeric contains filter encodes")
                .expect("numeric contains filter is present");
        assert_eq!(numeric_contains, hex("8301830f821261618211008105"));

        let spaced_contains = encode_filter_expression(
            Some("contains( items [ 0 ] . status , :kind )"),
            None,
            Some(&values),
        )
        .expect("spaced contains filter encodes")
        .expect("spaced contains filter is present");
        assert_eq!(spaced_contains, contains);
    }

    #[test]
    fn encodes_document_paths_in_in_and_size_filters() {
        let values = HashMap::from([
            (":first".into(), AttributeValue::S("open".into())),
            (":second".into(), AttributeValue::S("closed".into())),
        ]);
        let in_filter = encode_filter_expression(
            Some("items[0].status IN (:first, :second)"),
            None,
            Some(&values),
        )
        .expect("IN filter encodes")
        .expect("IN filter is present");
        assert_eq!(
            in_filter,
            hex(
                "8301830a8412656974656d73d90cfc00667374617475738282110082110182646f70656e66636c6f736564"
            )
        );

        let literal_in = encode_filter_expression(Some("a IN (b,c,d)"), None, None)
            .expect("literal IN filter encodes")
            .expect("literal IN filter is present");
        assert_eq!(
            literal_in,
            hex("8301830a821261618382126162821261638212616480")
        );

        let mixed_values = HashMap::from([(":value".into(), AttributeValue::S("open".into()))]);
        let mixed_in =
            encode_filter_expression(Some("a IN (b, :value, c)"), None, Some(&mixed_values))
                .expect("mixed IN filter encodes")
                .expect("mixed IN filter is present");
        assert_eq!(
            mixed_in,
            hex("8301830a8212616183821261628211008212616381646f70656e")
        );

        let size_filter =
            encode_filter_expression(Some("payload_size > size(payload[0])"), None, None)
                .expect("size filter encodes")
                .expect("size filter is present");
        assert_eq!(
            size_filter,
            hex("8301830482126c7061796c6f61645f73697a6582108312677061796c6f6164d90cfc0080")
        );
    }

    #[test]
    fn rejects_document_paths_in_key_conditions() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("profile.email = :email")
            .expression_attribute_values(":email", AttributeValue::S("open".into()))
            .build()
            .expect("query input is complete");
        assert_eq!(
            encode_query_key_condition(&input),
            Err(RequestError::UnsupportedExpression(
                "KeyConditionExpression document path"
            ))
        );

        let malformed_index = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("a < b[-25]")
            .expression_attribute_values(":value", AttributeValue::N("1".into()))
            .build()
            .expect("query input is complete");
        assert_eq!(
            encode_query_key_condition(&malformed_index),
            Err(RequestError::UnsupportedExpression(
                "KeyConditionExpression document path"
            ))
        );
    }

    #[test]
    fn allows_nested_function_commas_in_update_item_expression() {
        let input = UpdateItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("key".into()))
            .update_expression("SET items = list_append(items, :values)")
            .expression_attribute_values(
                ":values",
                AttributeValue::L(vec![AttributeValue::S("next".into())]),
            )
            .build()
            .expect("UpdateItem input is complete");
        assert!(encode_update_item(&input, &key_schema()).is_ok());
    }

    #[test]
    fn encodes_default_and_optional_delete_item_streams() {
        let input = DeleteItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("val".into()))
            .build()
            .expect("DeleteItem input is complete");
        assert_eq!(
            encode_delete_item(&input, &key_schema()).unwrap(),
            hex("011a3c696221455461626c654376616cbfff")
        );

        let optional = DeleteItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("val".into()))
            .return_values(ReturnValue::AllOld)
            .build()
            .expect("DeleteItem input is complete");
        assert_eq!(
            encode_delete_item(&optional, &key_schema()).unwrap(),
            hex("011a3c696221455461626c654376616cbf0702ff")
        );

        let conditional = DeleteItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("val".into()))
            .condition_expression("#status = :status")
            .expression_attribute_names("#status", "status")
            .expression_attribute_values(":status", AttributeValue::S("open".into()))
            .build()
            .expect("DeleteItem input is complete");
        assert_eq!(
            encode_delete_item(&conditional, &key_schema()).unwrap(),
            hex(
                "011a3c696221455461626c654376616cbf04568301830082126673746174757382110081646f70656eff"
            )
        );
    }

    #[test]
    fn rejects_unported_condition_failure_return_values() {
        let put = PutItemInput::builder()
            .table_name("Table")
            .item("pk", AttributeValue::S("val".into()))
            .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
            .build()
            .expect("PutItem input is complete");
        let delete = DeleteItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("val".into()))
            .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
            .build()
            .expect("DeleteItem input is complete");
        assert_eq!(
            encode_put_item(&put, &key_schema(), 0).unwrap_err(),
            RequestError::UnsupportedExpression("ConditionExpression")
        );
        assert_eq!(
            encode_delete_item(&delete, &key_schema()).unwrap_err(),
            RequestError::UnsupportedExpression("ConditionExpression")
        );
    }

    #[test]
    fn encodes_default_scan_stream_and_rejects_unported_options() {
        let input = ScanInput::builder()
            .table_name("Table")
            .build()
            .expect("Scan input is complete");
        assert_eq!(
            encode_scan(&input, &key_schema()).unwrap(),
            hex("013a6fc8309b455461626c65bfff")
        );

        let paginated = ScanInput::builder()
            .table_name("Table")
            .exclusive_start_key("pk", AttributeValue::S("val".into()))
            .build()
            .expect("Scan input is complete");
        assert_eq!(
            encode_scan(&paginated, &key_schema()).unwrap(),
            hex("013a6fc8309b455461626c65bf094376616cff")
        );

        let limited = ScanInput::builder()
            .table_name("Table")
            .consistent_read(true)
            .return_consumed_capacity(ReturnConsumedCapacity::Indexes)
            .limit(1)
            .build()
            .expect("Scan input is complete");
        assert_eq!(
            encode_scan(&limited, &key_schema()).unwrap(),
            hex("013a6fc8309b455461626c65bf030202010d01ff")
        );

        let count = ScanInput::builder()
            .table_name("Table")
            .select(Select::Count)
            .build()
            .expect("Scan input is complete");
        assert_eq!(
            encode_scan(&count, &key_schema()).unwrap(),
            hex("013a6fc8309b455461626c65bf0f03ff")
        );

        let filtered = ScanInput::builder()
            .table_name("Table")
            .filter_expression("#status = :status")
            .expression_attribute_names("#status", "status")
            .expression_attribute_values(":status", AttributeValue::S("open".into()))
            .build()
            .expect("Scan input is complete");
        assert_eq!(
            encode_scan(&filtered, &key_schema()).unwrap(),
            hex("013a6fc8309b455461626c65bf0a568301830082126673746174757382110081646f70656eff")
        );

        let projected = ScanInput::builder()
            .table_name("Table")
            .projection_expression("#partition, #status")
            .expression_attribute_names("#partition", "pk")
            .expression_attribute_names("#status", "status")
            .build()
            .expect("Scan input is complete");
        assert_eq!(
            encode_scan(&projected, &key_schema()).unwrap(),
            hex("013a6fc8309b455461626c65bf0051820182821262706b821266737461747573ff")
        );

        let nested_projection = ScanInput::builder()
            .table_name("Table")
            .projection_expression("profile.email, status")
            .build()
            .expect("Scan input is complete");
        assert_eq!(
            encode_scan(&nested_projection, &key_schema()).unwrap(),
            hex(
                "013a6fc8309b455461626c65bf00581c82018283126770726f66696c6565656d61696c821266737461747573ff"
            )
        );

        let indexed_projection = ScanInput::builder()
            .table_name("Table")
            .projection_expression("items[0].status")
            .build()
            .expect("Scan input is complete");
        assert_eq!(
            encode_scan(&indexed_projection, &key_schema()).unwrap(),
            hex("013a6fc8309b455461626c65bf00568201818412656974656d73d90cfc0066737461747573ff")
        );

        let spaced_indexed_projection = ScanInput::builder()
            .table_name("Table")
            .projection_expression("items [ 0 ]. status")
            .build()
            .expect("Scan input is complete");
        assert_eq!(
            encode_scan(&spaced_indexed_projection, &key_schema()).unwrap(),
            hex("013a6fc8309b455461626c65bf00568201818412656974656d73d90cfc0066737461747573ff")
        );

        let oversized_index_projection = ScanInput::builder()
            .table_name("Table")
            .projection_expression("items[9223372036854775808].status")
            .build()
            .expect("Scan input is complete");
        assert_eq!(
            encode_scan(&oversized_index_projection, &key_schema()).unwrap(),
            hex("013a6fc8309b455461626c65bf00568201818412656974656d73d90cfc0066737461747573ff")
        );
    }

    #[test]
    fn rejects_malformed_projection_document_paths() {
        for projection in [
            "items[-1]",
            "items[+1]",
            "items[",
            "items[1",
            "items[one]",
            "items[]",
        ] {
            let input = ScanInput::builder()
                .table_name("Table")
                .projection_expression(projection)
                .build()
                .expect("Scan input is complete");

            assert_eq!(
                encode_scan(&input, &key_schema()).unwrap_err(),
                RequestError::UnsupportedExpression(
                    "ProjectionExpression (only non-negative list indexes are supported)"
                ),
                "projection {projection:?} should be rejected"
            );
        }
        for projection in ["items..status", "items.,status", ",items"] {
            let input = ScanInput::builder()
                .table_name("Table")
                .projection_expression(projection)
                .build()
                .expect("Scan input is complete");

            assert_eq!(
                encode_scan(&input, &key_schema()).unwrap_err(),
                RequestError::UnsupportedExpression(
                    "ProjectionExpression (only attribute document paths are supported)"
                ),
                "projection {projection:?} should be rejected"
            );
        }
    }

    #[test]
    fn preserves_dots_inside_projection_attribute_aliases() {
        let input = ScanInput::builder()
            .table_name("Table")
            .projection_expression("#a[1].#b")
            .expression_attribute_names("#a", "with.dot")
            .expression_attribute_names("#b", "sub.field")
            .build()
            .expect("Scan input is complete");

        let encoded = encode_scan(&input, &key_schema()).expect("valid aliased projection");
        assert!(
            encoded
                .windows("with.dot".len())
                .any(|window| window == b"with.dot")
        );
        assert!(
            encoded
                .windows("sub.field".len())
                .any(|window| window == b"sub.field")
        );
    }

    #[test]
    fn encodes_partition_key_equality_query_stream() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :value")
            .expression_attribute_values(":value", AttributeValue::S("val".into()))
            .consistent_read(true)
            .return_consumed_capacity(ReturnConsumedCapacity::Indexes)
            .limit(1)
            .scan_index_forward(false)
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex("013a3781c2ae455461626c655183018300821262706b821100816376616cbf030202010d010e00ff")
        );
    }

    #[test]
    fn encodes_query_projection_stream_with_shared_aliases() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("#pk = :key")
            .projection_expression("#pk, #profile.email")
            .expression_attribute_names("#pk", "pk")
            .expression_attribute_names("#profile", "profile")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c654f83018300821262706b82110081616bbf005818820182821262706b83126770726f66696c6565656d61696cff"
            )
        );
    }

    #[test]
    fn encodes_go_projection_vectors() {
        let cases = [
            ("a1", None, "8201818212626131"),
            ("a1,a2", None, "82018282126261318212626132"),
            ("a1,a2.k1", None, "82018282126261318312626132626b31"),
            ("a1,a4[0]", None, "82018282126261318312626134d90cfc00"),
            (
                "a1,a3.#s1",
                Some(HashMap::from([("#s1".into(), "s1".into())])),
                "82018282126261318312626133627331",
            ),
        ];

        for (expression, names, expected) in cases {
            let encoded = encode_projection_expression(expression.into(), names.as_ref())
                .expect("Go projection vector encodes")
                .expect("projection is present");
            assert_eq!(encoded, hex(expected), "projection {expression:?}");
        }
    }

    #[test]
    fn encodes_count_query_stream() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :value")
            .expression_attribute_values(":value", AttributeValue::S("val".into()))
            .select(Select::Count)
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex("013a3781c2ae455461626c655183018300821262706b821100816376616cbf0f03ff")
        );
    }

    #[test]
    fn encodes_single_equality_query_filter_stream() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("status = :status")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .expression_attribute_values(":status", AttributeValue::S("open".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c654f83018300821262706b82110081616bbf0a568301830082126673746174757382110081646f70656eff"
            )
        );
    }

    #[test]
    fn encodes_query_filter_comparison_stream() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("score >= :score")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .expression_attribute_values(":score", AttributeValue::N("5".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c654f83018300821262706b82110081616bbf0a518301830382126573636f72658211008105ff"
            )
        );
    }

    #[test]
    fn encodes_query_begins_with_filter_stream() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("begins_with(status, :prefix)")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .expression_attribute_values(":prefix", AttributeValue::S("open".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c654f83018300821262706b82110081616bbf0a568301830e82126673746174757382110081646f70656eff"
            )
        );
    }

    #[test]
    fn encodes_go_case_insensitive_begins_with_filter_vector() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("begins_With(status, :prefix)")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .expression_attribute_values(":prefix", AttributeValue::S("open".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c654f83018300821262706b82110081616bbf0a568301830e82126673746174757382110081646f70656eff"
            )
        );
    }

    #[test]
    fn encodes_go_literal_begins_with_filter_vector() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("begins_With(a, substr)")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c654f83018300821262706b82110081616bbf0a528301830e8212616182126673756273747280ff"
            )
        );
    }

    #[test]
    fn encodes_go_literal_begins_with_function_vector() {
        let encoded = encode_filter_expression(Some("begins_With(a, substr)"), None, None)
            .expect("literal begins_with filter encodes")
            .expect("literal begins_with filter is present");
        assert_eq!(encoded, hex("8301830e8212616182126673756273747280"));
    }

    #[test]
    fn encodes_query_between_filter_stream() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("score BETWEEN :low AND :high")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .expression_attribute_values(":low", AttributeValue::N("1".into()))
            .expression_attribute_values(":high", AttributeValue::N("10".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c654f83018300821262706b82110081616bbf0a558301840982126573636f726582110082110182010aff"
            )
        );
    }

    #[test]
    fn encodes_go_between_filter_payload() {
        let values = HashMap::from([
            (":v1".into(), AttributeValue::N("5".into())),
            (":v2".into(), AttributeValue::N("10".into())),
        ]);
        let encoded = encode_filter_expression(Some("a between :v1 and :v2"), None, Some(&values))
            .expect("BETWEEN filter encodes")
            .expect("BETWEEN filter is present");
        assert_eq!(encoded, hex("830184098212616182110082110182050a"));
    }

    #[test]
    fn encodes_two_condition_query_filter_stream() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("status = :status AND score >= :score")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .expression_attribute_values(":status", AttributeValue::S("open".into()))
            .expression_attribute_values(":score", AttributeValue::N("5".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c654f83018300821262706b82110081616bbf0a5826830183068300821266737461747573821100830382126573636f726582110182646f70656e05ff"
            )
        );
    }

    #[test]
    fn encodes_or_query_filter_stream() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("status = :status OR score >= :score")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .expression_attribute_values(":status", AttributeValue::S("open".into()))
            .expression_attribute_values(":score", AttributeValue::N("5".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c654f83018300821262706b82110081616bbf0a5826830183078300821266737461747573821100830382126573636f726582110182646f70656e05ff"
            )
        );
    }

    #[test]
    fn encodes_not_equal_query_filter_stream() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("status <> :status")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .expression_attribute_values(":status", AttributeValue::S("closed".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c654f83018300821262706b82110081616bbf0a5818830183018212667374617475738211008166636c6f736564ff"
            )
        );
    }

    #[test]
    fn rejects_not_equal_query_sort_key_condition() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key AND sk <> :sort")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .expression_attribute_values(":sort", AttributeValue::S("s".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap_err(),
            RequestError::UnsupportedExpression(
                "KeyConditionExpression (the sort key does not support this comparison)"
            )
        );
    }

    #[test]
    fn encodes_negated_query_filter_stream() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("NOT (status = :status)")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .expression_attribute_values(":status", AttributeValue::S("closed".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c654f83018300821262706b82110081616bbf0a581a8301820883008212667374617475738211008166636c6f736564ff"
            )
        );

        let spaced = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("not\t(\nstatus = :status)")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .expression_attribute_values(":status", AttributeValue::S("closed".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&spaced, &key_schema()).unwrap(),
            encode_query(&input, &key_schema()).unwrap()
        );
    }

    #[test]
    fn encodes_attribute_exists_query_filter_stream() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("attribute_exists(status)")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c654f83018300821262706b82110081616bbf0a4e8301820b82126673746174757380ff"
            )
        );
    }

    #[test]
    fn encodes_go_attribute_exists_function_vector() {
        let encoded = encode_filter_expression(Some("attribute_exists(a)"), None, None)
            .expect("attribute_exists filter encodes")
            .expect("attribute_exists filter is present");
        assert_eq!(encoded, hex("8301820b8212616180"));
    }

    #[test]
    fn encodes_go_aliased_attribute_not_exists_function_vector() {
        let names = HashMap::from([("#a".into(), "a1".into())]);
        let encoded =
            encode_filter_expression(Some("attribute_not_exists(#a.k1)"), Some(&names), None)
                .expect("attribute_not_exists filter encodes")
                .expect("attribute_not_exists filter is present");
        assert_eq!(encoded, hex("8301820c8312626131626b3180"));
    }

    #[test]
    fn encodes_go_case_insensitive_attribute_exists_filter_vector() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("AtTrIbUtE_ExIsTs(status)")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c654f83018300821262706b82110081616bbf0a4e8301820b82126673746174757380ff"
            )
        );
    }

    #[test]
    fn encodes_attribute_not_exists_query_filter_stream() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("attribute_not_exists(status)")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c654f83018300821262706b82110081616bbf0a4e8301820c82126673746174757380ff"
            )
        );
    }

    #[test]
    fn encodes_go_case_insensitive_attribute_not_exists_filter_vector() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("AtTrIbUtE_NoT_ExIsTs(status)")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c654f83018300821262706b82110081616bbf0a4e8301820c82126673746174757380ff"
            )
        );
    }

    #[test]
    fn encodes_negated_attribute_exists_query_filter_stream() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("NOT (attribute_exists(status))")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c654f83018300821262706b82110081616bbf0a5083018208820b82126673746174757380ff"
            )
        );
    }

    #[test]
    fn encodes_unparenthesized_negated_contains_query_filter_stream() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("NOT contains(tags, :tag)")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .expression_attribute_values(":tag", AttributeValue::S("urgent".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c654f83018300821262706b82110081616bbf0a581883018208830f821264746167738211008166757267656e74ff"
            )
        );
    }

    #[test]
    fn encodes_attribute_type_query_filter_stream() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("attribute_type(status, S)")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c654f83018300821262706b82110081616bbf0a528301830d8212667374617475738212615380ff"
            )
        );
    }

    #[test]
    fn encodes_go_attribute_type_function_vector() {
        let encoded = encode_filter_expression(Some("Attribute_type(a, S)"), None, None)
            .expect("attribute_type filter encodes")
            .expect("attribute_type filter is present");
        assert_eq!(encoded, hex("8301830d821261618212615380"));
    }

    #[test]
    fn encodes_go_case_insensitive_attribute_type_query_filter_vector() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("AtTrIbUtE_TyPe(status, S)")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c654f83018300821262706b82110081616bbf0a528301830d8212667374617475738212615380ff"
            )
        );
    }

    #[test]
    fn encodes_attribute_type_placeholder_query_filter_stream() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("attribute_type(status, :type)")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .expression_attribute_values(":type", AttributeValue::S("S".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c654f83018300821262706b82110081616bbf0a538301830d821266737461747573821100816153ff"
            )
        );
    }

    #[test]
    fn encodes_contains_query_filter_stream() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("contains(tags, :tag)")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .expression_attribute_values(":tag", AttributeValue::S("urgent".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c654f83018300821262706b82110081616bbf0a568301830f821264746167738211008166757267656e74ff"
            )
        );
    }

    #[test]
    fn encodes_go_contains_function_vector() {
        let values = HashMap::from([(":v".into(), AttributeValue::N("5".into()))]);
        let encoded = encode_filter_expression(Some("CONTAINS(a, :v)"), None, Some(&values))
            .expect("contains filter encodes")
            .expect("contains filter is present");
        assert_eq!(encoded, hex("8301830f821261618211008105"));
    }

    #[test]
    fn encodes_go_case_insensitive_contains_filter_vector() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("CoNtAiNs(tags, :tag)")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .expression_attribute_values(":tag", AttributeValue::S("urgent".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c654f83018300821262706b82110081616bbf0a568301830f821264746167738211008166757267656e74ff"
            )
        );
    }

    #[test]
    fn encodes_in_query_filter_stream() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("status IN (:open, :pending)")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .expression_attribute_values(":open", AttributeValue::S("open".into()))
            .expression_attribute_values(":pending", AttributeValue::S("pending".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c654f83018300821262706b82110081616bbf0a58228301830a8212667374617475738282110082110182646f70656e6770656e64696e67ff"
            )
        );
    }

    #[test]
    fn encodes_between_and_comparison_query_filter_stream() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("score BETWEEN :low AND :high AND status = :status")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .expression_attribute_values(":low", AttributeValue::N("1".into()))
            .expression_attribute_values(":high", AttributeValue::N("10".into()))
            .expression_attribute_values(":status", AttributeValue::S("open".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c654f83018300821262706b82110081616bbf0a582a83018306840982126573636f7265821100821101830082126673746174757382110283010a646f70656eff"
            )
        );
    }

    #[test]
    fn accepts_case_insensitive_query_filter_keywords_and_functions() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("CONTAINS(tags, :tag) AnD attribute_Exists(active)")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .expression_attribute_values(":tag", AttributeValue::S("urgent".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c654f83018300821262706b82110081616bbf0a582383018306830f82126474616773821100820b8212666163746976658166757267656e74ff"
            )
        );
    }

    #[test]
    fn rejects_unicode_expression_whitespace() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("status\u{00a0}=\u{00a0}:status")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .expression_attribute_values(":status", AttributeValue::S("open".into()))
            .build()
            .expect("Query input is complete");

        assert!(encode_query(&input, &key_schema()).is_err());

        let form_feed = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("status\u{000c}=\u{000c}:status")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .expression_attribute_values(":status", AttributeValue::S("open".into()))
            .build()
            .expect("Query input is complete");
        assert!(encode_query(&form_feed, &key_schema()).is_err());
    }

    #[test]
    fn rejects_unicode_expression_whitespace_across_expression_surfaces() {
        assert!(encode_projection_expression(Some("profile\u{00a0}.email"), None).is_err());

        let key_condition = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk\u{00a0}=\u{00a0}:key")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .build()
            .expect("Query input is complete");
        assert!(encode_query_key_condition(&key_condition).is_err());

        let update = UpdateItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("k".into()))
            .update_expression("SET\u{00a0}status = :status")
            .expression_attribute_values(":status", AttributeValue::S("open".into()))
            .build()
            .expect("UpdateItem input is complete");
        assert!(encode_update_item(&update, &key_schema()).is_err());
    }

    #[test]
    fn rejects_missing_filter_placeholders_and_invalid_nested_functions() {
        let missing = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("status = :missing")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&missing, &key_schema()).unwrap_err(),
            RequestError::UnsupportedExpression("FilterExpression placeholder")
        );

        let nested = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("status = size(attribute_exists(active))")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&nested, &key_schema()).unwrap_err(),
            RequestError::UnsupportedExpression(
                "FilterExpression (only up to two conditions joined by `AND` or `OR` are supported)"
            )
        );
    }

    #[test]
    fn rejects_go_expression_error_corpus_cases() {
        assert_eq!(
            encode_projection_expression(Some("(a1)"), None).unwrap_err(),
            RequestError::UnsupportedExpression(
                "ProjectionExpression (only attribute document paths are supported)"
            )
        );
        assert_eq!(
            encode_projection_expression(Some("#a"), None).unwrap_err(),
            RequestError::UnsupportedExpression("ProjectionExpression attribute-name alias")
        );
        let unused_alias = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .projection_expression("a")
            .expression_attribute_names("#unused", "unused")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&unused_alias, &key_schema()).unwrap_err(),
            RequestError::UnsupportedExpression("Query unused attribute-name alias")
        );

        let missing = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("a < :value")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&missing, &key_schema()).unwrap_err(),
            RequestError::UnsupportedExpression("FilterExpression placeholder")
        );

        let unused = PutItemInput::builder()
            .table_name("Table")
            .item("pk", AttributeValue::S("k".into()))
            .condition_expression("attribute_exists(pk)")
            .expression_attribute_values(":unused", AttributeValue::S("value".into()))
            .build()
            .expect("PutItem input is complete");
        assert_eq!(
            super::reject_put_options(&unused).unwrap_err(),
            RequestError::UnsupportedExpression(
                "ConditionExpression unused expression attribute value"
            )
        );

        for condition in [
            "value < if_not_exists(other)",
            "value < size(attribute_exists(other))",
        ] {
            let input = PutItemInput::builder()
                .table_name("Table")
                .item("pk", AttributeValue::S("k".into()))
                .condition_expression(condition)
                .build()
                .expect("PutItem input is complete");
            assert_eq!(
                super::reject_put_options(&input).unwrap_err(),
                RequestError::UnsupportedExpression(
                    "ConditionExpression (only up to two conditions joined by `AND` or `OR` are supported)"
                ),
                "condition {condition:?}"
            );
        }

        let nested = UpdateItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("k".into()))
            .update_expression("SET value = size(other)")
            .build()
            .expect("UpdateItem input is complete");
        assert_eq!(
            encode_update_item(&nested, &key_schema()).unwrap_err(),
            RequestError::UnsupportedExpression("UpdateExpression action")
        );

        let nested_function = UpdateItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("k".into()))
            .update_expression("set value = if_not_exists(list_append(value, other))")
            .build()
            .expect("UpdateItem input is complete");
        assert_eq!(
            encode_update_item(&nested_function, &key_schema()).unwrap_err(),
            RequestError::UnsupportedExpression("UpdateExpression action")
        );
    }

    #[test]
    fn encodes_size_comparison_query_filter_stream() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("payload_size > size(payload)")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c654f83018300821262706b82110081616bbf0a58208301830482126c7061796c6f61645f73697a6582108212677061796c6f616480ff"
            )
        );
    }

    #[test]
    fn encodes_go_size_function_vector() {
        let encoded = encode_filter_expression(Some("a > size(c)"), None, None)
            .expect("size filter encodes")
            .expect("size filter is present");
        assert_eq!(encoded, hex("830183048212616182108212616380"));
    }

    #[test]
    fn encodes_go_case_insensitive_size_filter_vector() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .filter_expression("payload_size > SiZe(payload)")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c654f83018300821262706b82110081616bbf0a58208301830482126c7061796c6f61645f73697a6582108212677061796c6f616480ff"
            )
        );
    }

    #[test]
    fn rejects_unported_select_modes() {
        let scan = ScanInput::builder()
            .table_name("Table")
            .select(Select::SpecificAttributes)
            .build()
            .expect("Scan input is complete");
        assert_eq!(
            encode_scan(&scan, &key_schema()).unwrap_err(),
            RequestError::UnsupportedExpression("Select")
        );
    }

    #[test]
    fn encodes_two_key_equality_query_stream() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :partition AND sk = :sort")
            .expression_attribute_values(":partition", AttributeValue::S("p".into()))
            .expression_attribute_values(":sort", AttributeValue::N("5".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c65581c830183068300821262706b8211008300821262736b82110182617005bfff"
            )
        );
    }

    #[test]
    fn encodes_sort_key_comparison_query_stream() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :partition AND sk >= :sort")
            .expression_attribute_values(":partition", AttributeValue::S("p".into()))
            .expression_attribute_values(":sort", AttributeValue::N("5".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c65581c830183068300821262706b8211008303821262736b82110182617005bfff"
            )
        );
    }

    #[test]
    fn encodes_sort_key_begins_with_query_stream() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :partition AND begins_with(sk, :prefix)")
            .expression_attribute_values(":partition", AttributeValue::S("p".into()))
            .expression_attribute_values(":prefix", AttributeValue::S("prefix".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c655822830183068300821262706b821100830e821262736b82110182617066707265666978bfff"
            )
        );
    }

    #[test]
    fn encodes_sort_key_between_query_stream() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :partition AND sk BETWEEN :lower AND :upper")
            .expression_attribute_values(":partition", AttributeValue::S("p".into()))
            .expression_attribute_values(":lower", AttributeValue::N("1".into()))
            .expression_attribute_values(":upper", AttributeValue::N("10".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c655820830183068300821262706b8211008409821262736b821101821102836170010abfff"
            )
        );
    }

    #[test]
    fn encodes_query_attribute_name_aliases() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("#partition = :partition AND #sort >= :sort")
            .expression_attribute_names("#partition", "pk")
            .expression_attribute_names("#sort", "sk")
            .expression_attribute_values(":partition", AttributeValue::S("p".into()))
            .expression_attribute_values(":sort", AttributeValue::N("5".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c65581c830183068300821262706b8211008303821262736b82110182617005bfff"
            )
        );
    }

    #[test]
    fn rejects_missing_or_unused_query_attribute_name_aliases() {
        let missing = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("#partition = :partition")
            .expression_attribute_values(":partition", AttributeValue::S("p".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&missing, &key_schema()).unwrap_err(),
            RequestError::UnsupportedExpression("KeyConditionExpression attribute-name alias")
        );

        let unused = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :partition")
            .expression_attribute_names("#partition", "pk")
            .expression_attribute_values(":partition", AttributeValue::S("p".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&unused, &key_schema()).unwrap_err(),
            RequestError::UnsupportedExpression("Query unused attribute-name alias")
        );
    }

    #[test]
    fn encodes_go_key_condition_benchmark_vector() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :v1 AND hk < :v2")
            .expression_attribute_values(":v1", AttributeValue::S("pkval".into()))
            .expression_attribute_values(":v2", AttributeValue::N("5".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query_key_condition(&input).expect("key condition encodes"),
            hex("830183068300821262706b8211008302821262686b8211018265706b76616c05")
        );
    }

    #[test]
    fn encodes_query_aliases_across_key_condition_and_filter() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("#pk = :key")
            .filter_expression("#status = :status")
            .expression_attribute_names("#pk", "pk")
            .expression_attribute_names("#status", "status")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .expression_attribute_values(":status", AttributeValue::S("open".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap(),
            hex(
                "013a3781c2ae455461626c654f83018300821262706b82110081616bbf0a568301830082126673746174757382110081646f70656eff"
            )
        );
    }

    #[test]
    fn rejects_unused_query_expression_attribute_values() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :key")
            .expression_attribute_values(":key", AttributeValue::S("k".into()))
            .expression_attribute_values(":unused", AttributeValue::S("u".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap_err(),
            RequestError::UnsupportedExpression("Query unused expression attribute value")
        );
    }

    #[test]
    fn rejects_a_non_equality_partition_key_query_condition() {
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk > :partition")
            .expression_attribute_values(":partition", AttributeValue::S("p".into()))
            .build()
            .expect("Query input is complete");
        assert_eq!(
            encode_query(&input, &key_schema()).unwrap_err(),
            RequestError::UnsupportedExpression(
                "KeyConditionExpression (the first term must use `=`)"
            )
        );
    }

    #[test]
    fn rejects_missing_fields_and_legacy_get_item_attributes() {
        let missing_table = GetItemInput::builder()
            .key("pk", AttributeValue::S("val".into()))
            .build()
            .expect("SDK permits missing table names");
        assert_eq!(
            encode_get_item(&missing_table, &key_schema()).unwrap_err(),
            RequestError::MissingRequiredField("TableName")
        );

        let legacy_attributes = GetItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("val".into()))
            .attributes_to_get("pk")
            .build()
            .expect("GetItem input is complete");
        assert_eq!(
            encode_get_item(&legacy_attributes, &key_schema()).unwrap_err(),
            RequestError::UnsupportedExpression("ProjectionExpression")
        );
    }

    #[test]
    fn resolves_request_schema_state_from_the_registry() {
        let registry = SchemaRegistry::default();
        registry
            .insert_key_schema("Table".into(), key_schema())
            .unwrap();
        registry
            .insert_attribute_list(9, vec!["a".into(), "z".into()])
            .unwrap();
        let get = GetItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("val".into()))
            .build()
            .expect("GetItem input is complete");
        assert_eq!(
            encode_get_item_with_registry(&get, &registry).unwrap(),
            hex("011a0fb0cc6a455461626c654376616cbfff")
        );

        let put = PutItemInput::builder()
            .table_name("Table")
            .item("pk", AttributeValue::S("val".into()))
            .item("z", AttributeValue::N("1".into()))
            .item("a", AttributeValue::S("x".into()))
            .build()
            .expect("PutItem input is complete");
        assert_eq!(
            encode_put_item_with_registry(&put, &registry).unwrap(),
            hex("013a7d8e7e56455461626c654376616c4409617801bfff")
        );
        let missing = PutItemInput::builder()
            .table_name("Missing")
            .item("pk", AttributeValue::S("val".into()))
            .build()
            .expect("PutItem input is complete");
        assert_eq!(
            encode_put_item_with_registry(&missing, &registry).unwrap_err(),
            RequestError::Schema(SchemaError::MissingKeySchema("Missing".into()))
        );
    }

    fn hex(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                u8::from_str_radix(std::str::from_utf8(pair).expect("hex bytes are UTF-8"), 16)
                    .expect("fixture is valid hexadecimal")
            })
            .collect()
    }
}
