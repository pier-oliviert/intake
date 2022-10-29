use crate::events::segment::Segment;
use crate::events::Values;
use parquet::file::properties::WriterPropertiesPtr;
use parquet::schema::types::TypePtr;
use std::borrow::Borrow;
use std::hash::{Hash, Hasher};

#[derive(Debug)]
pub(crate) struct Schema {
    segment: Option<Segment>,

    name: String,
    types: TypePtr,
    properties: WriterPropertiesPtr,
}

impl PartialEq for Schema {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Hash for Schema {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl Eq for Schema {}

impl Borrow<String> for Schema {
    fn borrow(&self) -> &String {
        &self.name
    }
}

impl Borrow<str> for Schema {
    fn borrow(&self) -> &str {
        &self.name
    }
}

impl TryFrom<(&str, &Values)> for Schema {
    type Error = crate::events::errors::Error;

    fn try_from(tuple: (&str, &Values)) -> Result<Self, Self::Error> {
        use parquet::file::properties::WriterProperties;
        use parquet::schema::types::Type;

        let mut fields = Vec::new();

        let properties = WriterProperties::builder().build();
        let mut definition = Type::group_type_builder(tuple.0);
        for (key, value) in tuple.1.iter() {
            let field = Type::primitive_type_builder(key, value.into());
            fields.push(TypePtr::new(field.build()?));
        }

        definition = definition.with_fields(&mut fields);

        Ok(Schema {
            name: tuple.0.to_owned(),
            types: TypePtr::new(definition.build()?),
            properties: WriterPropertiesPtr::new(properties),
            segment: None,
        })
    }
}

impl Schema {
    #[inline]
    pub(crate) fn types(&self) -> TypePtr {
        self.types.clone()
    }

    #[inline]
    pub(crate) fn properties(&self) -> WriterPropertiesPtr {
        self.properties.clone()
    }

    pub(crate) fn name(&self) -> String {
        self.name.clone()
    }

    pub(crate) fn segment(&mut self) -> &mut Option<Segment> {
        &mut self.segment
    }
}

#[cfg(test)]
mod tests {
    use super::Schema;
    use crate::events::{Value, Values};
    use parquet::basic::Type as PhysicalType;

    #[test]
    fn generate_schema_from_values() {
        let mut values = Values::new();
        values.insert("a string".into(), Value::String("something".into()));

        let schema = Schema::try_from(("my_index", &values)).unwrap();
        let types = schema.types().get_fields().to_owned();
        assert_eq!(schema.types().is_group(), true);
        assert_eq!(types.len(), 1);
        assert_eq!(
            types.first().unwrap().get_physical_type(),
            PhysicalType::BYTE_ARRAY
        );
    }

    #[test]
    fn each_values_are_represented() {
        let mut values = Values::new();
        values.insert("a string".into(), Value::String("something".into()));
        values.insert("a number".into(), Value::Int64(981));
        values.insert("a float".into(), Value::Float(198.83));

        let schema = Schema::try_from(("my_index", &values)).unwrap();
        let types = schema.types().get_fields().to_owned();

        // This assert might be flakey as the order is an implementation
        // details.
        assert_eq!(types[2].get_physical_type(), PhysicalType::BYTE_ARRAY);
        assert_eq!(types[1].get_physical_type(), PhysicalType::INT64);
        assert_eq!(types[0].get_physical_type(), PhysicalType::FLOAT);
    }
}
