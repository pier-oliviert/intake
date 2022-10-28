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

        let properties = WriterProperties::builder().build();
        let definition = Type::group_type_builder(tuple.0).build()?;

        Ok(Schema {
            name: tuple.0.to_owned(),
            types: TypePtr::new(definition),
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
