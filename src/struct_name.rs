use serde::{ser::{self, Impossible, SerializeStruct}, Serialize, Serializer};

pub fn to_struct_name<T: Serialize>(value: &T) -> anyhow::Result<String> {
    let mut serializer = EntityNameSerializer::default();
    value.serialize(&mut serializer)?;
    Ok(serializer.name)
}

#[derive(Default)]
struct EntityNameSerializer {
    name: String,
}

impl Serializer for &mut EntityNameSerializer {
	type Ok = ();
	type Error = std::fmt::Error;
	type SerializeSeq = Impossible<Self::Ok, Self::Error>;
	type SerializeTuple = Impossible<Self::Ok, Self::Error>;
	type SerializeTupleStruct = Impossible<Self::Ok, Self::Error>;
	type SerializeTupleVariant = Impossible<Self::Ok, Self::Error>;
	type SerializeMap = Impossible<Self::Ok, Self::Error>;
	type SerializeStruct = Self;
	type SerializeStructVariant = Impossible<Self::Ok, Self::Error>;
    
    fn serialize_bool(self, v: bool) -> std::result::Result<Self::Ok, Self::Error> {
            todo!()
        }
    
    fn serialize_i8(self, v: i8) -> std::result::Result<Self::Ok, Self::Error> {
            todo!()
        }
    
    fn serialize_i16(self, v: i16) -> std::result::Result<Self::Ok, Self::Error> {
            todo!()
        }
    
    fn serialize_i32(self, v: i32) -> std::result::Result<Self::Ok, Self::Error> {
            todo!()
        }
    
    fn serialize_i64(self, v: i64) -> std::result::Result<Self::Ok, Self::Error> {
            todo!()
        }
    
    fn serialize_u8(self, v: u8) -> std::result::Result<Self::Ok, Self::Error> {
            todo!()
        }
    
    fn serialize_u16(self, v: u16) -> std::result::Result<Self::Ok, Self::Error> {
            todo!()
        }
    
    fn serialize_u32(self, v: u32) -> std::result::Result<Self::Ok, Self::Error> {
            todo!()
        }
    
    fn serialize_u64(self, v: u64) -> std::result::Result<Self::Ok, Self::Error> {
            todo!()
        }
    
    fn serialize_f32(self, v: f32) -> std::result::Result<Self::Ok, Self::Error> {
            todo!()
        }
    
    fn serialize_f64(self, v: f64) -> std::result::Result<Self::Ok, Self::Error> {
            todo!()
        }
    
    fn serialize_char(self, v: char) -> std::result::Result<Self::Ok, Self::Error> {
            todo!()
        }
    
    fn serialize_str(self, v: &str) -> std::result::Result<Self::Ok, Self::Error> {
            todo!()
        }
    
    fn serialize_bytes(self, v: &[u8]) -> std::result::Result<Self::Ok, Self::Error> {
            todo!()
        }
    
    fn serialize_none(self) -> std::result::Result<Self::Ok, Self::Error> {
            todo!()
        }
    
    fn serialize_some<T>(self, value: &T) -> std::result::Result<Self::Ok, Self::Error>
        where
            T: ?Sized + Serialize {
            todo!()
        }
    
    fn serialize_unit(self) -> std::result::Result<Self::Ok, Self::Error> {
            todo!()
        }
    
    fn serialize_unit_struct(self, name: &'static str) -> std::result::Result<Self::Ok, Self::Error> {
            todo!()
        }
    
    fn serialize_unit_variant(
            self,
            name: &'static str,
            variant_index: u32,
            variant: &'static str,
        ) -> std::result::Result<Self::Ok, Self::Error> {
            todo!()
        }
    
    fn serialize_newtype_struct<T>(
            self,
            name: &'static str,
            value: &T,
        ) -> std::result::Result<Self::Ok, Self::Error>
        where
            T: ?Sized + Serialize {
            todo!()
        }
    
    fn serialize_newtype_variant<T>(
            self,
            name: &'static str,
            variant_index: u32,
            variant: &'static str,
            value: &T,
        ) -> std::result::Result<Self::Ok, Self::Error>
        where
            T: ?Sized + Serialize {
            todo!()
        }
    
    fn serialize_seq(self, len: Option<usize>) -> std::result::Result<Self::SerializeSeq, Self::Error> {
            todo!()
        }
    
    fn serialize_tuple(self, len: usize) -> std::result::Result<Self::SerializeTuple, Self::Error> {
            todo!()
        }
    
    fn serialize_tuple_struct(
            self,
            name: &'static str,
            len: usize,
        ) -> std::result::Result<Self::SerializeTupleStruct, Self::Error> {
            todo!()
        }
    
    fn serialize_tuple_variant(
            self,
            name: &'static str,
            variant_index: u32,
            variant: &'static str,
            len: usize,
        ) -> std::result::Result<Self::SerializeTupleVariant, Self::Error> {
            todo!()
        }
    
    fn serialize_map(self, len: Option<usize>) -> std::result::Result<Self::SerializeMap, Self::Error> {
            todo!()
        }
    
    fn serialize_struct(
            self,
            name: &'static str,
            len: usize,
        ) -> std::result::Result<Self::SerializeStruct, Self::Error> {
            self.name = name.to_string();
            Ok(self)
        }
    
    fn serialize_struct_variant(
            self,
            name: &'static str,
            variant_index: u32,
            variant: &'static str,
            len: usize,
        ) -> std::result::Result<Self::SerializeStructVariant, Self::Error> {
            todo!()
        }
}

impl SerializeStruct for &mut EntityNameSerializer {
    type Ok = ();

    type Error = std::fmt::Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> std::result::Result<(), Self::Error>
    where
        T: ?Sized + Serialize {
            
        Ok(())
    }

    fn end(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(())
    }
}
