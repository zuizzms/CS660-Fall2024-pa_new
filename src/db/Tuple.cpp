#include <cstring>
#include <db/Tuple.hpp>
#include <stdexcept>
#include <unordered_set>

using namespace db;

Tuple::Tuple(const std::vector<field_t> &fields) : fields(fields) {}

type_t Tuple::field_type(size_t i) const {
  const field_t &field = fields.at(i);
  if (std::holds_alternative<int>(field)) {
    return type_t::INT;
  }
  if (std::holds_alternative<double>(field)) {
    return type_t::DOUBLE;
  }
  if (std::holds_alternative<std::string>(field)) {
    return type_t::CHAR;
  }
  throw std::logic_error("Unknown field type");
}

size_t Tuple::size() const { return fields.size(); }

const field_t &Tuple::get_field(size_t i) const { return fields.at(i); }

TupleDesc::TupleDesc(const std::vector<type_t> &types, const std::vector<std::string> &names)
  : types(types), names(names) {
  if (types.size() != names.size()) {
    throw std::invalid_argument("Mismatched sizes of types and names");
  }

  std::unordered_set<std::string> nameSet;
  for (const auto& name : names) {
    if (nameSet.find(name) != nameSet.end()) {
      throw std::invalid_argument("Field names must be unique");
    }
    nameSet.insert(name);
  }
}


bool TupleDesc::compatible(const Tuple &tuple) const {
  // Check that the number of fields matches
  if (tuple.size() != types.size()) {
    return false;
  }

  // Check that each field type matches the schema
  for (size_t i = 0; i < types.size(); ++i) {
    if (tuple.field_type(i) != types[i]) {
      return false;
    }
  }
  return true;
}

size_t TupleDesc::index_of(const std::string &name) const {
  for (size_t i = 0; i < names.size(); ++i) {
    if (names[i] == name) {
      return i;
    }
  }
  throw std::invalid_argument("Field name not found");
}

size_t TupleDesc::offset_of(const size_t &index) const {
  size_t offset = 0;
  for (size_t i = 0; i < index; ++i) {
    switch (types[i]) {
    case type_t::INT:
      offset += sizeof(int);
      break;
    case type_t::DOUBLE:
      offset += sizeof(double);
      break;
    case type_t::CHAR:
      offset += 64;  // Assuming CHAR(64)
      break;
    }
  }
  return offset;
}



size_t TupleDesc::length() const {
  size_t length = 0;
  for (const auto &type : types) {
    switch (type) {
    case type_t::INT:
      length += sizeof(int);
      break;
    case type_t::DOUBLE:
      length += sizeof(double);
      break;
    case type_t::CHAR:
      length += 64;  // Assuming CHAR(64)
      break;
    }
  }
  return length;
}


size_t TupleDesc::size() const {
  return types.size();
}


Tuple TupleDesc::deserialize(const uint8_t *data) const {
  std::vector<field_t> fields;
  size_t offset = 0;

  for (const auto &type : types) {
    switch (type) {
    case type_t::INT: {
      int value;
      std::memcpy(&value, data + offset, sizeof(int));
      fields.push_back(value);
      offset += sizeof(int);
      break;
    }
    case type_t::DOUBLE: {
      double value;
      std::memcpy(&value, data + offset, sizeof(double));
      fields.push_back(value);
      offset += sizeof(double);
      break;
    }
    case type_t::CHAR: {
      std::string value(reinterpret_cast<const char *>(data + offset), 64);
      fields.push_back(value);
      offset += 64;
      break;
    }
    }
  }
  return Tuple(fields);
}


void TupleDesc::serialize(uint8_t *data, const Tuple &t) const {
  size_t offset = 0;

  for (size_t i = 0; i < types.size(); ++i) {
    switch (types[i]) {
    case type_t::INT: {
      int value = std::get<int>(t.get_field(i));
      std::memcpy(data + offset, &value, sizeof(int));
      offset += sizeof(int);
      break;
    }
    case type_t::DOUBLE: {
      double value = std::get<double>(t.get_field(i));
      std::memcpy(data + offset, &value, sizeof(double));
      offset += sizeof(double);
      break;
    }
    case type_t::CHAR: {
      const std::string &value = std::get<std::string>(t.get_field(i));
      std::memcpy(data + offset, value.c_str(), 64);
      offset += 64;
      break;
    }
    }
  }
}


db::TupleDesc TupleDesc::merge(const TupleDesc &td1, const TupleDesc &td2) {
  std::vector<type_t> mergedTypes = td1.types;
  std::vector<std::string> mergedNames = td1.names;

  mergedTypes.insert(mergedTypes.end(), td2.types.begin(), td2.types.end());
  mergedNames.insert(mergedNames.end(), td2.names.begin(), td2.names.end());

  return TupleDesc(mergedTypes, mergedNames);
}

