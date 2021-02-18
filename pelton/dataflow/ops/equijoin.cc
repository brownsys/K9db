//
// Created by Leonhard Spiegelberg on 11/6/20.
//

#include "pelton/dataflow/ops/equijoin.h"

#include <string>
#include <utility>

#include "glog/logging.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace dataflow {

namespace {

inline void CopyIntoRecord(sqlast::ColumnDefinition::Type datatype,
                           Record *target, const Record &source,
                           size_t target_index, size_t source_index) {
  switch (datatype) {
    case sqlast::ColumnDefinition::Type::UINT:
      target->SetUInt(source.GetUInt(source_index), target_index);
      break;
    case sqlast::ColumnDefinition::Type::INT:
      target->SetInt(source.GetInt(source_index), target_index);
      break;
    case sqlast::ColumnDefinition::Type::TEXT:
      // Copies the string.
      target->SetString(
          std::make_unique<std::string>(source.GetString(source_index)),
          target_index);
      break;
    default:
      LOG(FATAL) << "Unsupported type in equijoin emit";
  }
}

}  // namespace

bool EquiJoinOperator::Process(NodeIndex source,
                               const std::vector<Record> &records,
                               std::vector<Record> *output) {
  for (const Record &record : records) {
    // In comparison to a normal HashJoin in a database, here
    // records flow from one operator in.

    // Append all input records to the corresponding hashtable.
    if (source == this->left()->index()) {
      // Match each record in the right table with this record.
      Key left_value = record.GetValue(this->left_id_);
      for (const auto &right : this->right_table_.Get(left_value)) {
        this->EmitRow(record, right, output);
      }

      // Save record in the appropriate table.
      this->left_table_.Insert(left_value, record);
    } else if (source == this->right()->index()) {
      // Match each record in the left table with this record.
      Key right_value = record.GetValue(this->right_id_);
      for (const auto &left : this->left_table_.Get(right_value)) {
        this->EmitRow(left, record, output);
      }

      // save record hashed to right table
      this->right_table_.Insert(right_value, record);
    } else {
      LOG(FATAL) << "EquiJoinOperator got input from Node " << source
                 << " but has parents " << this->left()->index() << " and "
                 << this->right()->index();
      return false;
    }
  }
  return true;
}

void EquiJoinOperator::ComputeJoinedSchema(const Schema &lschema,
                                           const Schema &rschema) {
  std::vector<sqlast::ColumnDefinition::Type> types = lschema.column_types();
  std::vector<std::string> names = lschema.column_names();
  std::vector<ColumnID> keys = lschema.keys();

  for (size_t i = 0; i < rschema.Size(); i++) {
    if (i != this->right_id_) {
      types.push_back(rschema.TypeOf(i));
      names.push_back(rschema.NameOf(i));
    }
  }
  for (ColumnID key_id : rschema.keys()) {
    // right_id_ column is dropped, if it is part of the key, we must
    // substitute it with left_id_ so that the key is complete.
    if (key_id == this->right_id_) {
      for (size_t i = 0; i < keys.size(); i++) {
        // left_id_ already part of key.
        if (keys.at(i) == this->left_id_) {
          break;
        }
        // Insert left_id_ in order.
        if (keys.at(i) > this->left_id_) {
          keys.insert(keys.begin() + i, this->left_id_);
          break;
        }
        // Push at the end.
        if (i == keys.size() - 1) {
          keys.push_back(this->left_id_);
          break;
        }
      }
    } else if (key_id < this->right_id_) {
      keys.push_back(lschema.Size() + key_id);
    } else {
      keys.push_back(lschema.Size() + key_id - 1);
    }
  }
  this->joined_schema_ = std::make_unique<Schema>(names, types, keys);
}

void EquiJoinOperator::EmitRow(const Record &left, const Record &right,
                               std::vector<Record> *output) {
  const Schema &lschema = left.schema();
  const Schema &rschema = right.schema();

  // Set schemas here lazily, note this can get optimized.
  if (!this->joined_schema_) {
    this->ComputeJoinedSchema(lschema, rschema);
  }

  // Create a concatenated record, dropping key column from left side.
  Record record{*this->joined_schema_};
  for (size_t i = 0; i < lschema.Size(); i++) {
    CopyIntoRecord(lschema.TypeOf(i), &record, left, i, i);
  }
  for (size_t i = 0; i < rschema.Size(); i++) {
    if (i == this->right_id_) {
      continue;
    }
    size_t j = i + lschema.Size();
    if (i > this->right_id_) {
      j--;
    }
    CopyIntoRecord(rschema.TypeOf(i), &record, right, j, i);
  }

  // add result record to output
  output->push_back(std::move(record));
}

}  // namespace dataflow
}  // namespace pelton
