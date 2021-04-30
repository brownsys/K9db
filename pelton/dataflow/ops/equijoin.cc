//
// Created by Leonhard Spiegelberg on 11/6/20.
//

#include "pelton/dataflow/ops/equijoin.h"

#include <string>
#include <utility>

#include "glog/logging.h"
#include "pelton/dataflow/value.h"
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
      target->SetNull(false, target_index);
      break;
    case sqlast::ColumnDefinition::Type::INT:
      target->SetInt(source.GetInt(source_index), target_index);
      target->SetNull(false, target_index);
      break;
    case sqlast::ColumnDefinition::Type::TEXT:
      // Copies the string.
      target->SetString(
          std::make_unique<std::string>(source.GetString(source_index)),
          target_index);
      target->SetNull(false, target_index);
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
      Key left_value = record.GetValues({this->left_id_});
      for (const Record &right : this->right_table_.Lookup(left_value)) {
        if (this->mode_ == Mode::RIGHT) {
          // Negate any previously emitted NULL + right records.
          for (const Record &right_null :
               this->emitted_nulls_.Lookup(left_value)) {
            this->EmitRow(this->null_records_.at(0), right_null, output, false);
          }
          this->emitted_nulls_.Erase(left_value);
        }
        this->EmitRow(record, right, output, record.IsPositive());
      }

      // additional check for left join
      if (mode_ == Mode::LEFT && 0 == this->right_table_.Count(left_value)) {
        this->EmitRow(record, this->null_records_.at(1), output,
                      record.IsPositive());
        this->emitted_nulls_.Insert(left_value, record);
      }

      // Save record in the appropriate table.
      this->left_table_.Insert(left_value, record);
    } else if (source == this->right()->index()) {
      // Match each record in the left table with this record.
      Key right_value = record.GetValues({this->right_id_});
      for (const Record &left : this->left_table_.Lookup(right_value)) {
        if (this->mode_ == Mode::LEFT) {
          // Negate any previously emitted Left + NULL records.
          for (const Record &left_null :
               this->emitted_nulls_.Lookup(right_value)) {
            this->EmitRow(left_null, this->null_records_.at(1), output, false);
          }
          this->emitted_nulls_.Erase(right_value);
        }
        this->EmitRow(left, record, output, record.IsPositive());
      }

      // additional check for right join
      if (mode_ == Mode::RIGHT && 0 == this->left_table_.Count(right_value)) {
        this->EmitRow(this->null_records_.at(0), record, output,
                      record.IsPositive());
        this->emitted_nulls_.Insert(right_value, record);
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

void EquiJoinOperator::ComputeOutputSchema() {
  // We only have enough information to compute the output schema when
  // both parents are known.
  if (this->input_schemas_.size() < 2) {
    return;
  }

  // Get the schema's of the two parents.
  const SchemaRef &lschema = this->input_schemas_.at(0);
  const SchemaRef &rschema = this->input_schemas_.at(1);

  // Construct joined schema.
  std::vector<sqlast::ColumnDefinition::Type> types = lschema.column_types();
  std::vector<std::string> names = lschema.column_names();
  std::vector<ColumnID> keys = lschema.keys();

  for (size_t i = 0; i < rschema.size(); i++) {
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
      keys.push_back(lschema.size() + key_id);
    } else {
      keys.push_back(lschema.size() + key_id - 1);
    }
  }

  // We own the joined schema.
  this->output_schema_ = SchemaFactory::Create(names, types, keys);

  // Initialize left and right null records
  this->null_records_.push_back(
      std::move(Record::NULLRecord(this->input_schemas_.at(0))));
  this->null_records_.push_back(
      std::move(Record::NULLRecord(this->input_schemas_.at(1))));
}

void EquiJoinOperator::EmitRow(const Record &left, const Record &right,
                               std::vector<Record> *output, bool positive) {
  const SchemaRef &lschema = left.schema();
  const SchemaRef &rschema = right.schema();

  // Create a concatenated record, dropping key column from left side.
  Record record{this->output_schema_, positive};
  for (size_t i = 0; i < lschema.size(); i++) {
    if (left.IsNull(i))
      record.SetNull(true, i);
    else
      CopyIntoRecord(lschema.TypeOf(i), &record, left, i, i);
  }
  for (size_t i = 0; i < rschema.size(); i++) {
    if (i == this->right_id_) {
      continue;
    }
    size_t j = i + lschema.size();
    if (i > this->right_id_) {
      j--;
    }
    if (right.IsNull(i)) {
      record.SetNull(true, j);
    } else
      CopyIntoRecord(rschema.TypeOf(i), &record, right, j, i);
  }

  // add result record to output
  output->push_back(std::move(record));
}

}  // namespace dataflow
}  // namespace pelton
