//
// Created by Leonhard Spiegelberg on 11/6/20.
//

#include "pelton/dataflow/ops/equijoin.h"

#include <string>
#include <utility>

#include "glog/logging.h"
#include "pelton/dataflow/key.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace dataflow {

namespace {

inline void CopyIntoRecord(sqlast::ColumnDefinition::Type datatype,
                           Record *target, const Record &source,
                           size_t target_index, size_t source_index) {
  if (source.IsNull(source_index)) {
    target->SetNull(true, target_index);
    return;
  }
  switch (datatype) {
    case sqlast::ColumnDefinition::Type::UINT:
      target->SetUInt(source.GetUInt(source_index), target_index);
      target->SetNull(false, target_index);
      break;
    case sqlast::ColumnDefinition::Type::INT:
      target->SetInt(source.GetInt(source_index), target_index);
      target->SetNull(false, target_index);
      break;
    case sqlast::ColumnDefinition::Type::DATETIME:
      // Copies the string.
      target->SetDateTime(
          std::make_unique<std::string>(source.GetDateTime(source_index)),
          target_index);
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

Operator *EquiJoinOperator::left() const { return this->parents_.at(0); }
Operator *EquiJoinOperator::right() const { return this->parents_.at(1); }

std::vector<Record> EquiJoinOperator::Process(NodeIndex source,
                                              std::vector<Record> &&records,
                                              const Promise &promise) {
  std::vector<Record> output;
  for (Record &record : records) {
    // In comparison to a normal HashJoin in a database, here
    // records flow from one operator in.

    // Append all input records to the corresponding hashtable.
    if (source == this->left()->index()) {
      // Find matching record from right table.
      Key lvalue = record.GetValues({this->left_id_});
      if (this->right_table_.Contains(lvalue)) {
        // Matching records exist.
        for (const Record &right : this->right_table_.Get(lvalue)) {
          if (this->mode_ == Mode::RIGHT) {
            // Negate any previously emitted NULL + right records.
            if (this->emitted_nulls_.Contains(lvalue)) {
              for (const Record &rnull : this->emitted_nulls_.Get(lvalue)) {
                this->EmitRow(this->null_records_.at(0), rnull, &output, false);
              }
              this->emitted_nulls_.Erase(lvalue);
            }
          }
          this->EmitRow(record, right, &output, record.IsPositive());
        }
      } else if (this->mode_ == Mode::LEFT) {
        // No mathing records with left join, emit this record with some nulls.
        this->EmitRow(record, this->null_records_.at(1), &output,
                      record.IsPositive());
        if (record.IsPositive()) {
          this->emitted_nulls_.Insert(lvalue, record.Copy());
        } else {
          this->emitted_nulls_.Delete(lvalue, record.Copy());
        }
      }

      // Save record in the appropriate table.
      if (record.IsPositive()) {
        this->left_table_.Insert(lvalue, std::move(record));
      } else {
        this->left_table_.Delete(lvalue, std::move(record));
      }
    } else if (source == this->right()->index()) {
      // Find matching record from left table.
      Key rvalue = record.GetValues({this->right_id_});
      if (this->left_table_.Contains(rvalue)) {
        // Matching records exist.
        for (const Record &left : this->left_table_.Get(rvalue)) {
          if (this->mode_ == Mode::LEFT) {
            // Negate any previously emitted Left + NULL records.
            if (this->emitted_nulls_.Contains(rvalue)) {
              for (const Record &lnull : this->emitted_nulls_.Get(rvalue)) {
                this->EmitRow(lnull, this->null_records_.at(1), &output, false);
              }
              this->emitted_nulls_.Erase(rvalue);
            }
          }
          this->EmitRow(left, record, &output, record.IsPositive());
        }
      } else if (mode_ == Mode::RIGHT) {
        // No mathing records with right join, emit this record with some nulls.
        this->EmitRow(this->null_records_.at(0), record, &output,
                      record.IsPositive());
        if (record.IsPositive()) {
          this->emitted_nulls_.Insert(rvalue, record.Copy());
        } else {
          this->emitted_nulls_.Delete(rvalue, record.Copy());
        }
      }

      // Save record in the appropriate table.
      if (record.IsPositive()) {
        this->right_table_.Insert(rvalue, std::move(record));
      } else {
        this->right_table_.Delete(rvalue, std::move(record));
      }
    } else {
      LOG(FATAL) << "EquiJoinOperator got input from Node " << source
                 << " but has parents " << this->left()->index() << " and "
                 << this->right()->index();
    }
  }
  return output;
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

  // Initialize the state.
  this->left_table_.Initialize(this->output_schema_);
  this->right_table_.Initialize(this->output_schema_);
  this->emitted_nulls_.Initialize(this->output_schema_);
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
    } else {
      CopyIntoRecord(rschema.TypeOf(i), &record, right, j, i);
    }
  }

  // add result record to output
  output->push_back(std::move(record));
}

std::unique_ptr<Operator> EquiJoinOperator::Clone() const {
  return std::make_unique<EquiJoinOperator>(this->left_id_, this->right_id_,
                                            this->mode_);
}

}  // namespace dataflow
}  // namespace pelton
