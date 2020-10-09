#ifndef PELTON_DATAFLOW_SCHEMA_H_
#define PELTON_DATAFLOW_SCHEMA_H_

namespace dataflow {

enum DataType {
  UINT,
  INT,
  TEXT,
  DATETIME,
};

using DataTypeSchema = std::vector<DataType>;

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_SCHEMA_H_
