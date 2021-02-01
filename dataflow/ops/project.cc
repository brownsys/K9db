#include "dataflow/ops/project.h"

#include <vector>

#include "dataflow/record.h"

namespace dataflow {

bool ProjectOperator::process(std::vector<Record>& rs,
                             std::vector<Record>& out_rs) {
  for(Record& r : rs){
    Record out_r(out_schema_);
    for(size_t i = 0; i < cids_.size(); i++){
      switch((*in_schema_).TypeOf(cids_.at(i))){
        case DataType::kUInt:
          out_r.set_uint(i, r.as_uint(cids_.at(i)));
          break;
        case DataType::kInt:
          out_r.set_int(i, r.as_int(cids_.at(i)));
          break;
        case DataType::kText:
          out_r.set_string(i, r.as_string(cids_.at(i)));
          break;
        default:
          // TODO(Ishan): Handle datetime
          LOG(FATAL) << "Unimplemented type";
      }
    }
    out_rs.push_back(out_r);
  }
  return true;
}

}  // namespace dataflow
