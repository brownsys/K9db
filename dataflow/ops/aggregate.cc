#include "dataflow/ops/aggregate.h"

#include <vector>

#include "dataflow/record.h"

namespace dataflow {

//This class is used to track state changes when processing given batch of input records
class StateChange{
  public:
    StateChange(bool is_insert) : is_insert_(is_insert){
      //nothing to do
    }
    StateChange(bool is_insert, uint64_t old_val) : is_insert_(is_insert), old_val_(old_val){
      //nothing to do
    }

    bool is_insert_;
    uint64_t old_val_;
};

bool AggregateOperator::process(std::vector<Record>& rs,
                                std::vector<Record>& out_rs) {
  absl::flat_hash_map<std::vector<RecordData>, StateChange> state_changes;
  for (Record& r : rs) {
    std::vector<RecordData> key = get_key(r);

    //handle negative record
    if(!r.positive()){
      if(!state_.contains(key)){
        continue;
      }
      switch (agg_func_) {
        case FuncCount:
          if(!state_changes.contains(key)){ //track state change
            state_changes.emplace(key,StateChange(false,state_.at(key)));
          }
          if(state_.at(key)>1){
            state_.at(key) = state_.at(key) - 1;
          } else{
            state_.at(key) = 0;
          }
          break;
        case FuncSum:
          if(!state_changes.contains(key)){ //track state change
            state_changes.emplace(key,StateChange(false,state_.at(key)));
          }
          if(state_.at(key)>r.raw_at(agg_col_).as_val()){
            state_.at(key) = state_.at(key) - r.raw_at(agg_col_).as_val();
          } else{
            state_.at(key) = 0;
          }
          break;
      }
      continue;
    }

    //handle positive record
    if (!state_.contains(key)) {
      switch (agg_func_) {
        case FuncCount:
          state_changes.emplace(key,StateChange(true));
          state_.emplace(key, 1);
          break;
        case FuncSum:
          state_changes.emplace(key,StateChange(true));
          state_.emplace(key, r.raw_at(agg_col_).as_val());
          break;
      }
    } else {
      switch (agg_func_) {
        case FuncCount:
          if(!state_changes.contains(key)){ //track state change
            state_changes.emplace(key,StateChange(false,state_.at(key)));
          }
          state_.at(key) = state_.at(key) + 1;
          break;
        case FuncSum:
          if(!state_changes.contains(key)){ //track state change
            state_changes.emplace(key,StateChange(false,state_.at(key)));
          }
          state_.at(key) = state_.at(key) + r.raw_at(agg_col_).as_val();
          break;
      }
    }
  }

  //emit records only for tracked changes
  for(auto const& item : state_changes){
    std::vector<RecordData> pos_record = item.first;
    pos_record.push_back(RecordData(state_.at(item.first)));
    if(item.second.is_insert_){
      out_rs.push_back(Record(true, pos_record, 3ULL));
    } else{
      std::vector<RecordData> neg_record = item.first;
      neg_record.push_back(RecordData(item.second.old_val_));
      out_rs.push_back(Record(false, neg_record, 3ULL));
      out_rs.push_back(Record(true, pos_record, 3ULL));
    }
  }

  return true;
}

}  // namespace dataflow
