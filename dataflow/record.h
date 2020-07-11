#include <unistd.h>

#include <limits>
#include <vector>

#define TOP_BIT std::numeric_limits<int64_t>::min()

struct RecordData {
  // store 8 bytes, which are either an immediate value or a pointer to data,
  // signified by the top bit (which isn't valid in an x86-64 pointer).
  uint64_t data_;

  char* operator*() {
    if (data_ & TOP_BIT) {
      return (char*)(data_ ^ TOP_BIT);
    } else {
      return (char*)&data_;
    }
  }
};

class Record {
 private:
  bool positive_;
  int timestamp_;

  std::vector<RecordData> data_;

 public:
  Record(bool positive, const std::vector<int>& v)
      : positive_(positive), timestamp_(0) {
    for (int i : v) {
      data_.push_back(RecordData{.data_ = (uint64_t)i});
    }
  }

  Record() : positive_(true), timestamp_(0) {}

  Record(int size) : positive_(true), timestamp_(0) { data_.resize(size); }

  bool is_positive() { return positive_; }

  int timestamp() { return timestamp_; }
};
