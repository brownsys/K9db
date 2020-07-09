class Record {
private:
  bool positive_;
  int timestamp_;

  std::vector<int> data_;

  Record(bool positive, const std::vector<int>& v) :
    timestamp(0),
    positive_(positive)
  {
    _v = v;
  }

public:
  Record() : positive_(true), timestamp_(0) {
  }

  Record(int size) : positive_(true), timestamp_(0) {
    data_.resize(size);
  }

  bool is_positive() {
    return positive_;
  }
}
