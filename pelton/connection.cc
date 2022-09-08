#include "pelton/connection.h"

#include <algorithm>
#include <utility>

#include "pelton/sql/rocksdb/connection.h"

namespace pelton {

// Constructor.
State::State(size_t w, bool c) : sstate_(), dstate_(w, c) {}
State::~State() {
  this->dstate_.Shutdown();
  this->database_ = nullptr;
}

// Initialize when db name is known.
void State::Initialize(const std::string &db_name) {
  if (this->database_ == nullptr) {
    this->database_ = std::make_unique<sql::RocksdbConnection>();
    this->database_->Open(db_name);
  }
}

// Manage cannonical prepared statements.
bool State::HasCanonicalStatement(const std::string &canonical) const {
  return this->stmts_.count(canonical) == 1;
}
size_t State::CanonicalStatementCount() const { return this->stmts_.size(); }
const prepared::CanonicalDescriptor &State::GetCanonicalStatement(
    const std::string &canonical) const {
  return this->stmts_.at(canonical);
}
void State::AddCanonicalStatement(const std::string &canonical,
                                  prepared::CanonicalDescriptor &&descriptor) {
  this->stmts_.emplace(canonical, std::move(descriptor));
}

// Statistics.
sql::SqlResult State::FlowDebug(const std::string &view_name) const {
  util::SharedLock lock = this->ReaderLock();
  return this->dstate_.FlowDebug(view_name);
}
sql::SqlResult State::SizeInMemory() const {
  util::SharedLock lock = this->ReaderLock();
  return this->dstate_.SizeInMemory();
}
sql::SqlResult State::NumShards() const {
  util::SharedLock lock = this->ReaderLock();
  return this->sstate_.NumShards();
}
sql::SqlResult State::PreparedDebug() const {
  // Acquire reader lock.
  util::SharedLock reader_lock = this->ReaderLock();
  // Create schema.
  dataflow::SchemaRef schema = dataflow::SchemaFactory::Create(
      {"Statement"}, {sqlast::ColumnDefinition::Type::TEXT}, {0});
  // Get all canonicalized statements and sort them.
  std::vector<std::string> vec;
  for (const auto &[canonical, _] : this->stmts_) {
    vec.push_back(canonical);
  }
  std::sort(vec.begin(), vec.end());
  // Create records.
  std::vector<dataflow::Record> records;
  for (const auto &canonical : vec) {
    records.emplace_back(schema, true,
                         std::make_unique<std::string>(canonical));
  }
  // Return result.
  return sql::SqlResult(sql::SqlResultSet(schema, std::move(records)));
}

// Locks.
util::UniqueLock State::WriterLock() { return util::UniqueLock(&this->mtx_); }
util::SharedLock State::ReaderLock() const {
  return util::SharedLock(&this->mtx_);
}

}  // namespace pelton
