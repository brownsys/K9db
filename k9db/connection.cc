#include "k9db/connection.h"

#include <algorithm>
#include <utility>

#include "k9db/dataflow/record.h"
#include "k9db/dataflow/schema.h"
#include "k9db/sql/factory.h"
#include "k9db/util/status.h"

namespace k9db {

// Constructor.
State::State(size_t w, bool c) : sstate_(), dstate_(w, c) {}
State::~State() {
  this->dstate_.Shutdown();
  this->database_ = nullptr;
}

// Initialize when db name is known.
std::vector<std::string> State::Initialize(const std::string &db_name,
                                           const std::string &db_path) {
  auto lock = this->WriterLock();
  if (this->database_ == nullptr) {
    this->database_ = sql::MakeConnection();
    return this->database_->Open(db_name, db_path);
  }
  return {};
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
  std::vector<dataflow::Record> records;
  util::SharedLock lock = this->ReaderLock();
  for (auto &&[kind, num] : this->sstate_.NumShards()) {
    records.emplace_back(dataflow::SchemaFactory::NUM_SHARDS_SCHEMA, true,
                         std::make_unique<std::string>(std::move(kind)), num);
  }
  return sql::SqlResult(sql::SqlResultSet(
      dataflow::SchemaFactory::NUM_SHARDS_SCHEMA, std::move(records)));
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

sql::SqlResult State::ListIndices() const {
  // Acquire reader lock.
  util::SharedLock reader_lock = this->ReaderLock();
  // Get schema.
  dataflow::SchemaRef schema = dataflow::SchemaFactory::LIST_INDICES_SCHEMA;
  // Loop over all tables and add their indices.
  std::vector<dataflow::Record> records;
  for (const std::string &table : this->sstate_.GetTables()) {
    for (const std::string &columns : this->database_->GetIndices(table)) {
      records.emplace_back(schema, true, std::make_unique<std::string>(table),
                           std::make_unique<std::string>(columns));
    }
  }
  // Return result.
  return sql::SqlResult(sql::SqlResultSet(schema, std::move(records)));
}

// Locks.
util::UniqueLock State::WriterLock() { return util::UniqueLock(&this->mtx_); }
util::SharedLock State::ReaderLock() const {
  return util::SharedLock(&this->mtx_);
}
util::SharedLock State::CanonicalReaderLock() const {
  return util::SharedLock(&this->canonical_mtx_);
}

}  // namespace k9db
