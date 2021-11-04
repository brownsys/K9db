// NOLINTFILE(build/include_what_you_use)
namespace {

// Type of keys by whether they describe the key produced by an operator,
// or required by an operator (from its parents).
enum class Any { UNIT };
enum class Unknown { UNIT };
enum class Inconsistent { UNIT };
using ProducerKey = std::variant<PartitionKey, Any, Inconsistent>;
using RequiredKey = std::variant<PartitionKey, Any>;

std::string STR(const ProducerKey &k) {
  std::string key = "";
  if (std::holds_alternative<PartitionKey>(k)) {
    key = "[ ";
    for (const auto &col : std::get<PartitionKey>(k)) {
      key += std::to_string(col) + ",";
    }
    key.pop_back();
    key += " ]";
  } else if (std::holds_alternative<Any>(k)) {
    key = "Any";
  } else {
    key = "Inconsistent";
  }
  return key;
}

std::string STR(const RequiredKey &k) {
  std::string key = "";
  if (std::holds_alternative<PartitionKey>(k)) {
    key = "[ ";
    for (const auto &col : std::get<PartitionKey>(k)) {
      key += std::to_string(col) + ",";
    }
    key.pop_back();
    key += " ]";
  } else {
    key = "Any";
  }
  return key;
}

// Initial keys by operator type.
ProducerKey ProducerKeyOf(const Operator &op) {
  switch (op.type()) {
    case Operator::Type::MAT_VIEW: {
      auto matview = static_cast<const MatViewOperator *>(&op);
      if (matview->key_cols().size() != 0) {
        return matview->key_cols();
      }
      return Any::UNIT;
    }
    case Operator::Type::EQUIJOIN: {
      auto equijoin = static_cast<const EquiJoinOperator *>(&op);
      return PartitionKey{equijoin->left_id()};
    }
    case Operator::Type::AGGREGATE: {
      auto aggregate = static_cast<const AggregateOperator *>(&op);
      size_t num = aggregate->group_columns().size();
      PartitionKey keys;
      keys.reserve(num);
      for (ColumnID i = 0; i < aggregate->group_columns().size(); i++) {
        keys.push_back(i);
      }
      return keys;
    }
    default:
      return Any::UNIT;
  }
}
RequiredKey RequiredKeyOf(const Operator &op, NodeIndex parent) {
  switch (op.type()) {
    case Operator::Type::MAT_VIEW: {
      auto matview = static_cast<const MatViewOperator *>(&op);
      if (matview->key_cols().size() != 0) {
        return matview->key_cols();
      }
      return Any::UNIT;
    }
    case Operator::Type::EQUIJOIN: {
      auto equijoin = static_cast<const EquiJoinOperator *>(&op);
      if (parent == equijoin->left()->index()) {
        return PartitionKey{equijoin->left_id()};
      } else if (parent == equijoin->right()->index()) {
        return PartitionKey{equijoin->right_id()};
      }
      LOG(FATAL) << "Bad equijoin source during partitioning";
    }
    case Operator::Type::AGGREGATE: {
      auto aggregate = static_cast<const AggregateOperator *>(&op);
      return aggregate->group_columns();
    }
    default:
      return Any::UNIT;
  }
}

ProducerKey ProjectKey(const ProjectOperator &op, PartitionKey key) {
  PartitionKey projected;
  projected.reserve(key.size());
  for (auto col : key) {
    auto projected_col = op.ProjectColumn(col);
    if (projected_col) {
      projected.push_back(projected_col.value());
    } else {
      return Inconsistent::UNIT;
    }
  }
  return projected;
}
ProducerKey UnprojectKey(const ProjectOperator &op, PartitionKey key) {
  PartitionKey unprojected;
  unprojected.reserve(key.size());
  for (auto col : key) {
    auto unprojected_col = op.UnprojectColumn(col);
    if (unprojected_col) {
      unprojected.push_back(unprojected_col.value());
    } else {
      return Inconsistent::UNIT;
    }
  }
  return unprojected;
}

// Node id -> partition key produced by this node.
using ProducerMap = std::unordered_map<NodeIndex, ProducerKey>;

// child id -> parent id -> partition key required by child from parent.
using RequiredMap =
    std::unordered_map<NodeIndex, std::unordered_map<NodeIndex, RequiredKey>>;

// Maps a node to the keys we want to force it to have (externally), rather than
// allowing the keys to be deduced organically by our BFS.
// This is useful for cases where we could not deduce any concrete keys (e.g.
// the flow partitioning is completely free with no constraints).
using ExternalMap = std::unordered_map<NodeIndex, PartitionKey>;

// Debugging.
void DebugPrint(DataFlowGraphPartition *partition, const ProducerMap &p,
                const RequiredMap &r) {
  std::cout << std::endl;
  std::cout << "Keys:" << std::endl;
  std::cout << std::endl;
  for (NodeIndex i = partition->Size() - 1; i >= 0; i--) {
    std::cout << "Node " << i << ": ";
    if (p.count(i)) {
      std::cout << STR(p.at(i));
    } else {
      std::cout << "UNDEFINED";
    }
    std::cout << std::endl;
    for (Operator *child : partition->GetNode(i)->children()) {
      NodeIndex j = child->index();
      std::cout << "  child " << j << ": ";
      if (r.count(j) && r.at(j).count(i)) {
        std::cout << STR(r.at(j).at(i));
      } else {
        std::cout << "UNDEFINED";
      }
      std::cout << std::endl;
    }
    if (i == 0) {
      break;
    }
  }
  std::cout << std::endl;
  std::cout << std::endl;
}

// Traverse the operator graph in DFS order.
// Returns the key required by operator op from the parent whose ID is from.
// Also, fills in the appropriate keys in *p and *r.
// Pre-condition: key is either a PartitionKey or Any.
// Pre-condition: from == UNDEFINED_NODE_INDEX iff op is an InputOperator.
RequiredKey DFS(const Operator &op, NodeIndex from, const ProducerKey &key,
                ProducerMap *p, RequiredMap *r, const ExternalMap &e) {
  bool external = e.count(op.index()) == 1;
  // Initialize r[op] if this is the first time we visit op.
  if (r->count(op.index()) == 0) {
    r->emplace(op.index(), std::unordered_map<NodeIndex, RequiredKey>{});
  }

  // Find required key between from and op.
  RequiredKey rkey = external ? e.at(op.index()) : RequiredKeyOf(op, from);

  // Check if op has its own partition key for outputs.
  ProducerKey opkey = external ? e.at(op.index()) : ProducerKeyOf(op);
  if (std::holds_alternative<Any>(opkey)) {
    if (std::holds_alternative<PartitionKey>(key)) {
      // Inherit key of parent.
      rkey = std::get<PartitionKey>(key);
      // Parent key might become inconsistent or get re-ordered if op is a
      // project (e.g. if it drops components of the key).
      if (op.type() == Operator::Type::PROJECT) {
        auto project = static_cast<const ProjectOperator *>(&op);
        opkey = ProjectKey(*project, std::get<PartitionKey>(key));
      } else {
        opkey = key;
      }
    }
  }

  // Traverse children.
  // Pre-condition met: opkey is either a PartitionKey or Any.
  std::vector<RequiredKey> children_keys;
  for (Operator *child : op.children()) {
    if (std::holds_alternative<Inconsistent>(opkey)) {
      // From the perspective of children, if the key is inconsistent,
      // then op becomes an unconstrained operation. We get to select any
      // desired new key post-project for free, since an exchange will be needed
      // anyway after a corrupting project.
      children_keys.push_back(DFS(*child, op.index(), Any::UNIT, p, r, e));
    } else {
      children_keys.push_back(DFS(*child, op.index(), opkey, p, r, e));
    }
  }

  // If op was free, we can use children to find optimal partitioning for it.
  // If op was inconsistent, it remains inconsistent regardless of children.
  if (std::holds_alternative<Any>(opkey) && children_keys.size() > 0) {
    // All children MUST have the same partitioning.
    if (children_keys.size() > 1 &&
        !std::equal(children_keys.cbegin() + 1, children_keys.cend(),
                    children_keys.cbegin())) {
      // We can select any of the children keys here and functionaly the
      // solution would probably work. However, it will lead to in-optimal
      // partitioning and placement of exchanges.
      // If we need to support this quickly but sub-optimally uncomment below.
      // Make sure to reflect projection logic below as well.
      // for (const RequiredKey &child_key : children_keys) {
      //   if (std::holds_alternative<PartitionKey>(child_key)) {
      //     rkey = child_key;
      //     opkey = std::get<PartitionKey>(rkey);
      //     break;
      //   }
      // }
      LOG(FATAL) << "Node has children with different partition keys.";
    } else if (std::all_of(children_keys.cbegin(), children_keys.cend(),
                           [](RequiredKey k) {
                             return std::holds_alternative<PartitionKey>(k);
                           })) {
      // Check that all children have been constrained.
      const RequiredKey &child_key = children_keys.front();
      if (std::holds_alternative<PartitionKey>(child_key)) {
        if (op.type() == Operator::Type::PROJECT) {
          // Child key (after project) must be translated to the key before
          // project.
          auto project = static_cast<const ProjectOperator *>(&op);
          opkey = UnprojectKey(*project, std::get<PartitionKey>(child_key));
          // Can the key be translated? (e.g. literal column added by project).
          if (std::holds_alternative<Inconsistent>(opkey)) {
            // Projection no longer constrained, we will add an exchange
            // for the new key after project (since it cannot be computed
            // any earlier).
            rkey = Any::UNIT;
          } else {
            // Projection translation can only re-order key, but key is valid.
            rkey = std::get<PartitionKey>(opkey);
          }
        } else {
          rkey = child_key;
          opkey = std::get<PartitionKey>(child_key);
        }
      }
    }
  }

  // Set keys in maps, provided having multiple parents does not create issues.
  // These cases should never occur, if we ever encounter this error we need to:
  // 1) check that the planning is correct. Most likely, the plan is wrong
  //    which is why this case pops up.
  // 2) check the logic of the query. A query needs to perform a union in a very
  //    weird way to exercise this case. The only queries I can think of that
  //    may exercise it are ones that contain unions of several weird
  //    sub-queries that are each radically different in their operators.
  // 3) change this function so that it assigns to the maps once, and then
  //    never re-assigns to them. The function checks p[op] before recursing to
  //    children, if it was set, the function sets r[op][source] = rkey
  //    or to p[op] if rkey is Any, and returns that value without recursing.
  if (p->count(op.index()) == 1) {
    CHECK(p->at(op.index()) == opkey) << "Parents set child key differently";
  } else {
    p->emplace(op.index(), opkey);
  }
  if (from != UNDEFINED_NODE_INDEX) {
    if (r->at(op.index()).count(from) == 1) {
      CHECK(r->at(op.index()).at(from) == rkey)
          << "Parents constraint child edge differently";
    } else {
      r->at(op.index()).emplace(from, rkey);
    }
  }

  return rkey;
}

// This is called after a round of DFS was executed. If the provided vector
// contains an operator for which no partitioning key was assigned, then we
// externally assign one to it based on its schema.
// returns true if such operators were found, false if all are assigned a key.
template <typename T>
bool ExternallyAssignKey(const std::vector<T *> &operators,
                         const ProducerMap &p, ExternalMap *e) {
  bool result = false;
  for (Operator *op : operators) {
    if (std::holds_alternative<Any>(p.at(op->index()))) {
      result = true;
      SchemaRef schema = op->output_schema();
      if (schema.keys().size() > 0) {
        e->emplace(op->index(), schema.keys());
      } else {
        PartitionKey key;
        for (ColumnID c = 0; c < schema.size(); c++) {
          key.push_back(c);
        }
        e->emplace(op->index(), std::move(key));
      }
    }
  }
  return result;
}

std::optional<PartitionKey> ExchangeKey(Operator *parent, Operator *child,
                                        const ProducerMap &p,
                                        const RequiredMap &r) {
  // Check that parent produces the required format.
  RequiredKey rkey = r.at(child->index()).at(parent->index());

  // No required format, anything goes, no need for exchange.
  if (std::holds_alternative<Any>(rkey)) {
    return {};
  }

  // If pkey is Any or inconsistent we need an exchange to specialize to rkey.
  PartitionKey in = std::get<PartitionKey>(rkey);
  ProducerKey pkey = p.at(parent->index());
  if (std::holds_alternative<PartitionKey>(pkey)) {
    // If out \equiv in, we do not need an exchange.
    PartitionKey out = std::get<PartitionKey>(pkey);
    // If parent is a project, we must take order and projectiosn when checking
    // whether out \equiv in .
    if (parent->type() == Operator::Type::PROJECT) {
      auto project = static_cast<ProjectOperator *>(parent);
      PartitionKey translated_in;
      for (ColumnID c : in) {
        auto result = project->UnprojectColumn(c);
        if (result) {
          translated_in.push_back(result.value());
        }
      }
      in = translated_in;
    }
    // With project accounted for, we just need out == in.
    if (out == in) {
      return {};
    }
  }

  // We know that pkey conflicts with in. Either because pkey is Any (i.e
  // underspecified) or Inconsistent, or because pkey is a concrete key that
  // is not semantically equivalent to in. We must add an exchange.
  return std::get<PartitionKey>(rkey);
}

}  // namespace
