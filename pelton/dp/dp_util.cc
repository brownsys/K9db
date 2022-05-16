#include "pelton/dp/dp_util.h"

#include <iterator>
#include <optional>
#include <regex>
#include <string>

namespace pelton {
namespace dp {

DPOptions::DPOptions() {
  epsilon = std::nullopt;
  delta = std::nullopt;
  bound_l = std::nullopt;
  bound_h = std::nullopt;
}

std::string tracker_name(const std::string &original_name) {
  return original_name + "_BudgetTracker";
}

std::string tracker_creation_query(const std::string &original_name) {
  const std::string tracker_name = pelton::dp::tracker_name(original_name);
  return "CREATE TABLE " + tracker_name +
         " (id INT PRIMARY KEY NOT NULL AUTO_INCREMENT, e INT, v varchar(255), "
         "q varchar(255));";
}

std::string tracker_insert_query(const std::string &tracker_name, const std::string &e, const std::string &v, const std::string &q) {
    return "INSERT INTO " + tracker_name + " (e, v, q) VALUES (" +
            e + ", '" + v + "', '" + q + "');";
}

std::string tracker_select_query(const std::string &tracker_name) {
  return "SELECT e FROM " + tracker_name + ";";
}

std::optional<double> ParseTableForDP(const std::string &table_name) {
  std::regex pii_dp_regex("_DP(?:_budget([\\.0-9]+))?",
                          std::regex_constants::ECMAScript);
  std::smatch matches;
  std::regex_search(table_name, matches, pii_dp_regex);

  if (matches.size() > 0) {
    // Use DP settings
    double budget = ((matches[1] != "") ? std::stof(matches[1]) : 10.0);
    return std::optional<double>{budget};
  }
  return std::nullopt;
}

std::optional<DPOptions> ParseViewForDP(const std::string &view_name) {
  std::regex dpr(
      "_DP(?:_(e(?:\\!|[0-9])+))?(?:_(d(?:\\!|[0-9])+))?(?:_(l(?:\\!|[0-9])+))?(?:"
      "_(h(?:\\!|[0-9])+))?",
      std::regex_constants::ECMAScript);
  std::smatch match;

  if (std::regex_search(view_name, match, dpr) == true) {
    // Use DP settings
    DPOptions options = DPOptions();
    for (uint i = 1; i < match.size(); i++) {
      std::string argument = match.str(i);
      std::string cleaned;
      std::regex replace("\\!");
      std::regex_replace(std::back_inserter(cleaned), argument.begin(),
                         argument.end(), replace, ".");

      char option = cleaned[0];
      cleaned.erase(0, 1);
      if (option == 'e') {
        options.epsilon = std::stof(cleaned);
      } else if (option == 'd') {
        options.delta = std::stof(cleaned);
      } else if (option == 'l') {
        options.bound_l = std::stof(cleaned);
      } else if (option == 'h') {
        options.bound_h = std::stof(cleaned);
      }
    }
    return options;
  }
  return std::nullopt;
}
}  // namespace dp
}  // namespace pelton
