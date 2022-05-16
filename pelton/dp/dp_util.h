#ifndef PELTON_DP_UTIL_H_
#define PELTON_DP_UTIL_H_

#include <iostream>
#include <optional>
#include <string>

namespace pelton {
namespace dp {

class DPOptions {
 public:
  std::optional<double> epsilon;
  std::optional<double> delta;
  std::optional<double> bound_l;
  std::optional<double> bound_h;
  DPOptions();
  void print() {
    std::cout << "epsilon " << std::to_string(epsilon.value_or(-1.0))
              << std::endl;
    std::cout << "delta   " << std::to_string(delta.value_or(-1.0))
              << std::endl;
    std::cout << "bound_l " << std::to_string(bound_l.value_or(-1.0))
              << std::endl;
    std::cout << "bound_h " << std::to_string(bound_h.value_or(-1.0))
              << std::endl;
  }
};

/*!
* Given the name of a (presumed budgeted) table, return the name of the budget
* tracker for the table.
* @param original_name the original name of the table.
* @return the name of the budget tracker for the original table.
*/
std::string tracker_name(const std::string &original_name);

/*!
* Given the name of a (presumed budgeted) table, return a query string for the creation
* of a budget tracker for the table.
* @param original_name the original name of the table.
* @return the query string to create a budget tracker for the table.
*/
std::string tracker_creation_query(const std::string &original_name);

/*!
* Given the name of a budget tracker, the epsilon value of a DP query, the resultant view name of that query, and the
* query itself, return the query string for updating the tracker with the provided query data.
* @param tracker_name the name of the budget tracker
* @param e string representation of the epsilon value
* @param v string representation of the DP view name
* @param q DP query string
* @return the query string to update the budget tracker for a new DP query
*/
std::string tracker_insert_query(const std::string &tracker_name, const std::string &e, const std::string &v, const std::string &q);

/*!
* Given the name of a budget tracker, return the query string to select the epsilon column.
* @param tracker_name the name of the budget tracker
* @return the query string to select the epsilon column from budget tracker
*/
std::string tracker_select_query(const std::string &tracker_name);

/*!
* Given the name of a table, parse to see if the table is budgeted.
* @param table_name the name of the table to parse
* @return an optional double representing the parsed budget. If the table is marked as budgeted but the budget is not specified, the default value of 10.0 is used. If the table is not marked as budgeted, the nullopt is returned.
*/
std::optional<double> ParseTableForDP(const std::string &table_name);

/*!
* Given the name of a view, parse to see if the view is to be differentially private.
* @param view_name the name of the view to parse
* @return an optional DPOptions representing the parsed DP parameters. If the view is not marked as DP, the nullopt is returned.
*/
std::optional<DPOptions> ParseViewForDP(const std::string &view_name);

}  // namespace dp
}  // namespace pelton

#endif  // PELTON_DP_UTIL_H_