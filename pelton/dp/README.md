# Differential Privacy in Pelton

It may be useful to provide users of Pelton simple mechanisms for running [differentially private](https://en.wikipedia.org/wiki/Differential_privacy) aggregate queries across shared user information. Because Pelton presents an interface compatible with standard SQL queries, other tools for creating and managing differentially private data releases (such as [PINQ](https://www.microsoft.com/en-us/research/project/privacy-integrated-queries-pinq/) and [SmartNoise/OpenDP](https://github.com/opendp)), integrating such tools into Pelton itself has the potential to offer a few benefits:
- Because Pelton already requires developers to annotate schemas for GDPR compliance, requiring them to add additional (optional) annotations to the same data subject tables is a relatively low-effort ask.
- While Pelton does not address access control directly, if a desire access control is implemented such that a user can run *only* differentially private queries, these efforts mean that Pelton (behind some access control mechanism) can be exposed directly to the restricted user. Real user data values are handled internally by Pelton, and noise is added before the differentially private view is committed. Additional proxy infrastructure (besides access control) between Pelton and the restricted user to ensure the restricted never has access to the unaggregated or un-noised data is thus not needed.

## Implementation

There are two main interfaces concerning differential privacy in Pelton. First, data subject tables can optionally be annotated with a privacy budget. Second, created materialized views can be annotated with differential privacy parameters.

This work relies on [Google's differential privacy libraries](https://github.com/google/differential-privacy).

### Specifying a Privacy Budget

For a table containing personally identifiable information, adding a suffix of `_DP` to the table name will mean that the table privacy budget will be set at a default value of `10`. Adding a suffix of `_DP_budgetX`, where `X` is some positive integer value, will set the privacy budget to `X`.

For example, this table creation query:
```sql
CREATE TABLE users (
    ...,
    PII_name VARCHAR(64) NOT NULL,
    ...,
);
```
can be assigned a default privacy budget of 10 by instead submitting the query:
```sql
CREATE TABLE users_DP (
    ...,
    PII_name VARCHAR(64) NOT NULL,
    ...,
);
```
or be assigned an arbitrary privacy budget of `X`, say 15, by submitting the query:
```sql
CREATE TABLE users_DP_budget15 (
    ...,
    PII_name VARCHAR(64) NOT NULL,
    ...,
);
```

When a budgeted table is created, Pelton creates an additional meta-table called a budget tracker. The name of the budget tracker table is the name of the budgeted table suffixed with "`_BudgetTracker`". The tracker for the above-created `users_DP_budget15` would be:
```sql
CREATE TABLE users_DP_budget15_BudgetTracker (
    id INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
    e INT,
    v VARCHAR(255),
    q VARCHAR(255)
);
```
Every time a differentially private query (meaning a new differentially private view is created) is made against `users_DP_budget15`, a new row is added to `users_DP_budget15_BudgetTracker`, with `e` representing the epsilon parameter for the query, `v` as the created view name, and `q` as the aggregate query. At any point, the `e` column can be summed to determine the expenditure of the privacy budget of a table, and the remaining budget can be determined by subtracting such value from the budget encoded in the original table name.

### Creating a Differentially Private View

At present, Pelton does not support direct aggregate queries against existing tables and materialized views (i.e. if `users` is a sharded table with a column `age`, running `SELECT SUM(age) FROM users;` will crash the system). Instead, these queries must be embedded within named SQL views, which Pelton keeps updated as component sharded tables
At present, Pelton does not support direct aggregate queries against existing tables and materialized views (i.e. if `users` is a sharded table with a column `age`, running `SELECT SUM(age) FROM users;` will crash the system). Instead, these queries must be embedded within named SQL views, which Pelton keeps updated as source sharded tables are updated. For example:
```sql
CREATE VIEW user_sum AS '"SELECT SUM(age) FROM users"';
SELECT * FROM user_sum;
```

Typically, this will create a view that produces the expected outcome were such a query run against a standard relational database, although Pelton uses custom aggregate operators under the hood in anticipation that these aggregates will need to be regularly updated as source information changes. While this makes sense given Pelton's usual intended use-case, the notion of re-computing differentially private statistics with every source data change can quickly deplete the privacy budgets of source tables. Using differentially private algorithms in this way is unusual in cases outside of testing the differentially private algorithms themselves.

Instead, differentially private views in Pelton are better thought of as data "releases." Once a DP View is created and the DP statistic embedded within is calculated, it is removed from Pelton's dataflow, so subsequent changes to source tables (including deletions or users) will not impact its values. This may jeopardize GDPR-compliance guarantees Pelton is usually able to offer: if DP epsilon values are set very high, DP algorithms may add a minimum of noise, so even if a user is otherwise removed from Pelton they may be distinguishable in the unaltered DP views. The same is true if privacy budgets for source tables are set too high. Ultimately, the guarantees of privacy this modification of the system can offer are still dependent on the developer's ability to determine and set reasonable DP parameters.

Much like creating a budgeted table, creation of a DP View is signaled to Pelton by appending a suffix to the view name:
- The view name begins with an arbitrary, valid view name.
- Following this arbitrary prefix, the DP view name must include "`_DP`", which signals that the view is to be handled with as differentially private.
- Following this, a number of parameters can be set, which will be passed to the underlying differential privacy library algorithms. These values are optional, should be delimited by the "`_`" character, and must be passed in the following order if included:
    1. "`eX`" indicates the epsilon value of `X` will be used, where `X` is a non-negative integer. If this parameter is omitted, a default value of `1` will be assumed.
    2. "`dX`" indicates the delta value of `X` will be used, where `X` is a non-negative integer. If this parameter is omitted, a default value of `0` will be assumed.
    3. "`lX`" indicates a lower bound value of `X` will be used, where `X` is a non-negative integer. The lower bound is only useful in the case that the view is computing a sum; otherwise this value is unused. If a sum is being computed but the value is not passed, the [DP library's default, to estimate the bounds on suffciently large input datasets](https://github.com/google/differential-privacy/blob/main/cc/docs/algorithms/approx-bounds.md), will be used.
    4. "`hX`" indicates an upper bound value of `X` will be used, where `X` is a non-negative integer. The upper bound is only useful in the case that the view is computing a sum; otherwise this value is unused. If a sum is being computed but the value is not passed, the [DP library's default, to estimate the bounds on suffciently large input datasets](https://github.com/google/differential-privacy/blob/main/cc/docs/algorithms/approx-bounds.md), will be used.

#### Supported Aggregate Operations

At present, Pelton itself supports two aggregate operators: `COUNT` and `SUM`. These are supported in differentially private queries as well.

The `COUNT` operator uses the DP library's [`Count` algorithm](https://github.com/google/differential-privacy/blob/main/cc/docs/algorithms/count.md).

The `SUM` operator uses the DP library's [`BoundedSum` algorithm](https://github.com/google/differential-privacy/blob/main/cc/docs/algorithms/bounded-sum.md). As a [bounded algorithm](https://github.com/google/differential-privacy/blob/main/cc/docs/algorithms/bounded-algorithm.md), this algorithm requires upper and lower bounds that are used as assumed absolute maxima and minima of raw values when determining the amount of noise to add. If these values are not supplied as DP parameters, then the algorithms will fall back to the default library behavior of using [`ApproxBounds`](https://github.com/google/differential-privacy/blob/main/cc/docs/algorithms/approx-bounds.md) to attempt to pick these values (see this documentation for more info on choosing bounds). Note: `ApproxBounds` has a number of default parameters of its own that are not modifiable through this system. In the case of particularly small source datasets, the default/recommended settings may mean this algorithm is unable to pick reasonable bounds, and an error may thrown (and propagated through Pelton, causing a crash).

## Limitations/Areas for Improvement

There are a number of limitations in the current implementation that are good paths for future improvement. In no particular order:
- **Supported Operations:** Google's Differential Privacy library already includes means of calculating mean, variance, standard deviation, and quantiles. The adaptation of these into Pelton can be trivial. The implementation of differentially private `SUM` and `COUNT` are found in `pelton::dataflow::ProcssDP()` in [`aggregate.cc`](../dataflow/ops/aggregate.cc). Source records are passed to `ProcssDP()` and iteratively added to the differentially private calculation through the library's [`AddEntry()` API](https://github.com/google/differential-privacy/blob/bf0abf446b2f9d625a824bf14a7e3a6b6ac2a3e4/cc/algorithms/algorithm.h#L71); few changes are needed between operator implementations. These *can* be added at present, but non-differentially private versions of these aggregates are not as trivial to implement (typically, operators in Pelton receive atomic additions and deletions of data which are used to update the statistic. For `SUM` and `COUNT` this is just adding to/subtracting from the existing value of a statistic, but other aggregates are much harder to keep "running" values of). Because DP operators are implemented separately and are only run once per view, they do not need to address concerns. Implementation at this point means there would not be parity of supported operators between normal and DP views.
- **Expanded support for non-integer DP parameters:** currently, the table and view name parsing functions in [`dp_util.cc`](./dp_util.cc) *can* parse decimal values for epsilon, delta, and bounds. Because of limitations in allowed names, the decimal character "." must be encoded as the exclamation mark character "!" (so "`..._DP_e4!2`" will be assumed to mean an epsilon value of `4.2`). Currently, however, Pelton is unable to store non-integer values. Because expenditures of budgeted tables are recorded in tables within Pelton itself, the current implementation necessarily casts decimal values to integers before recording in the tracker. So while it is possible to execute DP queries with non-integer parameters, non-integer epsilon values are not recommended because they are not precisely tracked, and non-integer bound value are nonsensical because source information cannot currently be non-integer.
- **Support for DP Queries on sharded, non-data subject tables with differing shard sizes:** at present, differentially private queries can be made against any table, regardless of wether it is marked as budgeted or even as a data subject table. However, it assumes that each row represents exactly one data subject, and each data subject is referenced in the table by only one data subject. This is sensible in the `user` table examples mentioned above, but quickly falls apart if the query is made against a table which has a one-to-many relationship with that `user` table. The result of a DP query will still be noised, but this is only event-DP, which is weaker from a privacy-protection perspective than full user-DP. There are two immediately-obvious paths for improvement:
    1. Allow for direct manipulation of [the library's `max_partitions` and `max_contributions` paramters.](https://github.com/google/differential-privacy/blob/main/cc/docs/algorithms/algorithm.md#construction) These parameters are exposed by the differential privacy library to account for this use case, where an individual may have contributed multiple data points to a set against which a differentially private aggregation is being run. By further enhancing the view name parsing rules (see [Creating a Differentially Private View](#creating-a-differentially-private-view) and [`dp_util.cc`](./dp_util.cc)), these parameters could be specified by the user, just as they can specify bound values and epsilon and delta parameters.
        - This is relatively straightforward to implement, as the `DPOptions` class can be easily extended to support these additional values.
        - However, this involves further complicating the amount of information that must be encoded in view names, which is a hacky approach to begin with.
        - Additionally, to be accurate, a user would first have to run non-DP queries to determine the maximum frequency with which a data subject's data appears in the source table, which contradicts the above-stated benefit of simplifying the use of differential privacy.
    2. Automatically tabulate the above-mentioned parameters on DP view creation.
        - This is more complicated to implement, because the most straightforward extension of the current DP implementation would require backwards-traversal up the dataflow to determine source data subject table limitations. To do this, either an additional structure must be maintained by the dataflow state to allow for efficient reverse-lookup of parent views, or updates to all existing data subject tables must be simulated to determine which dataflow paths lead to the immediate parents of the DP view being created. This approach was attempted but abandoned because of implementation difficulties. Unless there are a truly extraordinary number of tables and views being created, however, it is unlikely that these additions would adversely impact performance or space usage.
- **Improved Parser:** when Pelton's parser is improved to more richly support annotations, DP annotations can be handled similarly, so as to rely less on the names of tables and views. This may require additional changes, such as a mechanism to lookup budget and epsilon values based on table or view name (as such data would no longer be encoded in such names).
- **Different Differential Privacy Library:** this work originally aimed to use [OpenDP](https://opendp.org)'s [core library](https://github.com/opendp/opendp). Because the mathematics of differential privacy can be complex and prone to implementation errors, and because there is no distinct advantage to a custom implementation of common algorithms for Pelton, it makes sense to reuse these efforts. OpenDP's core library is a relatively large and mature collection of well-documented data structures and algorithms, most of which are vetted by a community process and are justified by [proof](https://docs.opendp.org/en/stable/developer/code-structure.html#proof)s. While this implementation of differentially private views in Pelton was originally intended to rely on OpenDP's core library, this library required a version of [OpenSSL](https://www.openssl.org) which caused issues with Pelton's build system which seemed difficult to resolve. Instead, Google's C++ Differential Privacy library is used. While sufficient for current capabilities, the OpenDP project does offer stronger verification guarantees and potentially a greater number of features for future expansion.

## Testing

Testing is complicated by the fact that Google's Differential Privacy library adds random noise to each differentially private query, and does not appear to provide a mechanism for fixing a seed for its internal randomness generation, meaning to write consistent E2E tests modifications to the library codebase would be needed. A "real-world" example modeling a potential use for a micromobility company using Pelton to track usage of their shared bikes or scooters, which has both nontrivial GDPR compliance concerns and a need to privately share aggregate statistics with researchers and officials. Unfortunately, while the partial OpenDP implementation was configured for this test, the current library's lack of support for fixing a seed means such a complex test is difficult to write tests for or inspect manually (although both OpenDP's and Google's DP implementations are assumed to be consistent with the definition of DP themselves). A sample of the test schema, along with queries to compare DP output to non-DP aggregates:
```sql
CREATE TABLE users ( \
    id INT PRIMARY KEY NOT NULL, \
    PII_name VARCHAR(64) NOT NULL, \
    email VARCHAR(64) NOT NULL, \
    age INT \
);
CREATE TABLE verification ( \
    id INT PRIMARY KEY NOT NULL, \
    OWNER_uid INT NOT NULL REFERENCES users(id), \
    license_number INT \
);
CREATE TABLE subscriptions ( \
    id INT PRIMARY KEY NOT NULL, \
    OWNER_uid INT NOT NULL REFERENCES users(id), \
    type INT NOT NULL, \
    start_date DATETIME, \
    cancel_date DATETIME, \
    status INT NOT NULL \
);
CREATE TABLE usage ( \
    id INT PRIMARY KEY NOT NULL, \
    OWNER_uid INT NOT NULL REFERENCES users(id), \
    IP_addr VARCHAR(32), \
    device_id VARCHAR(32), \
    ad_id VARCHAR(32), \
    os VARCHAR(32), \
    location VARCHAR(32), \
    battery INT \
);
CREATE TABLE vehicles ( \
    id INT PRIMARY KEY NOT NULL, \
    type INT, \
    state INT, \
    registered_date DATETIME, \
    location VARCHAR(32) \
);
CREATE TABLE trips ( \
    id INT PRIMARY KEY NOT NULL, \
    OWNER_uid INT NOT NULL REFERENCES users(id), \
    duration INT, \
    distance INT, \
    start_dt DATETIME, \
    end_dt DATETIME, \
    status INT, \
    vehicle INT REFERENCES vehicles(id) \
);

CREATE VIEW sub_count AS '"SELECT COUNT(id) FROM subscriptions WHERE status != 0"';
CREATE VIEW sub_count_DP_e1 AS '"SELECT COUNT(id) FROM subscriptions WHERE status != 0"';

SELECT * FROM sub_count;
SELECT * FROM sub_count_DP_e1;

CREATE VIEW trip_time_total AS '"SELECT SUM(duration) FROM trips"';
CREATE VIEW trip_time_total_DP_e5_d1_l0_h120 AS '"SELECT SUM(duration) FROM trips"';

SELECT * FROM trip_time_total;
SELECT * FROM trip_time_total_DP_e5_d1_l0_h120;
```

Instead, [`dp_test.sql`](./dp_test.sql) is provided to demonstrate the core functionality. It creates a simple budgeted data subject user table, inserts values into that table, and then creates differentially private views until that budget is exhausted. Running the contents against the Pelton proxy should produce first the sum of `448` (this is a normal, non-DP sum so should be constant) followed by four similar sums, but with noise added. Then, a view of the budget tracker should be returned by Pelton, followed by an error. The log of the proxy will report that the budget has been exhausted. For example:
```
+------+
| Sum  |
+------+
|  448 |
+------+
1 row in set (0.001 sec)

Query OK, 0 rows affected (0.017 sec)

+------+
| Sum  |
+------+
|  360 |
+------+
1 row in set (0.000 sec)

Query OK, 0 rows affected (0.013 sec)

+------+
| Sum  |
+------+
|  354 |
+------+
1 row in set (0.001 sec)

Query OK, 0 rows affected (0.014 sec)

+------+
| Sum  |
+------+
|  501 |
+------+
1 row in set (0.000 sec)

Query OK, 0 rows affected (0.015 sec)

+------+
| Sum  |
+------+
|  433 |
+------+
1 row in set (0.000 sec)

+------+------+------------------------+----------------------------------------+
| id   | e    | v                      | q                                      |
+------+------+------------------------+----------------------------------------+
|    1 |    1 | `v2_DP_e1_l0_h99!5`    | SELECT SUM(age) FROM users_DP_budget20 |
|    2 |    2 | `v3_DP_e2!8_l0_h99!5`  | SELECT SUM(age) FROM users_DP_budget20 |
|    3 |    4 | `v4_DP_e4_l0_h99!5`    | SELECT SUM(age) FROM users_DP_budget20 |
|    4 |   10 | `v5_DP_e10!2_l0_h99!5` | SELECT SUM(age) FROM users_DP_budget20 |
+------+------+------------------------+----------------------------------------+
4 rows in set (0.001 sec)

ERROR 2013 (HY000) at line 32 in file: '/home/pelton/pelton/dp/dp_test1.sql': Lost connection to server during query
```

With a logged error:
```
Privacy Budget for budgeted table users_DP_budget20 has been depleted from 20.000000 to 3, which is less than the epsilon value of 4.000000 specified for differentially private release view `v6_DP_e4_l0_h99!5` with query 'SELECT SUM(age) FROM users_DP_budget20'. A history of budget expenditures is available in table users_DP_budget20_BudgetTracker.
```

Note that though the second and fourth view names were originally specified as doubles, but because of the Pelton limitations mentioned [above](#limitationsareas-for-improvement), these are cast to integers before being recorded.
