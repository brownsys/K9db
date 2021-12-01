extern crate sqlparser;
extern crate regex;
#[macro_use]
extern crate lazy_static;

use sqlparser::ast::{ Statement, Expr };
use std::collections::HashMap;
use std::collections::HashSet;
use std::rc::Rc;

type Q = Rc<Statement>;

struct Categories {
    map: HashMap<String, HashSet<Q>>,
}

struct Categorizer<'a> {
    cats: &'a mut Categories,
    on_q: &'a Q,
}

impl <'a> Categorizer<'a> {
    fn add_move(&mut self, s: String) {
        self.cats.map.entry(s).or_insert_with(HashSet::new).insert(self.on_q.clone());
    }
    fn add(&mut self, s: &str) {
        self.add_move(s.to_string())
    }
}

trait Categorize {
    fn categorize<'a>(&self, _: &mut Categorizer<'a>) {}
}



impl Categories {
    fn new() -> Self {
        Categories { map: HashMap::new() }
    }
    fn categorize_query(&mut self, s: Statement) {
        let rc = Rc::new(s);
        rc.categorize(&mut Categorizer { cats: self, on_q: &rc });
    }
}

fn is_point_lookup(e : &Expr) -> bool {
    match e {
        Expr::BinaryOp {op:sqlparser::ast::BinaryOperator::Eq, ..} => true,
        _ => false,
    }
}

impl Categorize for Statement {
    fn categorize(&self, cats: &mut Categorizer) {
        use Statement::*;
        match self {
            Query(sel) => sel.categorize(cats),
            SetVariable{..} => cats.add("SET"),
            Update{assignments, selection, ..} => {
                if let Some(e) = &selection {
                    if !is_point_lookup(e) {
                        cats.add("UPDATE WHERE [complex]");
                    } else {
                        cats.add("UPDATE [point lookup]");
                    }
                    e.categorize(cats);
                } else {
                    cats.add("UPDATE");
                }
                assignments.iter().for_each(|a| a.value.categorize(cats))
            }
            Delete{selection, ..} => {
                if let Some(e) = &selection {
                    if !is_point_lookup(e) {
                        cats.add("DELETE WHERE [complex]");
                    } else {
                        cats.add("DELETE [point lookup]");
                    }
                    e.categorize(cats);
                } else {
                    cats.add("DELETE");
                }
            }
            Insert{or, overwrite, source, partitioned, after_columns, table, ..} => {
                cats.add("INSERT");
                assert!(or.is_none());
                assert!(!overwrite);
                source.categorize(cats);
                assert!(partitioned.is_none());
                assert!(after_columns.is_empty(), "{:?} {}", after_columns, self);
                assert!(!table);
            }
            Commit{chain} => assert!(!chain),
            ShowVariable{..} => cats.add("SHOW VARIABLES"),
            StartTransaction{modes} => {
                cats.add("TRANSACTION");
                modes.iter().for_each(|m| {
                    cats.add_move(format!("{}", m))
                });
            }
            _ => unimplemented!("{}", self),
        }
    }
}

impl Categorize for sqlparser::ast::Query {
    fn categorize(&self, cats: &mut Categorizer) {
        if let Some(w) = &self.with { unimplemented!("{}", w) }
        self.body.categorize(cats);
        if !self.order_by.is_empty() { cats.add("ORDER BY"); }
        if self.limit.is_some() { cats.add("LIMIT"); }
        if self.offset.is_some() { cats.add("OFFSET"); }
        if self.fetch.is_some() { cats.add("FETCH"); }
    }
}

impl Categorize for sqlparser::ast::SetExpr {
    fn categorize(&self, cats: &mut Categorizer) {
        use sqlparser::ast::SetExpr::{*};
        let self_cat = match self {
            Select(s) => { s.categorize(cats); None }
            Query(q) => { q.categorize(cats); Some("nested query") }
            SetOperation {left, right, ..} => {
                left.categorize(cats);
                right.categorize(cats);
                Some("set ops")
            }
            Values(_) => Some("values"),
            Insert(i) => { i.categorize(cats); Some("nested INSERT") }
        };
        self_cat.map(|c| cats.add(c));
    }
}

impl Categorize for sqlparser::ast::Select {
    fn categorize(&self, cats: &mut Categorizer) {
        if self.distinct { cats.add("DISTINCT") }
        if self.top.is_some() { cats.add("top") }
        self.from.iter().for_each(|t| {
            t.relation.categorize(cats);
            t.joins.iter().for_each(|j| j.categorize(cats))
        });
        assert!(self.lateral_views.is_empty());
        let select_is_simple = if let Some(s) = &self.selection {
            s.categorize(cats);
            is_point_lookup(s)
        } else {
            true
        };
        cats.add(if select_is_simple { "SELECT [point lookup]" } else {"SELECT WHERE [complex]" });
        if !self.group_by.is_empty() { cats.add("GROUP BY") }
        self.group_by.iter().for_each(|g| g.categorize(cats) );
        assert!(self.cluster_by.is_empty());
        assert!(self.distribute_by.is_empty());

        if !self.sort_by.is_empty() { cats.add("SORT BY") }
        self.sort_by.iter().for_each(|g| g.categorize(cats) );

        if let Some(h) = &self.having {
            cats.add("HAVING");
            h.categorize(cats);
        }
    }
}

impl Categorize for sqlparser::ast::Expr {
    fn categorize(&self, cats: &mut Categorizer) {
        use sqlparser::ast::Expr::*;
        match self {
            Wildcard | QualifiedWildcard(_) => Some("*"),
            IsDistinctFrom(e1, e2) | IsNotDistinctFrom(e1, e2) => { e1.categorize(cats); e2.categorize(cats); Some("IS DISTINCT FROM") },
            InList{expr, list, ..} => {
                expr.categorize(cats);
                list.iter().for_each(|e| e.categorize(cats));
                Some("IN [list]")
            },
            InSubquery{expr, subquery, ..} => {
                expr.categorize(cats);
                subquery.categorize(cats);
                Some("IN [subquery]")
            },
            Between {expr, low, high, ..} => {
                expr.categorize(cats);
                low.categorize(cats);
                high.categorize(cats);
                Some("between")
            },
            UnaryOp{op, expr} => {
                expr.categorize(cats);
                use sqlparser::ast::UnaryOperator::*;
                match op {
                    Plus | Minus | Not => None,
                    other => panic!("Unsupported unary operator {}", other),
                }
            }
            BinaryOp {left, op, right} => {
                left.categorize(cats);
                right.categorize(cats);
                use sqlparser::ast::BinaryOperator::*;
                match op {
                    StringConcat => Some("str concat"),
                    Like | NotLike => Some("operator `like`"),
                    Plus | Minus | Multiply | Divide | Modulo
                        | Gt | Lt | GtEq | LtEq | Spaceship
                        | Eq | NotEq | And | Or | Xor => None,
                    _ => panic!("Unsupported binary operator {}", op),
                }
            }
            Case{operand, conditions, results, else_result} => {
                operand.as_ref().map(|o| o.categorize(cats));
                conditions.iter().for_each(|c| c.categorize(cats));
                results.iter().for_each(|r| r.categorize(cats));
                else_result.as_ref().map(|r| r.categorize(cats));
                Some("CASE")
            }
            Cast {expr, ..}
            | TryCast{ expr, ..}
            | Nested(expr)
            => { expr.categorize(cats); None}
            Extract { expr, ..}
            => { expr.categorize(cats); Some("extract")}
            Collate {expr, ..}
            => { expr.categorize(cats); Some("collate")}
            Subquery(q) => { q.categorize(cats); Some("subquery")}
            Value(_) | Identifier(_) | CompoundIdentifier(_)
                | IsNull(_) | IsNotNull(_)  => None,
            Function(f) => { f.categorize(cats); None },
            other => panic!("Expression {} is not supported", other),
        }.map(|s| cats.add(s));
    }
}

impl Categorize for sqlparser::ast::Function {
    fn categorize(&self, cats: &mut Categorizer) {
        cats.add_move(format!("function({}){}", self.name, if self.distinct { "(distinct)" } else { "" }));
        assert!(self.over.is_none(), "{}", self.over.as_ref().unwrap());
    }
}

impl Categorize for sqlparser::ast::TableFactor {
    fn categorize(&self, _: &mut Categorizer) {
        use sqlparser::ast::TableFactor::*;
        match self {
            Table {args, with_hints, ..} => assert!(args.is_empty() && with_hints.is_empty(), "{}", self),
            other => unimplemented!("{}", other)
        }
    }
}

impl Categorize for sqlparser::ast::JoinConstraint {
    fn categorize(&self, cats: &mut Categorizer) {
        use sqlparser::ast::JoinConstraint::*;
        let self_cat = match self {
            On(e) => { e.categorize(cats); "join ON" },
            Using(_) => "join USING",
            Natural => "NATURAL join",
            None => "unspecified join",
        };
        cats.add(self_cat);
    }
}

impl Categorize for sqlparser::ast::Join {
    fn categorize(&self, cats: &mut Categorizer) {
        use sqlparser::ast::JoinOperator::*;
        let join_t = match &self.join_operator {
            Inner(_) => "INNER",
            LeftOuter(_) => "LEFT OUTER",
            RightOuter(_) => "RIGHT OUTER",
            FullOuter(_) => "FULL OUTER",
            CrossJoin => "CROSS",
            other => unimplemented!("{} ({:?})", self, other),
        };
        cats.add_move(format!("{} join", join_t));
        match &self.join_operator {
            LeftOuter(constr) | Inner(constr) | RightOuter(constr)
                | FullOuter(constr) => constr.categorize(cats),
            _ => (),
        }
    }
}

lazy_static! {
    static ref MYSQL_QUERY_LOG_REGEX : regex::Regex = regex::Regex::new(r"(?m)^\s+(\d+)\s(\w+)\t(.*?$(\n\t{4}.*?$)*)").expect("Regex failed to compile");
}

use std::io::{Read, Write};

fn get_inp() -> String {
    let mut query = String::new();
    use std::io::Read;
    std::io::stdin().read_to_string(&mut query).unwrap();
    query
}

fn main2() {
    let mut buf = String::new();
    let files = std::env::args().skip(1).collect::<Vec<_>>();
    println!("Handling {:?}", files);
    let query_vecs : Vec<Vec<Statement>> = files.iter().map(|a| {
        buf.clear();
        println!("Parsing {}", a);
        std::fs::File::open(a).unwrap().read_to_string(&mut buf).unwrap();
        MYSQL_QUERY_LOG_REGEX.captures_iter(&buf).filter_map(|mtch| {
            let q = mtch.get(3).expect("query not found").as_str();
            sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::MySqlDialect {}, q).ok().and_then(|v| {
                v.into_iter().next()
            })
        }).collect::<Vec<_>>()
    }).collect::<Vec<_>>();
    println!("Parsing done, calculating intersections");
    let common_queries : HashSet<&Statement> = query_vecs.iter().map(|q| q.iter().collect::<HashSet<_>>()).reduce(|q1, q2| q1.intersection(&q2).map(|i| *i).collect()).expect("no queries");
    for (file, qs) in files.iter().zip(query_vecs.iter()) {
        let fname = format!("{}-dedup", file);
        println!("Writing {}", &fname);
        let mut f = std::fs::File::create(&fname).unwrap();
        for q in qs.iter().filter(|q| !common_queries.contains(q)) {
            writeln!(f, "{}", q).unwrap();
        }
    }
}

fn main1() {
    let query = get_inp();
    //let q2 : String = query.chars().skip_while(|c| *c != '\n').skip(1).skip_while(|c| *c != '\n').skip(1).collect();

    //let offsId = q2.chars().enumerate().find_map(|(i, c)| if c == 'I' { Some(i) } else { None }).expect("There should have been an I here");

    //eprintln!("Found 'Id' offset of {}", offsId);

    //let rgx = regex::Regex::new(&format!("^(?m).{{{n}}}(\\d+) (\\w+) +(\\S.*?$( {{{m},}}\\S.*?$)*)", n=offsId, m=offsId + 3)).unwrap();
    println!("Parsing queries ...");
    let mtch_it = MYSQL_QUERY_LOG_REGEX.captures_iter(&query).map(|mtch| {
        let q_id = mtch.get(1).expect("id not found").as_str().parse::<u32>();
        let q_type =  mtch.get(2).expect("type not found").as_str();
        let q = mtch.get(3).expect("query not found").as_str();
        (q_id,
         q_type,
         q,
         if q.chars().all(|c| c.is_whitespace()) {
             None
         } else if q_type == "Query" {
             let res = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::MySqlDialect {}, q);
             if res.is_err() {
                 if q.starts_with("BEGIN TRANSACTION") || q.starts_with("START TRANSACTION") {
                     Some(Ok(vec![Statement::StartTransaction{modes:vec![]}]))
                 } else if q.starts_with("SET SESSION") {
                     Some(Ok(vec![Statement::SetVariable{local:false,hivevar:false,variable:sqlparser::ast::Ident::new("dummy"),value:vec![sqlparser::ast::SetVariableValue::Literal(sqlparser::ast::Value::Null)]}]))
                 } else {
                     eprintln!("{}", q);
                     Some(res)
                 }
             } else {
                 Some(res)
             }
         } else {
             if q_type != "Connect" {
                 eprintln!("{} : {}", q_type, q);
             }
             None
         })
    });
    let matches = mtch_it.collect::<Vec<_>>();
    let num_mtch = matches.len();
    let num_matches_success =
        matches.iter().filter(|m| m.3.as_ref().map_or(false, |r| r.is_ok())).count();
    let num_matches_failure =
        matches.iter().filter(|m| m.3.as_ref().map_or(false, |r| r.is_err())).count();
    let num_matches_other =
        matches.iter().filter(|m| m.3.is_none()).count();
    let queries : HashSet<_> = matches.into_iter().filter_map(|(_, _, q_str, r)|
        r.and_then(|q|
            q.map_or_else(
                |err| {
                    None
                },
                |asts| {
                    assert!(asts.len() == 1);
                    let ast = asts.into_iter().next().unwrap();
                    Some(ast)
                }
            )
        )
    ).collect();
    println!(
        "Found {} log entries. {} queries parsed, {} unique queries, {} parse failures and {} non-query entries.",
        num_mtch,
        num_matches_success,
        queries.len(),
        num_matches_failure,
        num_matches_other,
    );
    let mut cats = Categories::new();
    for ast in queries.into_iter() {
        cats.categorize_query(ast);
    }

    let mut catv = cats.map.into_iter().collect::<Vec<_>>();
    catv.sort_by_key(|(_, q)| q.len());
    let count_width = catv.last().map(|c| ( c.1.len() as f64 ).log10().ceil() as usize).unwrap_or(0);
    let fltr = vec!['[', ']', '`'];
    let feature_strs = catv.iter().map(|c| format!("[{}](#{})", c.0, c.0.to_lowercase().replace(" ", "-").chars().filter(|c| !fltr.contains(c)).collect::<String>())).collect::<Vec<_>>();

    let feature_width = feature_strs.iter().map(|c| c.len()).max().unwrap_or(0);

    println!("## Overview");
    println!("");
    println!("|{:f_width$} | {:>c_width$}|", "Feature", "# Q", c_width=count_width, f_width=feature_width);
    println!("|{:-<f_width$}-|-{:-<c_width$}|", "-", "-", c_width=count_width, f_width=feature_width);
    for (feature, ( _, queries )) in feature_strs.into_iter().rev().zip(catv.iter().rev()) {
        println!("|{:f_width$} | {:>c_width$}|", feature, queries.len(), c_width=count_width, f_width=feature_width);
    }
    println!("");
    println!("## Feature Detail");

    for (feature, queries) in catv.iter().rev() {
        println!("### `{}`", feature);
        println!("");
        println!("Found in {} unique queries.", queries.len());
        println!("");
        queries.iter().take(10)
            .for_each(|q|
                      println!("- ``` {} ```", q)
                      );

    }
}


fn main() {
    main2();
}
