use sqlparser::{dialect::DuckDbDialect, tokenizer::Token};
use tui::{none, Color, Style};

/// SQL highlighter
pub struct Highlighter {
    styles: Vec<(u64, Style)>,
    idx: usize,
}

impl Highlighter {
    /// Create a new highlighter fir the given query
    pub fn load(query: &str) -> Self {
        let mut tmp = Self {
            styles: vec![(0, tui::none())],
            idx: 0,
        };
        for token in sqlparser::tokenizer::Tokenizer::new(&DuckDbDialect, query)
            .tokenize_with_location()
            .unwrap_or_default()
        {
            tmp.styles.push((
                token.location.column - 1,
                match token.token {
                    Token::Mul
                    | Token::Plus
                    | Token::Minus
                    | Token::Div
                    | Token::DoubleEq
                    | Token::Eq
                    | Token::Neq
                    | Token::Lt
                    | Token::Gt
                    | Token::LtEq
                    | Token::GtEq
                    | Token::DuckIntDiv
                    | Token::Mod => none().fg(Color::Yellow),
                    Token::Number(_, _) => none().fg(Color::DarkMagenta),
                    Token::SingleQuotedString(_) | Token::DoubleQuotedString(_) => {
                        none().fg(Color::Green).italic()
                    }
                    Token::Word(mut w) => {
                        w.value.make_ascii_lowercase();
                        if w.value == "current"
                            || DUCKDB_FUNCTIONS.binary_search(&w.value.as_str()).is_ok()
                        {
                            none().fg(Color::Cyan)
                        } else if DUCKDB_KEYWORDS.binary_search(&w.value.as_str()).is_ok() {
                            none().fg(Color::DarkBlue)
                        } else {
                            none()
                        }
                    }
                    _ => none(),
                },
            ))
        }
        tmp.styles.push((u64::MAX, none()));
        tmp
    }

    /// Return the style at the given position
    pub fn style(&mut self, pos: u64) -> Style {
        // Move left
        while pos < self.styles[self.idx].0 {
            self.idx -= 1;
        }

        // Move right
        while self.idx < self.styles.len() && pos >= self.styles[self.idx + 1].0 {
            self.idx += 1;
        }

        self.styles[self.idx].1
    }
}

const DUCKDB_KEYWORDS: &[&str] = &[
    "abort",
    "absolute",
    "access",
    "action",
    "add",
    "admin",
    "after",
    "aggregate",
    "all",
    "also",
    "alter",
    "always",
    "analyse",
    "analyze",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "assertion",
    "assignment",
    "asymmetric",
    "at",
    "attach",
    "attribute",
    "backward",
    "before",
    "begin",
    "both",
    "by",
    "cache",
    "call",
    "called",
    "cascade",
    "cascaded",
    "case",
    "cast",
    "catalog",
    "chain",
    "characteristics",
    "check",
    "checkpoint",
    "class",
    "close",
    "cluster",
    "collate",
    "column",
    "comment",
    "comments",
    "commit",
    "committed",
    "compression",
    "configuration",
    "conflict",
    "connection",
    "constraint",
    "constraints",
    "content",
    "continue",
    "conversion",
    "copy",
    "cost",
    "create",
    "csv",
    "cube",
    "current",
    "cursor",
    "cycle",
    "data",
    "database",
    "day",
    "days",
    "deallocate",
    "declare",
    "default",
    "defaults",
    "deferrable",
    "deferred",
    "definer",
    "delete",
    "delimiter",
    "delimiters",
    "depends",
    "desc",
    "describe",
    "detach",
    "dictionary",
    "disable",
    "discard",
    "distinct",
    "do",
    "document",
    "domain",
    "double",
    "drop",
    "each",
    "else",
    "enable",
    "encoding",
    "encrypted",
    "end",
    "enum",
    "escape",
    "event",
    "except",
    "exclude",
    "excluding",
    "exclusive",
    "execute",
    "explain",
    "export",
    "export_state",
    "extension",
    "external",
    "false",
    "family",
    "fetch",
    "filter",
    "first",
    "following",
    "for",
    "force",
    "foreign",
    "forward",
    "from",
    "function",
    "functions",
    "global",
    "grant",
    "granted",
    "group",
    "handler",
    "having",
    "header",
    "hold",
    "hour",
    "hours",
    "identity",
    "if",
    "ignore",
    "immediate",
    "immutable",
    "implicit",
    "import",
    "in",
    "include",
    "including",
    "increment",
    "index",
    "indexes",
    "inherit",
    "inherits",
    "initially",
    "inline",
    "input",
    "insensitive",
    "insert",
    "install",
    "instead",
    "intersect",
    "into",
    "invoker",
    "isolation",
    "json",
    "key",
    "label",
    "language",
    "large",
    "last",
    "lateral",
    "leading",
    "leakproof",
    "level",
    "limit",
    "listen",
    "load",
    "local",
    "location",
    "lock",
    "locked",
    "logged",
    "macro",
    "mapping",
    "match",
    "materialized",
    "maxvalue",
    "method",
    "microsecond",
    "microseconds",
    "millisecond",
    "milliseconds",
    "minute",
    "minutes",
    "minvalue",
    "mode",
    "month",
    "months",
    "move",
    "name",
    "names",
    "new",
    "next",
    "no",
    "not",
    "nothing",
    "notify",
    "nowait",
    "null",
    "nulls",
    "object",
    "of",
    "off",
    "offset",
    "oids",
    "old",
    "on",
    "only",
    "operator",
    "option",
    "options",
    "or",
    "order",
    "ordinality",
    "over",
    "overriding",
    "owned",
    "owner",
    "parallel",
    "parser",
    "partial",
    "partition",
    "passing",
    "password",
    "percent",
    "pivot",
    "pivot_longer",
    "pivot_wider",
    "placing",
    "plans",
    "policy",
    "pragma",
    "preceding",
    "prepare",
    "prepared",
    "preserve",
    "primary",
    "prior",
    "privileges",
    "procedural",
    "procedure",
    "program",
    "publication",
    "qualify",
    "quote",
    "range",
    "read",
    "reassign",
    "recheck",
    "recursive",
    "ref",
    "references",
    "referencing",
    "refresh",
    "reindex",
    "relative",
    "release",
    "rename",
    "repeatable",
    "replace",
    "replica",
    "reset",
    "respect",
    "restart",
    "restrict",
    "returning",
    "returns",
    "revoke",
    "role",
    "rollback",
    "rollup",
    "rows",
    "rule",
    "sample",
    "savepoint",
    "schema",
    "schemas",
    "scroll",
    "search",
    "second",
    "seconds",
    "security",
    "select",
    "sequence",
    "sequences",
    "serializable",
    "server",
    "session",
    "set",
    "sets",
    "share",
    "show",
    "simple",
    "skip",
    "snapshot",
    "some",
    "sql",
    "stable",
    "standalone",
    "start",
    "statement",
    "statistics",
    "stdin",
    "stdout",
    "storage",
    "stored",
    "strict",
    "strip",
    "subscription",
    "summarize",
    "symmetric",
    "sysid",
    "system",
    "table",
    "tables",
    "tablespace",
    "temp",
    "template",
    "temporary",
    "text",
    "then",
    "to",
    "trailing",
    "transaction",
    "transform",
    "trigger",
    "true",
    "truncate",
    "trusted",
    "type",
    "types",
    "unbounded",
    "uncommitted",
    "unencrypted",
    "union",
    "unique",
    "unknown",
    "unlisten",
    "unlogged",
    "unpivot",
    "until",
    "update",
    "use",
    "user",
    "using",
    "vacuum",
    "valid",
    "validate",
    "validator",
    "value",
    "variadic",
    "varying",
    "version",
    "view",
    "views",
    "virtual",
    "volatile",
    "when",
    "where",
    "whitespace",
    "window",
    "with",
    "within",
    "without",
    "work",
    "wrapper",
    "write",
    "xml",
    "year",
    "years",
    "yes",
    "zone",
];

const DUCKDB_FUNCTIONS: &[&str] = &[
    "abs",
    "acos",
    "add",
    "age",
    "aggregate",
    "alias",
    "all_profiling_output",
    "any_value",
    "apply",
    "approx_count_distinct",
    "approx_quantile",
    "arbitrary",
    "arg_max",
    "arg_min",
    "argmax",
    "argmin",
    "array_agg",
    "array_aggr",
    "array_aggregate",
    "array_append",
    "array_apply",
    "array_cat",
    "array_concat",
    "array_contains",
    "array_distinct",
    "array_extract",
    "array_filter",
    "array_has",
    "array_indexof",
    "array_length",
    "array_pop_back",
    "array_pop_front",
    "array_position",
    "array_prepend",
    "array_push_back",
    "array_push_front",
    "array_reverse_sort",
    "array_slice",
    "array_sort",
    "array_to_json",
    "array_to_string",
    "array_transform",
    "array_unique",
    "arrow_scan",
    "arrow_scan_dumb",
    "ascii",
    "asin",
    "atan",
    "atan2",
    "avg",
    "bar",
    "base64",
    "bin",
    "bit_and",
    "bit_count",
    "bit_length",
    "bit_or",
    "bit_position",
    "bit_xor",
    "bitstring",
    "bitstring_agg",
    "bool_and",
    "bool_or",
    "cardinality",
    "cbrt",
    "ceil",
    "ceiling",
    "century",
    "checkpoint",
    "chr",
    "col_description",
    "collations",
    "combine",
    "concat",
    "concat_ws",
    "constant_or_null",
    "contains",
    "corr",
    "cos",
    "cot",
    "count",
    "count_if",
    "count_star",
    "covar_pop",
    "covar_samp",
    "current_catalog",
    "current_database",
    "current_date",
    "current_query",
    "current_role",
    "current_schema",
    "current_schemas",
    "current_setting",
    "current_user",
    "currval",
    "damerau_levenshtein",
    "database_list",
    "database_size",
    "date_add",
    "date_diff",
    "date_part",
    "date_sub",
    "date_trunc",
    "datediff",
    "datepart",
    "datesub",
    "datetrunc",
    "day",
    "dayname",
    "dayofmonth",
    "dayofweek",
    "dayofyear",
    "decade",
    "decode",
    "degrees",
    "disable_checkpoint_on_shutdown",
    "disable_object_cache",
    "disable_optimizer",
    "disable_print_progress_bar",
    "disable_profile",
    "disable_profiling",
    "disable_progress_bar",
    "disable_verification",
    "disable_verify_external",
    "disable_verify_parallelism",
    "disable_verify_serializer",
    "divide",
    "duckdb_columns",
    "duckdb_constraints",
    "duckdb_databases",
    "duckdb_dependencies",
    "duckdb_extensions",
    "duckdb_functions",
    "duckdb_indexes",
    "duckdb_keywords",
    "duckdb_schemas",
    "duckdb_sequences",
    "duckdb_settings",
    "duckdb_tables",
    "duckdb_temporary_files",
    "duckdb_types",
    "duckdb_views",
    "editdist3",
    "element_at",
    "enable_checkpoint_on_shutdown",
    "enable_object_cache",
    "enable_optimizer",
    "enable_print_progress_bar",
    "enable_profile",
    "enable_profiling",
    "enable_progress_bar",
    "enable_verification",
    "encode",
    "entropy",
    "enum_code",
    "enum_first",
    "enum_last",
    "enum_range",
    "enum_range_boundary",
    "epoch",
    "epoch_ms",
    "era",
    "error",
    "even",
    "exp",
    "factorial",
    "favg",
    "fdiv",
    "filter",
    "finalize",
    "first",
    "flatten",
    "floor",
    "fmod",
    "force_checkpoint",
    "force_index_join",
    "format",
    "formatReadableDecimalSize",
    "format_bytes",
    "format_pg_type",
    "format_type",
    "from_base64",
    "from_binary",
    "from_hex",
    "from_json",
    "from_json_strict",
    "fsum",
    "functions",
    "gamma",
    "gcd",
    "gen_random_uuid",
    "generate_series",
    "generate_subscripts",
    "get_bit",
    "get_current_time",
    "get_current_timestamp",
    "glob",
    "greatest",
    "greatest_common_divisor",
    "group_concat",
    "hamming",
    "has_any_column_privilege",
    "has_column_privilege",
    "has_database_privilege",
    "has_foreign_data_wrapper_privilege",
    "has_function_privilege",
    "has_language_privilege",
    "has_schema_privilege",
    "has_sequence_privilege",
    "has_server_privilege",
    "has_table_privilege",
    "has_tablespace_privilege",
    "hash",
    "hex",
    "histogram",
    "hour",
    "ilike_escape",
    "import_database",
    "in_search_path",
    "index_scan",
    "inet_client_addr",
    "inet_client_port",
    "inet_server_addr",
    "inet_server_port",
    "instr",
    "isfinite",
    "isinf",
    "isnan",
    "isodow",
    "isoyear",
    "jaccard",
    "jaro_similarity",
    "jaro_winkler_similarity",
    "json",
    "json_array",
    "json_array_length",
    "json_contains",
    "json_deserialize_sql",
    "json_execute_serialized_sql",
    "json_extract",
    "json_extract_path",
    "json_extract_path_text",
    "json_extract_string",
    "json_group_array",
    "json_group_object",
    "json_group_structure",
    "json_keys",
    "json_merge_patch",
    "json_object",
    "json_quote",
    "json_serialize_sql",
    "json_structure",
    "json_transform",
    "json_transform_strict",
    "json_type",
    "json_valid",
    "kahan_sum",
    "kurtosis",
    "last",
    "last_day",
    "lcase",
    "lcm",
    "least",
    "least_common_multiple",
    "left",
    "left_grapheme",
    "len",
    "length",
    "length_grapheme",
    "levenshtein",
    "lgamma",
    "like_escape",
    "list",
    "list_aggr",
    "list_aggregate",
    "list_any_value",
    "list_append",
    "list_apply",
    "list_approx_count_distinct",
    "list_avg",
    "list_bit_and",
    "list_bit_or",
    "list_bit_xor",
    "list_bool_and",
    "list_bool_or",
    "list_cat",
    "list_concat",
    "list_contains",
    "list_count",
    "list_distinct",
    "list_element",
    "list_entropy",
    "list_extract",
    "list_filter",
    "list_first",
    "list_has",
    "list_histogram",
    "list_indexof",
    "list_kurtosis",
    "list_last",
    "list_mad",
    "list_max",
    "list_median",
    "list_min",
    "list_mode",
    "list_pack",
    "list_position",
    "list_prepend",
    "list_product",
    "list_reverse_sort",
    "list_sem",
    "list_skewness",
    "list_slice",
    "list_sort",
    "list_stddev_pop",
    "list_stddev_samp",
    "list_string_agg",
    "list_sum",
    "list_transform",
    "list_unique",
    "list_value",
    "list_var_pop",
    "list_var_samp",
    "ln",
    "log",
    "log10",
    "log2",
    "lower",
    "lpad",
    "ltrim",
    "mad",
    "make_date",
    "make_time",
    "make_timestamp",
    "map",
    "map_concat",
    "map_entries",
    "map_extract",
    "map_from_entries",
    "map_keys",
    "map_values",
    "max",
    "max_by",
    "md5",
    "md5_number",
    "md5_number_lower",
    "md5_number_upper",
    "mean",
    "median",
    "microsecond",
    "millennium",
    "millisecond",
    "min",
    "min_by",
    "minute",
    "mismatches",
    "mod",
    "mode",
    "month",
    "monthname",
    "multiply",
    "nextafter",
    "nextval",
    "nfc_normalize",
    "not_ilike_escape",
    "not_like_escape",
    "now",
    "nullif",
    "obj_description",
    "octet_length",
    "ord",
    "parquet_metadata",
    "parquet_scan",
    "parquet_schema",
    "pg_collation_is_visible",
    "pg_conf_load_time",
    "pg_conversion_is_visible",
    "pg_function_is_visible",
    "pg_get_constraintdef",
    "pg_get_expr",
    "pg_get_viewdef",
    "pg_has_role",
    "pg_is_other_temp_schema",
    "pg_my_temp_schema",
    "pg_opclass_is_visible",
    "pg_operator_is_visible",
    "pg_opfamily_is_visible",
    "pg_postmaster_start_time",
    "pg_size_pretty",
    "pg_table_is_visible",
    "pg_ts_config_is_visible",
    "pg_ts_dict_is_visible",
    "pg_ts_parser_is_visible",
    "pg_ts_template_is_visible",
    "pg_type_is_visible",
    "pg_typeof",
    "pi",
    "position",
    "pow",
    "power",
    "pragma_collations",
    "pragma_database_size",
    "pragma_detailed_profiling_output",
    "pragma_last_profiling_output",
    "pragma_storage_info",
    "pragma_table_info",
    "pragma_version",
    "prefix",
    "printf",
    "product",
    "quantile",
    "quantile_cont",
    "quantile_disc",
    "quarter",
    "radians",
    "random",
    "range",
    "read_csv",
    "read_csv_auto",
    "read_json",
    "read_json_auto",
    "read_json_objects",
    "read_json_objects_auto",
    "read_ndjson",
    "read_ndjson_auto",
    "read_ndjson_objects",
    "read_parquet",
    "regexp_extract",
    "regexp_extract_all",
    "regexp_full_match",
    "regexp_matches",
    "regexp_replace",
    "regexp_split_to_array",
    "regr_avgx",
    "regr_avgy",
    "regr_count",
    "regr_intercept",
    "regr_r2",
    "regr_slope",
    "regr_sxx",
    "regr_sxy",
    "regr_syy",
    "repeat",
    "repeat_row",
    "replace",
    "reservoir_quantile",
    "reverse",
    "right",
    "right_grapheme",
    "round",
    "round_even",
    "roundbankers",
    "row",
    "row_to_json",
    "rpad",
    "rtrim",
    "second",
    "sem",
    "seq_scan",
    "session_user",
    "set_bit",
    "setseed",
    "shobj_description",
    "show",
    "show_databases",
    "show_tables",
    "show_tables_expanded",
    "sign",
    "signbit",
    "sin",
    "skewness",
    "split",
    "split_part",
    "sqrt",
    "starts_with",
    "stats",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "storage_info",
    "str_split",
    "str_split_regex",
    "strftime",
    "string_agg",
    "string_split",
    "string_split_regex",
    "string_to_array",
    "strip_accents",
    "strlen",
    "strpos",
    "strptime",
    "struct_extract",
    "struct_insert",
    "struct_pack",
    "substr",
    "substring",
    "substring_grapheme",
    "subtract",
    "suffix",
    "sum",
    "sum_no_overflow",
    "sumkahan",
    "summary",
    "table_info",
    "tan",
    "test_all_types",
    "test_vector_types",
    "time_bucket",
    "timezone",
    "timezone_hour",
    "timezone_minute",
    "to_base64",
    "to_binary",
    "to_days",
    "to_hex",
    "to_hours",
    "to_json",
    "to_microseconds",
    "to_milliseconds",
    "to_minutes",
    "to_months",
    "to_seconds",
    "to_timestamp",
    "to_years",
    "today",
    "transaction_timestamp",
    "translate",
    "trim",
    "trunc",
    "try_strptime",
    "txid_current",
    "typeof",
    "ucase",
    "unbin",
    "unhex",
    "unicode",
    "union_extract",
    "union_tag",
    "union_value",
    "unnest",
    "upper",
    "user",
    "uuid",
    "var_pop",
    "var_samp",
    "variance",
    "verify_external",
    "verify_parallelism",
    "verify_serializer",
    "version",
    "week",
    "weekday",
    "weekofyear",
    "xor",
    "year",
    "yearweek",
];
