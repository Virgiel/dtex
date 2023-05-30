use polars::sql::keywords::{all_functions, all_keywords};
use sqlparser::{dialect::GenericDialect, keywords::Keyword, tokenizer::Token};
use tui::{none, Color, Style};

pub struct Highlighter {
    styles: Vec<(u64, Style)>,
    idx: usize,
}

impl Highlighter {
    pub fn new(query: &str) -> Self {
        let mut tmp = Self {
            styles: vec![(0, tui::none())],
            idx: 0,
        };
        for token in sqlparser::tokenizer::Tokenizer::new(&GenericDialect::default(), query)
            .tokenize_with_location()
            .unwrap_or_default()
        {
            tmp.styles.push((
                token.location.column - 1,
                match token.token {
                    Token::Number(_, _) => none().fg(Color::Yellow),
                    Token::SingleQuotedString(_) | Token::DoubleQuotedString(_) => {
                        none().fg(Color::Yellow).italic()
                    }
                    Token::Word(w) => match w.keyword {
                        Keyword::SELECT
                        | Keyword::FROM
                        | Keyword::WHERE
                        | Keyword::GROUP
                        | Keyword::BY
                        | Keyword::ORDER
                        | Keyword::LIMIT
                        | Keyword::OFFSET
                        | Keyword::AND
                        | Keyword::OR
                        | Keyword::AS
                        | Keyword::ON
                        | Keyword::INNER
                        | Keyword::LEFT
                        | Keyword::RIGHT
                        | Keyword::FULL
                        | Keyword::OUTER
                        | Keyword::JOIN
                        | Keyword::CREATE
                        | Keyword::TABLE
                        | Keyword::SHOW
                        | Keyword::TABLES
                        | Keyword::VARCHAR
                        | Keyword::INT
                        | Keyword::FLOAT
                        | Keyword::DOUBLE
                        | Keyword::BOOLEAN
                        | Keyword::DATE
                        | Keyword::TIME
                        | Keyword::DATETIME
                        | Keyword::ARRAY
                        | Keyword::ASC
                        | Keyword::DESC
                        | Keyword::NULL
                        | Keyword::NOT
                        | Keyword::IN
                        | Keyword::WITH => none().fg(Color::Magenta),
                        _ => match w.to_string().as_str() {
                            s if all_functions().contains(&s) => none().fg(Color::Cyan),
                            s if all_keywords().contains(&s) => none().fg(Color::Magenta),
                            "current" => none().fg(Color::Green),
                            _ => none(),
                        },
                    },
                    _ => none(),
                },
            ))
        }
        tmp.styles.push((u64::MAX, none()));
        tmp
    }

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
