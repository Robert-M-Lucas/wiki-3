use std::cmp::min;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::time::Instant;
use hhmmss::Hhmmss;
use rusqlite::{Connection, ToSql};
use rusqlite::types::{ToSqlOutput, ValueRef};

fn main() {
    let file = File::open("enwiki-20230801-pages-articles.xml").unwrap();
    let reader = BufReader::new(file);

    let mut db = DB::new(1000, 100_000);

    let mut count: u32 = 0;
    let start = Instant::now();
    const TOTAL_ARTICLES: u32 = 23_100_000;

    let mut lines = reader.lines();

    const TITLE_TAG: &str = "    <title>";
    const END_TITLE_TAG: &str = "</title>";
    const TEXT_TAG: &str = "      <text";
    const END_TEXT_TAG: &str = "</text>";

    'main_loop: loop {
        let title;
        loop {
            let line = match lines.next() {
                Some(Ok(line)) => line,
                Some(Err(e)) => {
                    println!("Breaking main loop due to error reading line: {:?}", e);
                    break 'main_loop;
                }
                None => {{
                    println!("No more lines");
                    break 'main_loop;
                }}
            };
            if line.len() < TITLE_TAG.len() || !line.is_char_boundary(TITLE_TAG.len()) || &line[..TITLE_TAG.len()] != TITLE_TAG {
                continue;
            }

            title = line[TITLE_TAG.len()..line.len() - END_TITLE_TAG.len()].to_string();
            break;
        }

        for pattern in FORBIDDEN_PATTERNS {
            if title.find(pattern).is_some() {
                continue 'main_loop;
            }
        }


        let mut body = String::with_capacity(30);
        loop {
            let line = match lines.next() {
                Some(Ok(line)) => line,
                Some(Err(e)) => {
                    println!("Breaking main loop due to error reading line: {:?}", e);
                    break 'main_loop;
                }
                None => {{
                    println!("No more lines");
                    break 'main_loop;
                }}
            };
            if line.len() < TEXT_TAG.len() || !line.is_char_boundary(TEXT_TAG.len()) || &line[..TEXT_TAG.len()] != TEXT_TAG {
                continue;
            }

            let start = line.find('>');
            let mut line_owned = line[(start.unwrap() + '>'.len_utf8())..].to_string();
            let mut line = line_owned.as_str();
            loop {
                let mut end = false;
                if line.len() >= END_TEXT_TAG.len() &&
                    line.is_char_boundary(line.len() - END_TEXT_TAG.len()) &&
                    &line[(line.len() - END_TEXT_TAG.len())..] == END_TEXT_TAG {
                    end = true;
                    line = &line[..line.len() - END_TEXT_TAG.len()];
                }

                body += line;
                if end { break; }
                else { body.push('\n'); }
                line_owned = lines.next().unwrap().unwrap();
                line = line_owned.as_str();
            }
            break;
        }

        let (links, is_redirect) = match get_links_from_body(body, &title) {
            Ok(links) => links,
            Err(e) => {
                println!("{}", e);
                continue;
            }
        };

        // db.cache(title.to_lowercase(), links, is_redirect);
        db.cache(title, links, is_redirect);

        count += 1;
        if count % 50_000 == 0 {
            if count < TOTAL_ARTICLES {
                println!("Completed {} articles in {} [{:?}/article]. ETA: {}", count, start.elapsed().hhmmss(), start.elapsed() / count, ((start.elapsed() / count) * (TOTAL_ARTICLES - count)).hhmmss())
            }
            else {
                println!("Completed {} articles in {} [{:?}/article]", count, start.elapsed().hhmmss(), start.elapsed() / count);
            }
        }
    }

    db.write_to_db();

    drop(db);
    fs::rename("table.db", "completed-table.db").unwrap();

    println!("Completed {} articles in {} [{:?}/article]", count, start.elapsed().hhmmss(), start.elapsed() / count);
}

struct DB {
    conn: Connection,
    batch_size: usize,
    insert_threshold: usize,
    to_insert: Vec<(String, String, bool)>
}

impl DB {
    pub fn new(batch_size: usize, insert_threshold: usize) -> Self {
        let conn = Connection::open("table.db").unwrap();

        conn.execute_batch(
            "PRAGMA journal_mode = OFF;
              PRAGMA synchronous = 0;
              PRAGMA cache_size = 1000000;
              PRAGMA locking_mode = EXCLUSIVE;
              PRAGMA temp_store = MEMORY;",
        ).unwrap();

        conn.execute(
            "DROP TABLE IF EXISTS page_references",
            ()
        ).unwrap();

        conn.execute(
            "DROP TABLE IF EXISTS page_reference_errors",
            ()
        ).unwrap();

        conn.execute(
            "CREATE TABLE IF NOT EXISTS page_references (
            title TEXT PRIMARY KEY,
            links TEXT,
            is_redirect INTEGER
         )",
            ()
        ).unwrap();

        conn.execute(
            "CREATE TABLE IF NOT EXISTS page_reference_errors (
            title TEXT,
            links TEXT,
            is_redirect INTEGER
         )",
            ()
        ).unwrap();


        Self {
            conn,
            batch_size,
            insert_threshold,
            to_insert: Vec::with_capacity(insert_threshold)
        }
    }

    pub fn write_to_db(&mut self) {
        if self.to_insert.len() == 0 {
            println!("Cancelling db write as cache is empty");
            return;
        }

        let start = Instant::now();
        println!("Writing {} entries to database", self.to_insert.len());

        let mut cached_statement =
            self.conn.prepare_cached(
                format!("INSERT INTO page_references VALUES {}", " (?, ?, ?),".repeat(self.batch_size - 1) + " (?, ?, ?)")
                    .as_str()).unwrap();

        let mut individual_cached_statement =
            self.conn.prepare_cached("INSERT INTO page_references VALUES (?, ?, ?)").unwrap();

        let mut params = Vec::with_capacity(self.batch_size * 3);
        let (batchable, non_batchable) = self.to_insert.split_at(
            self.to_insert.len() - (self.to_insert.len() % self.batch_size)
        );

        let mut count = 0;
        for data in batchable {
            params.push(&data.0 as &dyn ToSql);
            params.push(&data.1 as &dyn ToSql);
            params.push(&data.2 as &dyn ToSql);
            count += 1;
            if count == self.batch_size {
                if let Err(e) = cached_statement.execute(&*params) {
                    println!("Database batch failed due to error - retrying one at a time: {:?}", e);

                    for params in params.chunks(3) {
                        if let Err(e) = individual_cached_statement.execute(params) {
                            let title = match params[0].to_sql().unwrap()
                            {
                                ToSqlOutput::Borrowed(ValueRef::Text(value)) => String::from_utf8_lossy(value),
                                _ => panic!()
                            };

                            let links = match params[1].to_sql().unwrap()
                            {
                                ToSqlOutput::Borrowed(ValueRef::Text(value)) => String::from_utf8_lossy(value),
                                _ => panic!()
                            };

                            println!(
                                "Database insert on data [{}, {}, {:?}] failed due to error: {:?}",
                                title,
                                links,
                                params[2].to_sql().unwrap(),
                                e
                            );

                            let result = self.conn.execute("INSERT INTO page_reference_errors VALUES (?, ?, ?)", params);
                            if result.is_err() { println!("{:?}", result.unwrap_err()); }
                        }
                    }
                }
                params = Vec::with_capacity(self.batch_size * 3);
                count = 0;
            }
        }

        if non_batchable.len() > 0 {
            for data in non_batchable {
                if let Err(e) = individual_cached_statement.execute((&data.0, &data.1, &data.2)) {
                    println!(
                        "Database insert on data [{:?}, {:?}, {:?}] failed due to error: {:?}",
                        data.0,
                        data.1,
                        data.2,
                        e
                    );

                    let result = self.conn.execute("INSERT INTO page_reference_errors VALUES (?, ?, ?)", (&data.0, &data.1, &data.2));
                    if result.is_err() { println!("{:?}", result.unwrap_err()); }
                }
            }
        }

        self.to_insert = Vec::with_capacity(self.insert_threshold);
        println!("Finished writing to database in {:?}", start.elapsed());
    }

    pub fn cache(&mut self, title: String, links: String, is_redirect: bool) {
        self.to_insert.push((title, links, is_redirect));
        if self.to_insert.len() >= self.insert_threshold {
            self.write_to_db();
        }
    }
}

const REDIRECT_TEXT: &str = "#REDIRECT [[";
const FORBIDDEN_PATTERNS: [&str; 10] = [
    "Wikipedia:",
    "Category:",
    "File:",
    "Special:",
    "Template:",
    "Template_talk:",
    "User:",
    "WP:",
    "Help:",
    "File:",
    // "Portal:",
];

const SEE_ALSO: &str = "==See also==";
const REFERENCES: &str = "==References==";
fn get_links_from_body(body: String, title: &String) -> Result<(String, bool), String> {
    return if body.len() > REDIRECT_TEXT.len() && body.is_char_boundary(REDIRECT_TEXT.len()) && &body[..REDIRECT_TEXT.len()] == REDIRECT_TEXT {
        let end = body.find("]]");
        if let Some(end) = end {
            let redirect = body[REDIRECT_TEXT.len()..end].trim();
            let redirect = redirect.split('#').nth(0).unwrap().trim();
            for pattern in FORBIDDEN_PATTERNS {
                if redirect.len() >= pattern.len() && redirect.is_char_boundary(pattern.len()) && &redirect[..pattern.len()] == pattern {
                    return Ok(("".to_string(), true));
                }
            }
            Ok((redirect.to_string(), true))
        } else {
            Err(format!("Getting redirect link from '{}' failed", title))
        }
    } else {
        let mut references: String = String::new();

        let mut first = true;
        let limit = body.find(SEE_ALSO).or_else(|| body.find(REFERENCES)).unwrap_or(body.len());
        let body = &body[..limit];
        'link_search_loop: for (link_pos, _) in body.match_indices("[[") {
            let after_link_start = &body[link_pos + "[[".len()..];
            let end1 = after_link_start.find('|');
            let end2 = after_link_start.find(']');
            let end = if end1.is_some() && end2.is_some() {
                Some(min(end1.unwrap(), end2.unwrap()))
            }
            else {
                end1.or_else(|| end2)
            };

            if let Some(end) = end {
                let mut link = after_link_start[..end].trim();
                for pattern in FORBIDDEN_PATTERNS {
                    if link.len() >= pattern.len() && link.is_char_boundary(pattern.len()) && &link[..pattern.len()] == pattern {
                        continue 'link_search_loop;
                    }
                }

                if let Some(pos) = link.find('#') {
                    if pos == 0 {
                        continue;
                    }
                    link = &link[..pos];
                }

                if first {
                    first = false;
                } else {
                    references += "<|>";
                }
                references += link;
            } else {
                break;
            }
        }

        Ok((references, false))
    }
}