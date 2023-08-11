use std::collections::{HashSet, VecDeque};
use std::time::Instant;
use num_format::{Locale, ToFormattedString};
use rusqlite::Connection;

fn main() {
    // ! CASE SENSITIVE
    let starting_at = "Jake Paul";
    let searching_for = "Danionella cerebrum";

    let start_time = Instant::now();

    let db = Connection::open("completed-table.db").unwrap();
    let mut cached_query = db.prepare_cached("SELECT * FROM page_references WHERE title = ?").unwrap();

    let mut visited = HashSet::new();
    visited.insert(starting_at.to_string());

    //? Consider linked list
    let mut open_set = VecDeque::with_capacity(1_000_000);
    open_set.push_back(Page {
        page: String::from(starting_at),
        from: Vec::new(),
    });

    let mut count: u32 = 0;

    'main_loop: loop {
        let mut page = open_set.pop_front().unwrap();

        count += 1;
        if count % 1000 == 0 {
            println!(
                "Pages searched: {} [{:?}/page] | Cache size: {} | Open set size: {}",
                count.to_formatted_string(&Locale::en),
                start_time.elapsed() / count,
                visited.len().to_formatted_string(&Locale::en),
                open_set.len().to_formatted_string(&Locale::en)
            );
        }

        let links: String;
        loop {
            if page.page.as_str() == searching_for {
                println!("{}", page.to_str());
                break 'main_loop;
            }

            let mut res = cached_query.query_row(
                (&page.page,),
                |row| Ok((row.get(1).unwrap(), row.get(2).unwrap()))
            );

            if res.is_err() {
                let mut v: Vec<char> = page.page.chars().collect();
                v[0] = v[0].to_uppercase().nth(0).unwrap();
                page.page = v.into_iter().collect();

                res = db.query_row(
                    "SELECT * FROM page_references WHERE title = ?",
                    (&page.page,),
                    |row| Ok((row.get(1).unwrap(), row.get(2).unwrap()))
                );
            }

            if let Err(_e) = res {
                // println!("Fetching {:?}->{} failed with: {:?}", page.from.last(), page.page, e);
                continue 'main_loop;
            }

            let (links_, redirect): (String, bool) = res.unwrap();

            if redirect {
                if links_.is_empty() {
                    continue 'main_loop;
                }
                page.add_to_path(links_);
                continue;
            }

            links = links_;
            break;
        }

        for link in links.split("<|>") {
            if link.is_empty() {
                continue;
            }

            if !visited.insert(link.to_string()) {
                continue;
            }

            if link == searching_for {
                page.add_to_path(link.to_string());
                println!("{}", page.to_str());
                break 'main_loop;
            }

            open_set.push_back(page.add_to_path_clone(link.to_string()));
        }

        if open_set.is_empty() {
            println!("No more pages!");
            break;
        }
    }

    println!("Completed in {:?}", start_time.elapsed());
}

struct Page {
    pub page: String,
    pub from: Vec<String>,
}

impl Page {
    pub fn to_str(&self) -> String {
        let mut print_string = String::new();

        for page in &self.from {
            print_string += "https://en.wikipedia.org/wiki/";
            url_escape::encode_path_to_string(page, &mut print_string);
            print_string += " ->\n"
        }

        print_string += "https://en.wikipedia.org/wiki/";
        url_escape::encode_path_to_string(&self.page, &mut print_string);
        print_string
    }

    pub fn add_to_path(&mut self, next: String) {
        self.from.push(self.page.clone());
        self.page = next;
    }

    pub fn add_to_path_clone(&self, next: String) -> Self {
        let mut from = self.from.clone();
        from.push(self.page.clone());

        Self {
            page: next,
            from
        }
    }
}