use std::collections::{HashSet, VecDeque};
use std::collections::hash_map::DefaultHasher;
use std::env;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::rc::Rc;
use std::time::Instant;
use hhmmss::Hhmmss;
use num_format::{Locale, ToFormattedString};
use rusqlite::{Connection, Error};

// No Rc: 10.1M Cache - 4.3GB
// Rc: 15M Cache - 8.2GB
// Double Rc:  10.6M - 1.2GB

fn main() {
    let args: Vec<String> = env::args().into_iter().collect();

    // ! CASE SENSITIVE
    // let starting_at = "Tobi 12";
    // let searching_for = "xxINVALIDxx";

    let (starting_at, searching_for) = if args.len() >= 3 {
        (args[1].as_str(), args[2].as_str())
    }
    else {
        ("Bedford", "Paul Singer (businessman)")
    };


    let start_time = Instant::now();

    let db = Connection::open("completed-table.db").unwrap();
    db.execute_batch(
        "PRAGMA synchronous = 0;
              PRAGMA locking_mode = EXCLUSIVE;
              PRAGMA temp_store = MEMORY;
              PRAGMA journal_mode = OFF;
              "
        ,
    ).unwrap();
    let mut cached_query = db.prepare_cached("SELECT * FROM page_references WHERE title = ?").unwrap();

    for p in [starting_at, searching_for] {
        let res: Result<(String, bool), Error> = cached_query.query_row(
            (p,),
            |row| Ok((row.get(1).unwrap(), row.get(2).unwrap()))
        );

        if res.is_ok() {
            println!("Page {} is valid", p);
        }
        else {
            println!("Page {} is invalid", p);
        }
    }


    let starting_page = PageHolder::from_page(Page::new(
        String::from(starting_at),
        None,
    ));

    let mut visited = HashSet::with_capacity(17_000_000);
    visited.insert(starting_page.clone());

    //? Consider linked list
    let mut open_set = VecDeque::with_capacity(1_000_000);
    open_set.push_back(starting_page);

    let mut count: u32 = 0;

    'main_loop: loop {
        let mut page = open_set.pop_front().unwrap();
        if open_set.is_empty() {
            println!("Last page: {}", page.to_str());
        }

        count += 1;
        if count % 10_000 == 0 {
            println!(
                "Pages searched: {} [{:?}/page] | Cache size: {} | Open set size: {}",
                count.to_formatted_string(&Locale::en),
                start_time.elapsed() / count,
                visited.len().to_formatted_string(&Locale::en),
                open_set.len().to_formatted_string(&Locale::en),
            );
        }

        let mut loop_count: usize = 0;
        let links: String;
        loop {
            if loop_count >= 20 {
                println!("Link following for '{}' has reached a depth of {}", page.get_page().page, loop_count);
                continue 'main_loop;
            }
            loop_count += 1;

            if page.get_page().page.as_str() == searching_for {
                println!("{}", page.to_str());
                break 'main_loop;
            }

            let mut res = cached_query.query_row(
                (&page.get_page().page,),
                |row| Ok((row.get(1).unwrap(), row.get(2).unwrap()))
            );

            if res.is_err() {
                let mut v: Vec<char> = page.get_page().page.chars().collect();
                v[0] = v[0].to_uppercase().nth(0).unwrap();
                let modified_name: String = v.into_iter().collect();

                res = db.query_row(
                    "SELECT * FROM page_references WHERE title = ?",
                    (&modified_name,),
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
                page = PageHolder::add_to_path(&page, links_.clone());

                visited.insert(page.clone());
                continue;
            }

            links = links_;
            break;
        }

        for link in links.split("<|>") {
            if link.is_empty() {
                continue;
            }

            let new_page = PageHolder::add_to_path(&page, link.to_string());

            if !visited.insert(new_page.clone()) {
                continue;
            }

            if link == searching_for {
                println!("{}", new_page.to_str());
                break 'main_loop;
            }

            open_set.push_back(new_page);
        }

        if open_set.is_empty() {
            println!("No more pages!");
            break;
        }
    }

    println!("Completed in {:?}", start_time.elapsed().hhmmssxxx());
}

#[derive(Clone)]
struct PageHolder {
    pub page: Rc<Page>
}

impl PageHolder {
    pub fn from_page(page: Page) -> PageHolder {
        PageHolder { page: Rc::new(page) }
    }

    pub fn get_page(&self) -> &Page {
        self.page.deref()
    }

    pub fn to_str(&self) -> String {
        let mut print_string = String::from("https://en.wikipedia.org/wiki/");
        url_escape::encode_path_to_string(&self.get_page().page, &mut print_string);

        let mut from = self.get_page().from.as_ref().map(|p| p.get_page());
        while from.is_some() {
            let mut new_link = "https://en.wikipedia.org/wiki/".to_string();
            url_escape::encode_path_to_string(from.unwrap().page.clone(), &mut new_link);
            from = from.unwrap().from.as_ref().map(|p| p.get_page());
            new_link += " ->\n";
            print_string = new_link + print_string.as_str();
        }

        print_string
    }

    pub fn add_to_path(prev: &PageHolder, next: String) -> PageHolder {
        PageHolder::from_page(
            Page::new(
                next,
                Some(prev.clone())
            )
        )
    }
}

impl Hash for PageHolder {
    fn hash<H: Hasher>(&self, state: &mut H) {

        state.write_u64(self.get_page().hash);
    }
}

impl PartialEq for PageHolder {
    fn eq(&self, other: &Self) -> bool {
        self.get_page().hash == other.get_page().hash
    }
}

impl Eq for PageHolder {}

struct Page {
    pub page: String,
    pub hash: u64,
    pub from: Option<PageHolder>,
}

impl Page {
    pub fn new(page: String, from: Option<PageHolder>) -> Page {
        let mut hasher = DefaultHasher::new();
        page.hash(&mut hasher);
        let hash = hasher.finish();

        Page {
            page,
            hash,
            from
        }
    }
}