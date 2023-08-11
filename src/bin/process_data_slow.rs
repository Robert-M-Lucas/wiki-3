use std::cmp::min;
use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, Write};
use std::path::Path;
use std::time::Instant;
use rusqlite::Connection;
use xml::common::Position;
use xml::{EventReader, ParserConfig};
use xml::reader::{ParserConfig2, XmlEvent};


fn print_xml(event: XmlEvent) {
    match event {
        XmlEvent::StartDocument { .. } => { println!("Start document"); }
        XmlEvent::EndDocument => { println!("End document"); }
        XmlEvent::ProcessingInstruction { .. } => { println!("Processing instruction") }
        XmlEvent::StartElement {
            name,
            namespace,
            attributes

        } => {
            println!("Start of '{}'", name.local_name);
            for attribute in attributes {
                println!("Attribute {}: {}", attribute.name.local_name, attribute.value);
            }
        }
        XmlEvent::EndElement { name } => { println!("End of {:?}", name) }
        XmlEvent::CData(_) => { println!("CData"); }
        XmlEvent::Comment(_) => { println!("Comment"); }
        XmlEvent::Characters(chars) => { println!("Characters: {}", chars); }
        XmlEvent::Whitespace(_) => { println!("Whitespace"); }
    }
}

fn next_until_element(element_name: &str, event_reader: &mut EventReader<File>) -> XmlEvent {
    let mut ret = XmlEvent::EndDocument;
    loop {
        ret = event_reader.next().unwrap();
        match &ret {
            XmlEvent::StartElement { name, attributes: _, namespace: _ } => {
                if name.local_name.as_str() == element_name {
                    break;
                }
            }
            _ => {}
        };
    }
    ret
}

const TOTAL_ARTICLES: u32 = 23_100_000;

fn main() {
    // fs::create_dir_all("data").unwrap();
    let conn = Connection::open("table.db").unwrap();

    conn.execute(
        "CREATE TABLE IF NOT EXISTS page_references (
            title TEXT,
            reference TEXT,
            is_redirect INTEGER
         )",
        ()
    ).unwrap();

    let file = File::open("enwiki-20230801-pages-articles.xml").unwrap();
    let parser_config = ParserConfig::new().trim_whitespace(true);

    let mut parser = EventReader::new_with_config(file, parser_config);
    parser.next().unwrap(); // Start
    parser.next().unwrap(); // Mediawiki
    parser.next().unwrap(); // Siteinfo
    parser.skip().unwrap(); // Skip to page

    parser.next().unwrap(); // Page

    let mut count = 0;
    let start = Instant::now();

    let mut insert_statement = conn.prepare("INSERT INTO page_references VALUES (?1, ?2, FALSE)").unwrap();
    let mut insert_redirect_statement = conn.prepare("INSERT INTO page_references VALUES (?1, ?2, TRUE)").unwrap();


    'main_loop: loop {
        // Get to title
        loop {
            let next = parser.next().unwrap();
            match &next {
                XmlEvent::StartDocument { .. } | XmlEvent::EndDocument => { panic!() }
                XmlEvent::StartElement { name, namespace: _, attributes: _ } => {
                    if name.local_name.as_str() == "title" {
                        break;
                    }
                }
                XmlEvent::EndElement { name } => {
                    if name.local_name.as_str() == "page" { panic!(); }
                    continue;
                }
                XmlEvent::CData(_) | XmlEvent::Comment(_) | XmlEvent::Whitespace(_) | XmlEvent::Characters(_) | XmlEvent::ProcessingInstruction { .. } => { continue; }
            };
        }

        let title = match parser.next().unwrap() {
            XmlEvent::Characters(title) => title,
            _ => panic!()
        };

        // Get to revision
        loop {
            let next = parser.next().unwrap();
            match &next {
                XmlEvent::StartDocument { .. } | XmlEvent::EndDocument => { panic!() }
                XmlEvent::StartElement { name, namespace: _, attributes: _ } => {
                    if name.local_name.as_str() == "revision" {
                        break;
                    }
                }
                XmlEvent::EndElement { name } => {
                    if name.local_name.as_str() == "page" { panic!(); }
                    continue;
                }
                XmlEvent::CData(_) | XmlEvent::Comment(_) | XmlEvent::Whitespace(_) | XmlEvent::Characters(_) | XmlEvent::ProcessingInstruction { .. } => { continue; }
            };
        }

        // Get to text
        loop {
            let next = parser.next().unwrap();
            match &next {
                XmlEvent::StartDocument { .. } | XmlEvent::EndDocument => { panic!() }
                XmlEvent::StartElement { name, namespace: _, attributes: _ } => {
                    if name.local_name.as_str() == "text" {
                        break;
                    }
                }
                XmlEvent::EndElement { name } => {
                    if name.local_name.as_str() == "page" { panic!(); }
                    continue;
                }
                XmlEvent::CData(_) | XmlEvent::Comment(_) | XmlEvent::Whitespace(_) | XmlEvent::Characters(_) | XmlEvent::ProcessingInstruction { .. } => { continue; }
            };
        }

        let text = match parser.next().unwrap() {
            XmlEvent::Characters(text) => text,
            _ => panic!()
        };

        // if !title.contains('/') {
        //     let full_path = String::from("data/") + title.as_str();
        //     let path = Path::new(full_path.as_str());
        //     let res = File::create(path).and_then(|mut s| s.write_all(text.as_bytes()));
        //     if res.is_err() {
        //         println!("{} : {:?}", title, res.unwrap_err());
        //     }
        // }
        // else {
        //     println!("Skipping '{}' due to '/'", title);
        // }



        const REDIRECT_TEXT: &str = "#REDIRECT [[";
        if text.len() > REDIRECT_TEXT.len() && text.is_char_boundary(REDIRECT_TEXT.len()) && &text[..REDIRECT_TEXT.len()] == REDIRECT_TEXT {
            let end = text.find("]]");
            if let Some(end) = end {
                let redirect: String = text[REDIRECT_TEXT.len()..].chars().take(end - REDIRECT_TEXT.len()).collect();
                if let Err(e) = insert_redirect_statement.execute((&title, &redirect)) {
                    println!("Redirect insert [{}]->[{}] failed with error {:?}", title, redirect, e);
                }
            }
            else {
                println!("Getting redirect link from '{}' failed", title);
            }
        }
        else {
            let mut references: Vec<String> = Vec::new();

            const FORBIDDEN_PATTERNS: [&str; 7] = [
                "Wikipedia:",
                "Category:",
                "File:",
                "Special:",
                "Template:",
                "Template_talk:",
                "User:"
            ];

            const SEE_ALSO: &str = "==See also==";
            const REFERENCES: &str = "==References==";

            let mut link_depth: i32 = 0;
            let mut buffer = String::with_capacity(10);
            let mut previous = ' ';
            let limit = text.find(SEE_ALSO).or_else(|| text.find(REFERENCES)).unwrap_or(text.len());
            for c in text.chars().take(limit) {
                if link_depth > 0 {
                    if c == ']' && previous == ']' {
                        link_depth -= 1;

                        if link_depth == 0 {
                            buffer = buffer.chars().take(buffer.len() - 1).collect();
                            buffer = buffer.split('|').nth(0).unwrap().to_string();

                            let mut failed = false;
                            for pattern in FORBIDDEN_PATTERNS {
                                if buffer.contains(pattern) {
                                    failed = true;
                                    break;
                                }
                            }

                            if !failed {
                                references.push(buffer);
                                buffer = String::new();
                            }
                        }
                    }
                    else {
                        buffer.push(c);
                    }
                }
                else {
                    if c == '[' && previous == '[' {
                        link_depth += 1;
                    }
                }

                previous = c;
            }

            for link in references {
                if let Err(e) = insert_statement.execute(
                    (&title, &link)
                ) {
                    println!("Link insert [{}]->[{}] failed with error {:?}", title, link, e);
                }
            }
        }



        // Get to page end
        loop {
            let next = parser.next().unwrap();
            match &next {
                XmlEvent::StartDocument { .. } | XmlEvent::EndDocument => { panic!() }
                XmlEvent::EndElement { name } => {
                    if name.local_name.as_str() == "page" { break; }
                }
                XmlEvent::CData(_) | XmlEvent::Comment(_) | XmlEvent::Whitespace(_) | XmlEvent::Characters(_) | XmlEvent::ProcessingInstruction { .. } | XmlEvent::StartElement { .. } => { continue; }
            };
        }

        match parser.next().unwrap() {
            XmlEvent::StartElement { name, namespace: _, attributes: _ } => {
                if name.local_name != "page" { break 'main_loop; }
            }
            _ => {}
        };

        count += 1;
        if count % 10 == 0 {
            println!("Completed {} articles in {:?} [{:?}/article]. ETA: {:?}", count, start.elapsed(), start.elapsed() / count, ((start.elapsed() / count) * (TOTAL_ARTICLES - count)) );
        }

    }

    println!("Finished in {:?}", start.elapsed());
}