import xml.etree.ElementTree as ET
import sqlite3
import logging
import typing
import csv
import hashlib
import os

os.makedirs('output', exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a',
    filename='output/errors.log'
)
logger = logging.getLogger(__name__)

COLS = ['category', 'title', 'authors', 'price', 'publish_year']

hash_seen: set = set()


def compute_hash(book_dict: dict) -> bool:
    """Return True if unique, False if duplicate. Hash on stable string fields only."""
    h = hashlib.sha256()
   
    stable_authors = ', '.join(book_dict['authors'])
    input_str = f"{book_dict['category']}|{book_dict['title']}|{stable_authors}|{book_dict['price']}|{book_dict['publish_year']}"
    h.update(input_str.encode('utf-8'))
    digest = h.hexdigest()
    if digest in hash_seen:
        logger.warning(f"Duplicate skipped: '{book_dict['title']}'")
        return False
    hash_seen.add(digest)
    return True


def db_connection() -> typing.Tuple[sqlite3.Connection, sqlite3.Cursor]:
    """Create (or open) the books database and ensure the table exists."""
    conn = sqlite3.connect('output/books.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS books (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            category     TEXT,
            title        TEXT UNIQUE,
            authors      TEXT,
            price        REAL,
            publish_year INTEGER
        )
    ''')
    conn.commit()
    return conn, cursor


def validate_book(book_dict: dict) -> bool:
    """
    Validates a book dict. Returns True only when:
      - category, title, authors, publish_year are present and non-empty
      - price is present and successfully converts to float
    """
    for field in ['category', 'title', 'publish_year']:
        if not book_dict.get(field):
            logger.error(f"Missing '{field}' — skipping: '{book_dict.get('title', 'UNKNOWN')}'")
            return False

    if not book_dict['authors']:
        logger.error(f"No authors found — skipping: '{book_dict['title']}'")
        return False

    if book_dict['price'] is None:
        logger.error(f"Missing <price> — skipping: '{book_dict['title']}'")
        return False

    try:
        book_dict['price'] = float(book_dict['price'])
    except (ValueError, TypeError):
        logger.error(f"Bad price value '{book_dict['price']}' — skipping: '{book_dict['title']}'")
        return False

    return True


def parse_xml(conn: sqlite3.Connection, cursor: sqlite3.Cursor) -> typing.Tuple[list, int]:
    """
    Stream-parse data/books.xml with iterparse.
    Validates and deduplicates each book before loading to SQLite.
    Returns (loaded_books, total_found).
    """
    logger.info('Starting parse: data/books.xml')
    loaded = []
    total = 0

    for _, elem in ET.iterparse('data/books.xml', events=('end',)):
        if elem.tag != 'book':
            
            continue

        total += 1
        price_elem = elem.find('price')

        book = {
            'category':     elem.get('category'),
            'cover':        elem.get('cover'),          
            'title':        elem.find('title').text,
            'authors':      [a.text for a in elem.findall('author')],
            'price':        price_elem.text if price_elem is not None else None,
            'publish_year': elem.find('year').text,
        }

        if validate_book(book) and compute_hash(book):
            try:
                cursor.execute(
                    '''
                    INSERT OR IGNORE INTO books (category, title, authors, price, publish_year)
                    VALUES (?, ?, ?, ?, ?)
                    ''',
                    (
                        book['category'],
                        book['title'],
                        ', '.join(book['authors']),
                        book['price'],
                        book['publish_year'],
                    )
                )
                conn.commit()
                if cursor.rowcount == 1:        # actually inserted
                    loaded.append(book)
                    logger.info(f"Loaded: '{book['title']}'")
                else:                           # UNIQUE constraint silently blocked it
                    logger.info(f"Already in DB, skipped: '{book['title']}'")
                    
            except sqlite3.Error as e:
                logger.error(f"DB insert failed for '{book['title']}': {e}")

       
        elem.clear()

    logger.info(f"Done. total={total} loaded={len(loaded)} skipped={total - len(loaded)}")
    return loaded, total


def write_csv(data: list) -> None:
    """Export the loaded books list to output/books_output.csv."""
    path = 'output/books_output.csv'
    with open(path, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=COLS)
        writer.writeheader()
        for book in data:
            writer.writerow({
                'category':     book['category'],
                'title':        book['title'],
                'authors':      ', '.join(book['authors']),
                'price':        book['price'],
                'publish_year': book['publish_year'],
            })
    logger.info(f"CSV written: {path}")

def fetch_from_db(conn, cursor):
    cursor.execute('select * from books')
    rows = cursor.fetchall()
    print('\n--- Books in Database ---')
    for row in rows:
        print(row)

if __name__ == '__main__':
    conn, cursor = db_connection()
    data, total = parse_xml(conn, cursor)

    avg_price = sum(b['price'] for b in data) / len(data) if data else 0.0

    print('\n--- Pipeline Summary ---')
    print(f'Books found:   {total}')
    print(f'Books loaded:  {len(data)}')
    print(f'Books skipped: {total - len(data)}')
    print(f'Avg price:     ${avg_price:.2f}')
    print()

    write_csv(data)
    print('Output saved to:  output/books_output.csv')
    print('Errors logged to: output/errors.log')
    fetch_from_db(conn, cursor)

    conn.close()