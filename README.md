# Books XML Data Pipeline
ETL (Extract, Transform, Load) pipeline that parses an XML dataset of books, validates the data, loads it into a SQLite database, and exports the results to CSV. Built as a hands-on learning project covering core data engineering concepts.

---

## What it does

```
books.xml → parse → validate → deduplicate → SQLite DB → CSV export
```

1. **Parses** `data/books.xml` using Python's `iterparse` for memory-efficient streaming
2. **Validates** each book — rejects missing fields and invalid price values
3. **Deduplicates** using SHA-256 hashing in memory and a UNIQUE constraint in the database
4. **Loads** valid books into a SQLite database with `INSERT OR IGNORE`
5. **Exports** the loaded data to `output/books_output.csv`
6. **Logs** all errors, warnings, and skipped records to `output/errors.log`

---

## Dataset

The dataset (`data/books.xml`) contains 10 books with intentional data quality issues baked in:

| Book | Issue |
|---|---|
| To Kill a Mockingbird | Price is `NOT_A_PRICE` — invalid, gets rejected |
| The Selfish Gene | Missing `<price>` tag entirely — gets rejected |
| XQuery Kick Start | Has 5 authors — tests multi-author handling |
| JavaScript & The Good Parts | Contains `&amp;` — tests special character handling |
| Salt, Fat, Acid, Heat | Title has commas — tests punctuation in titles |

Expected result: **8 books loaded, 2 skipped** on every run.

---

## Project structure

```
books-xml-pipeline/
├── data/
│   └── books.xml            # input dataset
├── output/
│   ├── books.db             # SQLite database (auto-created)
│   ├── books_output.csv     # CSV export (auto-created)
│   └── errors.log           # validation and pipeline logs (auto-created)
├── pipeline.py              # main pipeline script
├── requirements.txt         # dependencies
└── README.md
```

---

## How to install

```bash
# Clone the repo
git clone https://github.com/your-username/books-xml-pipeline.git
cd books-xml-pipeline

# Create and activate a virtual environment
python -m venv env
source env/bin/activate        # Mac/Linux
env\Scripts\activate           # Windows

# Install dependencies
pip install -r requirements.txt
```

---

## How to run

```bash
python pipeline.py
```

Expected output:

```
--- Pipeline Summary ---
Books found:   10
Books loaded:  8
Books skipped: 2
Avg price:     $29.80

Output saved to:  output/books_output.csv
Errors logged to: output/errors.log
```

To reset and run fresh:

```bash
rm output/books.db
python pipeline.py
```

---

## Output files

### `output/books.db`
SQLite database with a single `books` table:

| Column | Type | Notes |
|---|---|---|
| id | INTEGER | Auto-incremented primary key |
| category | TEXT | Book genre (cooking, fiction, web, etc.) |
| title | TEXT | Unique — enforced by DB constraint |
| authors | TEXT | Comma-separated if multiple authors |
| price | REAL | Validated as a float before insert |
| publish_year | INTEGER | Year of publication |

### `output/books_output.csv`
Flat CSV export of all successfully loaded books. Same columns as the database (excluding `id`).

### `output/errors.log`
Structured log of every pipeline run. Example entries:

```
INFO  - Loaded: 'Everyday Italian'
ERROR - Bad price value 'NOT_A_PRICE' — skipping: 'To Kill a Mockingbird'
ERROR - Missing <price> — skipping: 'The Selfish Gene'
INFO  - Already in DB, skipped: 'Harry Potter'
INFO  - Done. total=10 loaded=8 skipped=2
```

---

## Key engineering decisions

**Idempotent pipeline** — running the pipeline multiple times always produces the same result. Re-runs never create duplicate rows. This is enforced at two levels: a SHA-256 hash check in memory, and a `UNIQUE` constraint + `INSERT OR IGNORE` in the database.

**Validate before loading** — invalid records are caught during validation and logged to `errors.log` before they ever reach the database. The pipeline never silently drops bad data.

**`iterparse` over `ET.parse()`** — the pipeline uses streaming XML parsing so it can handle large files without loading the entire document into memory.

**`cursor.rowcount` for honest reporting** — the summary counts only rows that were actually inserted into the database, not rows that were processed. On re-runs, `Books loaded: 0` is the correct and expected output.

**Schema migrations** — `CREATE TABLE IF NOT EXISTS` silently ignores schema changes on existing databases. If you modify the schema, delete `output/books.db` before running so the table is recreated correctly.

---

## Key Learnings

- Parsing XML with `iterparse` for memory-efficient streaming
- Writing a validation layer that catches bad data before it enters the database
- Using `INSERT OR IGNORE` with a `UNIQUE` constraint to make pipelines safe to re-run
- Why `cursor.rowcount` matters — the difference between "processed" and "actually inserted"
- How `CREATE TABLE IF NOT EXISTS` behaves on existing databases (schema changes are ignored)
- Structured logging with Python's `logging` module — separating INFO, WARNING, and ERROR
- Building an end-to-end ETL pipeline: XML → validate → SQLite → CSV

---

## Requirements

```
# requirements.txt
lxml
pandas
```

Python 3.8 or higher.
