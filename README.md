Replica_db
==========

**A fast database cloning tool that actually understands your data**

you know that feeling when you need to test your app woth realistic data, but cant use production data because of privacy laws, security policies, or just common sense? Thats exactly why replica_db exists. Its a tool that looks at your real database, learns what kind of data you have and then generate completely fake data that looks and behaves just like the real data. 

What Does it actually Do?
-------------------------

Think of replica_db as a database photocopier, but instead of making exact copies, it learns the shape of your data and creats brand new data that follows the same patterns. if your real database has customers aged 25-65 with salaries brtween $35k -$150k, replica_db will generate fake customers in that same range. but here's the clever part: it also understands that older people tend to earn more, so it wont generate a 22 year old making $200k (unless  your real data actually shows pattern). 

The tool works in two steps. First, it scans your database and creates what I call a genome - basically a statistical snapshot of your data saved as a JSON file. This file is tiny (usually under 20 KB) and contains zero actual data from your database, just the patterns. second, you use that genome to generate as much fake data as you need. Want a million rows? No problem. Want it to look exactly like production but with fake names and numbers? That's the whole point.

Real-World Performance
----------------------

I tested replica_db on real Uber trip data from New york city - 564,516 actual trips from April 2014. This dataset has timestamps, GPS coordinates (latitude and longitude) and base codes.

**Scanning the database took 2.2 seconds.** That's about 256k rows/sec. That includes connecting to Postgres, reading all the table structures, streaming through half a million rows, calculating statistics for every column, and computing the correlation matrix between latitude and longitude. The resulting genome file was 13 KB.

**Generating 5 million synthetic rows took 101 seconds.** That's about 49,000 rows per second. And this isn't 5 million random numbers - this is 5 million statistically valid trips with correlated GPS coordinates, realistic timestamps, and proper base code distributions. The generated data maintains the same patterns as the real data: most trips cluster in Manhattan, the time distribution matches real traffic patterns, and the lat/lon coordinates are properly correlated so you don't end up with impossible locations.

_Tested on a standard Windows laptop with dockerized Postgres._

Why This Matters?
-----------------

Most tools that generate fake data are either too random or too slow. Random data generators will give you data like a 5-year-old with a PhD and $500k salary. That kind of data breaks your tests because it doesn't reflect reality. On the other hand, copying real data is fast but creates massive privacy and security problems. Even anonymizing real data is risky because smart people can often reverse-engineer it.

replica_db solves both problems. It generates realistic data that maintains all the relationships and patterns from your real database, but it's completely synthetic. There's no way to trace it back to real data because it never contained real data in the first place. Plus, it's incredibly fast. We're talking about scanning a half-million row table in 2 seconds and generating 5 million synthetic rows in under 2 minutes.

Understanding Relationships
---------------------------

Here's where replica_db gets interesting. Most fake data generators treat every column independently. They might correctly generate ages between 20-70 and salaries between $30k-$200k, but they don't understand that these numbers should move together. Real data has correlations: older people usually earn more, people in expensive cities pay higher rent, busy hours have more transactions.

replica_db has something called Gaussian Copulas to preserve these relationships. When it sees that latitude and longitude in your database are corelated -0.9999 (meaning they move together almost perfectly, makes sense for GPS coordinates), it makes sure the synthetic data has that same correlation. The results?? The fake Uber trip data will have coordinates that actually make sense instead of sending riders to random ocean locations.

How it works? (The simple version)
----------------------------------

The sanning phase reads your database and builds up a statistical model. For numeric columns like age or price, it creates histograms showing what values appear and how often. For text columns like category or status, it counts how frequently each value appears. For columns that move together (like latitude and longitude, or age and salary), it calculates how strongly they're correlated.

All of this gets saved into waht i call a genome file. This file contains zero actual data - just the patterns. Think of it like this: instead of taking a photo of your face, it measures the distances between your features. The measurements can't be reverse-engineered into your actual face, but they contain enough information to generate a realistic face that would fit those measurements.

The generation phase loads that genome and starts producing rows. For each row, if there are correlated columns, it first generates a set of correlated random numbers. Then it uses those to pick values from each column's histogram in a way that preserves the relationships. The output is in Postgres COPY format, which means you can pipe it directly into any Postgres database with incredible speed.

Getting Started
---------------

You'll need Rust installed on your system. If you dont have it, download it from [rust-lang.org](https://www.rust-lang.org). it takes about 2 minuites to install. The clone the repository and and build it:

```
git clone https://github.com/Pragadeesh-19/replica_db

cd replica_db

cargo build --release
```

Usage
-----

**Step 1: Scan your database**

Point replica_db at your postgres database and tell it where to save the genome:
```
.\target\release\replica_db.exe scan --url postgresql://user:password@localhost/your_database --output my-genome.json
```
This reads your entire schema and samples your data.  It's safe to run on production databases because it only reads data, never writes anything. The sampling uses reservoir sampling, so memory usage stays constant regardless of how big your tables are.

**Step 2: Generate Synthetic data**
```
.\target\release\replica_db.exe gen --genome my_genome.json --rows 100000 | psql target_database
```
This generates 100000 rows per table and pipes them directly into your database. The --rows parameter controls how many rows to generate per table.

```
.\target\release\replica_db.exe gen --genome my_genome.exe --rows 50000 --seed 42
```
Now everytime you run it with seed, you will get identical output. 

Technical Details 
-----------------

Under the hood, replica_db is written in rust for speed and safety. It uses sqlx for async database access, which means it can scan tables without blocking. The profiling phase uses Algorithm R (reservoir sampling) to maintain constant memory usage regardless of table size. That's how it can scan millions of rows using only about 10 MB of RAM per table.

The correlation detection uses Pearson correlation coefficients and stores them as covariance matrices. During generation, it uses Cholesky decomposition to efficiently generate correlated random samples. This is the same math that quant traders use for risk modeling, adapted for database generation.

The topological sort ensures that parent tables are always generated before child tables, so foreign keys are always valid. The KeyStore caches primary keys in memory as they're generated, and child tables sample from that cache when they need foreign key values.

Why not use python (Faker)?
--------------------------

Faker is random, it doesnt know your data's shape. Python based SDV (synthetic data vault) manages about 200 rows/second on correlations whereas replica_db does ~40k rows per second.  


What it doesn't Do
------------------

replica_db doesnt anonymize data - it generates completely new data. If you need to preserve specific records or relationships to real people, this isn't the right tool. It also doesn't understand complex business rules (like "email addresses must be valid" or "phone numbers must match this format"). It learns patterns from your data, so if your real data has invalid emails, the synthetic data might too.

It also doesn't handle certain types of constrains automatically. If you have CHECK constraints or complex triggers in your database, you might need to disable those before loading synthetic data. Foreign keys are handled perfectly thanks to the topological sort, but custom business logic isn't.

Contributions
-------------

If you find bugs or have ideas for improvements, please open an issue. Pull requests are welcome especially for:

* Support for other databases (MySQL, SQL Server, Oracle)

* Better handling of specific data types (JSON, arrays, geographic types)

* More sophisticated correlation models (non-linear relationships)

* Performance optimizations

Credits
-------

Built with Rust, sqlx, nalgebra for the heavy math. 
