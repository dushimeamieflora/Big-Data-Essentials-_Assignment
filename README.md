# Distributed Taxi Trip Analytics — Apache Hadoop / HDFS / Python MapReduce

NYC Yellow Taxi trip data, **April, May, June 2023** (matches the "2-3 monthly
datasets" requirement in Section 2 of the assignment brief).

## 1\. Environment Assumptions

* Hadoop 3.x (HDFS + YARN) installed and running in pseudo-distributed or
cluster mode. Verify with `jps` — you should see `NameNode`, `DataNode`,
`ResourceManager`, `NodeManager`, `SecondaryNameNode`.
* Python 3.8+ available on **every node** (mappers/reducers run as
subprocesses via Hadoop Streaming, so `python3` must be on each
NodeManager's `PATH`, or shipped with `-file`).
* `pandas` and `pyarrow` installed locally for the Parquet→CSV conversion,
data cleaning, and performance-benchmark steps (these are NOT run inside
Hadoop, only the plain Python mappers/reducers are).
* `matplotlib` installed locally for the visualization step.
* `hadoop-streaming-\*.jar` present under
`$HADOOP\_HOME/share/hadoop/tools/lib/`.

## 2\. Project Structure

```
taxi\_project/
├── README.md                       (this file)
├── commands.txt                    (every HDFS / Hadoop Streaming command, in order)
├── cleaning/
│   ├── convert\_parquet\_to\_csv.py   (Step 5 of assignment: Parquet -> CSV)
│   └── clean\_data.py               (Step 7: data cleaning + report)
├── mappers/                        (one mapper per analysis, a-i, + generic top-N mapper)
├── reducers/                       (matching reducer per analysis, + generic top-N reducer)
├── performance/
│   └── pandas\_benchmark.py         (Section 12: Pandas vs Hadoop comparison)
└── visualizations/
    └── generate\_charts.py          (Section 14: all 7 required charts)
```

## 3\. Column Layout Assumption

`clean\_data.py` writes the cleaned CSV with **no header inconsistency** —
every mapper assumes this exact column order (standard TLC Yellow Taxi
schema, 0-indexed):

```
0  VendorID              7  PULocationID          14 tolls\_amount
1  tpep\_pickup\_datetime  8  DOLocationID          15 improvement\_surcharge
2  tpep\_dropoff\_datetime 9  payment\_type          16 total\_amount
3  passenger\_count       10 fare\_amount           17 congestion\_surcharge
4  trip\_distance         11 extra                 18 airport\_fee
5  RatecodeID            12 mta\_tax
6  store\_and\_fwd\_flag    13 tip\_amount
```

If your Parquet→CSV conversion produces columns in a different order,
update the index constants at the top of each `mappers/\*.py` file before
running.

## 4\. Step-by-Step Execution

Full command reference is in **`commands.txt`** — follow it top to bottom.
Summary of the flow:

1. **Download** the 3 monthly Parquet files from the official TLC page.
2. **Convert** Parquet → CSV (`cleaning/convert\_parquet\_to\_csv.py`).
3. **Clean** the combined data and capture the printed report
(`cleaning/clean\_data.py`) — this report IS your Section 7 deliverable,
don't just discard the console output.
4. **Create the HDFS directory tree** (Section 6).
5. **Upload** raw + cleaned CSVs to HDFS; capture `-ls -h`, `-du -h`,
`dfsadmin -report`, and `fsck ... -blocks` output as screenshots.
6. **Run each MapReduce job** (a) through (i) via Hadoop Streaming — one
mapper/reducer pair per analysis, commands in `commands.txt` Step 6.
7. **Run the compulsory multi-stage job** (Step 7 in `commands.txt`):
Stage 1 = revenue-by-zone (analysis d); Stage 2 = generic top-N reducer
picks the Top 10 zones from Stage 1's HDFS output. Screenshot the
*intermediate* Stage-1 output as required evidence.
8. **Reuse the same top-N mapper/reducer** for Top10/Bottom10 pickup zones
and Top20 routes (by count and by revenue) — Step 8 in `commands.txt`.
9. **Screenshot YARN** (`http://localhost:8088/cluster`) for at least 2-3
jobs: Application ID, state, start/finish time, containers, final
status.
10. **Run the Pandas benchmark** on the same cleaned CSV and compare
against the Hadoop job's numbers from the YARN Job History UI —
fill in the comparison table from Section 12.
11. **Pull results locally** with `hdfs dfs -getmerge` and run
`visualizations/generate\_charts.py` to produce all 7 required charts.
12. **Archive** the raw input CSVs in HDFS.

## 5\. Testing a Mapper/Reducer Pair Locally First

Always sanity-check on a small sample before submitting to the cluster —
this exactly mimics what Hadoop Streaming does internally (mapper → sort
→ reducer):

```bash
head -1000 cleaned\_2023\_q2.csv > sample.csv
cat sample.csv | python3 mappers/mapper\_hourly.py | sort | python3 reducers/reducer\_hourly.py
```

All 20 mapper/reducer scripts in this project have already been
syntax-checked and functionally tested on synthetic sample data.

## 6\. Notes on the Anomaly Detection Job (analysis i)

`mapper\_anomaly.py` emits one line per triggered rule *and* a
`VALID\_TOTAL` / `ANOMALY\_TOTAL` counter line per record, so
`reducer\_anomaly.py`'s output directly gives you:

* counts per anomaly type (invalid\_distance, invalid\_fare,
invalid\_passenger\_count, invalid\_duration, extreme\_fare\_per\_mile,
invalid\_total, unparseable\_record)
* `ANOMALY\_TOTAL` and `VALID\_TOTAL`, from which you compute the
percentage of anomalous records for Business Question (j).

## 7\. Report Writing

Use the 16-section structure from the assignment (Section 18). Every
screenshot needs a short paragraph of interpretation next to it —
screenshots alone receive limited credit per Section 16.

## 8\. Academic Integrity

You are expected to be able to explain, for any script here: the
key/value design of the mapper, what happens during Shuffle \& Sort, the
reducer's aggregation logic, and how the HDFS input/output paths connect
the stages — especially for the multi-stage job in Step 7.

