Production-grade Spark requirements (YOU solve these)

Now the fun part.
These are concrete, solvable Spark Scala tasks using your datasets.

⸻

🔥 Requirement 1: Transaction Deduplication

Problem
•	Same transaction_id may appear multiple times
•	Keep the latest ingest_time per transaction

Constraints
•	Must work with skew
•	Must not full-scan history every day

👉 Forces:
•	Window functions
•	Incremental logic
•	Partition pruning

⸻

🔥 Requirement 2: Daily Customer Spend (Correct with Late Data)

Problem
•	Compute daily spend per customer
•	Late transactions can arrive 3 days late
•	Must update historical aggregates

👉 Forces:
•	Reprocessing windows
•	Idempotent jobs
•	Event-time thinking

⸻

🔥 Requirement 3: SCD2 Customer Join

Problem
•	Join transactions to customer attributes
•	Correct attributes as of transaction time

👉 Forces:
•	Range joins
•	Window logic
•	Join optimization

⸻

🔥 Requirement 4: Fraud-like Velocity Signal

Problem
•	Flag accounts with:
•	5 txns in 2 minutes
•	amount spike vs rolling avg

👉 Forces:
•	Window aggregations
•	State management (batch style first)

⸻

🔥 Requirement 5: Data Quality Pipeline

Problem
•	Separate:
•	valid transactions
•	quarantined transactions
•	Emit metrics

👉 Forces:
•	Schema enforcement
•	Conditional routing
•	Accumulators / metrics



1️⃣ Convert your DataGenerator into a Spark-based scalable generator
2️⃣ Give exact Scala Spark skeletons for each requirement (no solution logic)
3️⃣ Design performance experiments (AQE on/off, salting, broadcast)
4️⃣ Turn this into an interview-grade Spark project

