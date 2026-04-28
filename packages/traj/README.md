# AAU AIS Trajectory

## Overview
This repository contains the architecture and data structures for the AAU AIS (Automatic Identification System) Trajectory project. It focuses on managing large-scale maritime movement data, specifically addressing the challenges of spatial data integrity across distributed database engines.

## Core Challenge: Geometry Table Duplication Across Engines

A critical architectural hurdle in this system is the **duplication of geometry tables** caused by our hybrid infrastructure spanning **DuckDB** and **DuckLake**. 

### Why This Is Hard to Fix

The duplication issue is exacerbated by three specific technical constraints:

1. **Lack of Atomic Cross-Engine Transactions**  
   Our architecture relies on two distinct database engines (DuckDB and DuckLake). Consequently, we cannot commit loads to both engines within a single atomic transaction. If a load operation fails after dimensions have been committed to one engine but before geometries are synchronized in the other, the system enters an inconsistent state.

2. **Absence of Natural Join Keys for Geometry**  
   Unlike attribute data, spatial data does not have a simple unique identifier that serves as an obvious join column. Checking for "equivalence" between two geometry objects (e.g., determining if two POLYGONs are identical) is computationally expensive and often requires tolerance thresholds. This makes it difficult to implement a reliable "idempotent" load strategy that skips already-existing geometries without scanning vast amounts of data.

3. **Retry Amplification (The "Double Load" Problem)**  
   Due to the lack of atomic transactions and reliable geometric deduplication, partial failures lead to data duplication. Specifically:
   - If the process loads **Dimensions** successfully (and commits them) but fails while loading the **Fact**, the loaded dimension data will remain in the system.
   - On the next retry attempt, the system sees the existing geometry dimension data but cannot easily match them to the *previously attempted* (but failed/uncommitted) geometries.
   - As a result, the same geometries are loaded again, leading to duplicate spatial records and bloating the dataset.

## Next Steps
- [ ] Investigate implementing a temporary staging area or logging layer to track load states across DuckDB and DuckLake.
- [ ] Evaluate if a surrogate key can be generated during the dimension load to facilitate matching during retry attempts.
- [ ] Optimize spatial equivalence checks to reduce the cost of identifying existing geometries.