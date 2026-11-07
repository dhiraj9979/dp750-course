# DP-750 Module 1: Designing and Building Data Pipelines

Exercise files for **Module 1** of *DP-750: Deploying and Maintaining Data Pipelines and Workloads*.

| File | Used in | What it does |
|---|---|---|
| `setup_course_data.py` | Clips 5 and 6 | Creates the schemas and landing volume, lands taxi files, and resets the module |
| `00_orchestrator.py` | Clip 5 | Runs bronze → silver → gold in order with `dbutils.notebook.run` |
| `01_bronze.py` | Clip 5 | Ingests raw CSV files into `bronze_trips` |
| `02_silver.py` | Clip 5 | Cleans and validates trips into `silver_trips` (can simulate a failure) |
| `03_gold.py` | Clip 5 | Aggregates fares by pickup zip into `gold_fares_by_zip` |
| `m1_c6_pipeline.sql` | Clip 6 | SQL for the Lakeflow Spark Declarative Pipeline |

---

## Prerequisites

- An **Azure subscription** and an **Azure Databricks workspace** on the **Premium** or **Trial (Premium)** pricing tier.
- **Unity Catalog** enabled on the workspace. This is the default for new workspaces; Step 4 confirms it.

---

## Clip 5: Building a Notebook Pipeline with Precedence Constraints

### Step 1: Download the files
1. On this repository's main page, click **Code → Download ZIP**.
2. Extract the ZIP and open the **`module1`** folder.

### Step 2: Create compute (one time)
1. In Databricks, go to **Compute → All-purpose compute → Create compute**.
2. Use these settings:
   - **Name:** `dp750-single-node`
   - **Policy:** Unrestricted
   - **Machine learning:** unchecked
   - **Runtime:** the latest **LTS** version
   - **Photon:** off
   - **Single node:** on
   - **Node type:** `Standard_D4ds_v5`, or any 4-core node your quota allows
   - **Terminate after:** 15 minutes
3. Click **Create** and wait for the **green dot**.

> **Quota error?** In the Azure portal, open **Quotas → Compute** and request more vCPUs for that VM family in your region. Alternatively, choose **Serverless** when you attach a notebook.

### Step 3: Create a folder and import the notebooks
1. Go to **Workspace → Home → Create → Folder**, and name it **`DP-750 Module 1`**.
2. Open the folder and click **⋮ → Import → File**. Import each of these files, one at a time:
   - `setup_course_data.py`
   - `00_orchestrator.py`
   - `01_bronze.py`
   - `02_silver.py`
   - `03_gold.py`
3. Check that the folder shows exactly these five notebooks, with **no `.py` extension**:
   `setup_course_data`, `00_orchestrator`, `01_bronze`, `02_silver`, `03_gold`.

> The orchestrator calls the other notebooks by name. If a name ends in `.py`, right-click it, choose **Rename**, and remove the extension.

### Step 4: Prepare the course data
1. Open **setup_course_data** and attach it to **dp750-single-node** using the compute selector at the top right.
2. Run **cell 1** (click into it and press **Shift + Enter**).
   - ✅ Expected: `Using catalog: <your catalog>` and `Ready: schemas dp750 and m1_declarative, and volume dp750.landing`
   - ❌ If the catalog is `hive_metastore`, Unity Catalog isn't enabled. Enable it before continuing.
3. Run **cell 2**.
   - ✅ Expected: `First batch landed: batch_001 (5,000 rows)`
4. *(Optional)* In **Catalog → your catalog → dp750 → Volumes → landing → taxi**, you should see a **batch_001** folder.

> Don't run cells 3 or 4 yet. Cell 3 is used in Clip 6, and cell 4 resets the module.

### Step 5: Explore the layer notebooks
- **`01_bronze`**: reads every CSV file in the landing volume, adds `_source_file` and `_ingested_at`, writes `bronze_trips`, and returns its row count.
- **`02_silver`**: casts columns, removes trips with invalid fares or distances, and **fails fast** if the data looks wrong. Its `fail` parameter simulates a failure.
- **`03_gold`**: aggregates trips and average fares by `pickup_zip` into `gold_fares_by_zip`.

### Step 6: Run the pipeline with the orchestrator
1. Open **00_orchestrator** and attach it to **dp750-single-node**.
2. Check the widgets at the top:
   - **catalog:** your catalog (filled in automatically)
   - **schema:** `m1_notebook`
   - **fail_silver:** `false`
3. Click **Run all**.
   - ✅ Expected output in the last cell:
     ```
     Step 1 complete: bronze wrote 5000 rows
     Step 2 complete: silver wrote <slightly fewer than 5000> rows
     Step 3 complete: gold wrote <about 90> rows
     ```
   - The **Notebook Workflows** table under the cell lists three runs, all **Succeeded**.
4. *(Optional)* **Catalog → your catalog → m1_notebook** shows `bronze_trips`, `silver_trips`, and `gold_fares_by_zip`.

**Try a failure:** set **fail_silver** to `true` and click **Run all**.
- ✅ Expected: the cell fails with *"Simulated data-quality failure in silver,"* and **Step 3 never runs**.
- Set **fail_silver** back to `false` afterwards.

### Step 7: Run the same steps as a job
1. Go to **Jobs & Pipelines → Create → Job**, and name it **`m1-notebook-pipeline`**.
2. Add three Notebook tasks, each on **dp750-single-node**:

   | Task name | Path | Depends on | Parameters |
   |---|---|---|---|
   | `bronze` | `DP-750 Module 1/01_bronze` | — | — |
   | `silver` | `DP-750 Module 1/02_silver` | `bronze` | `fail` = `false` |
   | `gold` | `DP-750 Module 1/03_gold` | `silver` | — |

3. Confirm that the graph shows **bronze → silver → gold**, then click **Run now**.
   - ✅ Expected: open the run from the **Runs** tab. All three tasks show **Succeeded**.

### Step 8: Watch a failure stop downstream tasks
1. On the **Tasks** tab, open **silver**, change `fail` to `true`, and click **Run now**.
2. Open the new run.
   - ✅ Expected:
     - **bronze:** Succeeded
     - **silver:** Failed, with *"Simulated data-quality failure in silver"*
     - **gold:** **Upstream failed** (skipped)
3. Set `fail` back to **`false`** on the silver task.

---

## Clip 6: Building the Same Pipeline with Lakeflow Spark Declarative Pipelines

> Complete Clip 5 first. This clip reuses the landing volume and `setup_course_data`.

### Step 1: Create the pipeline
1. Go to **Jobs & Pipelines → Create → ETL pipeline**. The pipeline editor opens with a sample file.
2. Click the title and rename the pipeline to **`m1-declarative-pipeline`**.
3. At the top right, click the **catalog / schema** indicator. Keep **your catalog**, and set the schema to **`m1_declarative`**.

### Step 2: Add the SQL
1. In the file panel, delete the sample **my_transformation.py** (**⋮ → Delete**).
2. On the **transformations** folder, click **+ → File**, and name it **`trips_medallion.sql`**.
3. Open `m1_c6_pipeline.sql` from the downloaded files in a text editor, copy everything, and paste it into the new file.
4. **Replace `dbw_dp750_course`** in the volume path with **your catalog name** (shown in cell 1 of `setup_course_data`).

### Step 3: Run the pipeline
1. Click **Run pipeline**. The first run takes a few minutes while compute starts.
   - ✅ Expected: in the **Tables** panel, all three tables succeed:
     - `bronze_trips`: about 5K rows
     - `silver_trips`: slightly under 5K rows
     - `gold_fares_by_zip`: about 90 rows
2. Open the **Pipeline graph** tab.
   - ✅ Expected: **bronze_trips → silver_trips → gold_fares_by_zip**, worked out automatically.

### Step 4: Check the expectation metrics
1. In the Tables panel, look at the **Expectations** column for `silver_trips`.
   - ✅ Expected: **2 unmet** expectations, with a **dropped** count for `valid_fare` and a **warning** count for `valid_distance`.
2. Click **silver_trips → Data** to see the clean, typed rows.

### Step 5: See incremental processing
1. In **setup_course_data**, run **cell 3**.
   - ✅ Expected: `New file landed: batch_<timestamp> (5,000 rows)`
2. In the pipeline, click **Run pipeline** again.
   - ✅ Expected: `bronze_trips` processes **about 5K new rows** in this update (only the new file), and its total grows to **about 10K**.

---

## Starting over

1. Delete the **m1-notebook-pipeline** job and the **m1-declarative-pipeline** pipeline (**Jobs & Pipelines → ⋮ → Delete**). Deleting the pipeline also removes its tables.
2. In **setup_course_data**, run **cell 4**, then cells **1** and **2** again.

> Keep your work if you're continuing the course. Clip 6 reuses the landing volume, and Module 2 builds on this job.
