# 🎬 IMDB Movie Market Analysis — PySpark on Databricks

![PySpark](https://img.shields.io/badge/PySpark-3.x-orange)
![Databricks](https://img.shields.io/badge/Platform-Databricks-red)
![Python](https://img.shields.io/badge/Python-3.x-blue)
![Domain](https://img.shields.io/badge/Domain-Entertainment%20Analytics-purple)
![Dataset](https://img.shields.io/badge/Dataset-IMDB%20Top%201000-green)

---

## 📌 Project Overview

This project performs **large-scale Exploratory Data Analysis (EDA)** on the IMDB Movie Dataset using **Apache PySpark on Databricks**, analysing box office revenue trends, genre performance, audience ratings, and yearly market patterns across the film industry.

The analysis demonstrates how distributed computing with PySpark handles movie market data at scale — using DataFrame operations, Window Functions, and aggregations that would be inefficient in standard pandas.

---

## 🗂️ Analysis Pipeline

```
IMDB CSV Dataset (Databricks FileStore)
        ↓
Data Ingestion — spark.read.csv with schema inference
        ↓
Data Cleaning — Null detection, type casting, mean imputation
        ↓
Exploratory Analysis — Ratings, Revenue, Genre, Year-wise trends
        ↓
Window Functions — Top-N movies per year using row_number()
        ↓
Genre Explosion — Multi-genre split & revenue aggregation
        ↓
Business Insights — Highest/lowest revenue genres, top rated films
```

---

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| **Apache PySpark** | Distributed DataFrame operations & SQL analytics |
| **Databricks Community Edition** | Cloud notebook environment & Spark cluster |
| **PySpark SQL Functions** | Aggregations, window functions, explode, split |
| **IMDB Movie Dataset** | Source data — titles, genres, ratings, revenue, metascore |

---

## 📊 Dataset

**Source:** IMDB Movie Dataset (Kaggle)
**File:** `IMDB_Movie_Data.csv`

| Column | Type | Description |
|---|---|---|
| Title | String | Movie title |
| Genre | String | Comma-separated genre tags |
| Year | Integer | Release year |
| Runtime (Minutes) | Integer | Film duration |
| Rating | Float | IMDB audience rating (0–10) |
| Votes | Integer | Number of votes cast |
| Revenue (Millions) | Float | Box office revenue in USD millions |
| Metascore | Integer | Critic score (0–100) |
| Director | String | Director name |

---

## 🔍 Analysis Performed

### 1️⃣ Data Ingestion & Schema Handling
- Loaded CSV with `inferSchema=True`, handling multi-line fields, escaped quotes, and leading/trailing whitespace
- Cast all numeric columns (`Year`, `Runtime`, `Rating`, `Votes`, `Revenue`, `Metascore`) from string to correct types using `.withColumn()` + `.cast()`

### 2️⃣ Null Value Treatment
- Detected nulls across all columns using `count(when(col.isNull()))` pattern
- Applied **mean imputation** strategy for `Revenue (Millions)` and `Metascore` — chosen over dropping rows to preserve dataset size and avoid revenue bias

### 3️⃣ Ratings Analysis
- Rounded ratings to 1 decimal place for clean grouping
- Extracted top 10 highest-rated movies via `.orderBy(col("Rating").desc())`
- Computed platform-wide average rating using `.agg(avg("Rating"))`
- Identified highest and lowest rated individual films

### 4️⃣ Revenue Analysis
- Calculated **yearly total box office revenue** using `groupBy("Year").agg(sum("Revenue"))`
- Extracted **Top 10 highest-grossing films of 2016** using filter + orderBy
- Identified **lowest revenue film of 2009** with director attribution

### 5️⃣ Window Function — Top 3 Movies Per Year
```python
windowSpec = Window.partitionBy("Year").orderBy(col("Revenue (Millions)").desc())
df_with_rank = df.withColumn("Rank", row_number().over(windowSpec))
top3_per_year = df_with_rank.filter(df_with_rank["rank"] <= 3)
```
Used `row_number()` over a year-partitioned window to rank films within each year — a real-world analytical pattern used in industry reporting.

### 6️⃣ Genre Revenue Analysis
- Exploded multi-genre strings into individual rows using `explode(split(col("Genre"), ", "))`
- Aggregated total revenue per genre across the entire dataset
- Identified the **highest and lowest revenue-generating genres**

---

## 🔑 Key Analytical Findings

- **Window functions** were used to rank top-grossing films within each year — avoiding expensive self-joins
- **Genre explosion** technique demonstrated handling of multi-valued columns — a common real-world data cleaning challenge
- **Mean imputation** was applied for revenue nulls rather than row deletion — preserving analytical integrity for revenue trend analysis
- **Top 10 films of 2016** and **lowest revenue film of 2009** extracted as targeted business queries
- Genre-level revenue aggregation revealed which film categories drive the most box office value vs which underperform

---

## 📁 Repository Structure

```
databricks-pyspark-movie-market-analysis/
│
├── Databricks Test Notebook.ipynb    # Full PySpark EDA notebook (Databricks export)
├── Spark Test Notebook.ipynb         # Spark environment test & setup validation
└── README.md
```

---

## 🚀 How to Reproduce

### Option A — Databricks Community Edition (Recommended)
1. Sign up for free at [community.cloud.databricks.com](https://community.cloud.databricks.com)
2. Create a new cluster (Runtime 12.x or above)
3. Upload `IMDB_Movie_Data.csv` to **FileStore** → `Data` → `Upload`
4. Import `Databricks Test Notebook.ipynb` into your workspace
5. Attach to cluster and **Run All**

### Option B — Local PySpark
```bash
pip install pyspark
```
Update the file path from `/FileStore/tables/IMDB_Movie_Data-1.csv` to your local path, then run the notebook in Jupyter.

---

## 💡 PySpark Concepts Demonstrated

| Concept | Where Used |
|---|---|
| `inferSchema` + type casting | Data ingestion & cleaning |
| `count(when(isNull()))` | Null detection across all columns |
| `fillna()` with mean | Revenue & Metascore imputation |
| `groupBy().agg()` | Yearly revenue, genre revenue |
| `Window.partitionBy().orderBy()` | Top-N films per year |
| `row_number()` over window | Yearly revenue ranking |
| `explode(split())` | Multi-genre column expansion |
| `orderBy().limit()` | Top-K queries |
| `filter()` | Year and genre-based subsetting |

---

## 👩‍💻 Author

**Shreya Jadhav**
Data Analyst | Python · SQL · PySpark · Power BI · LangChain
📧 shreyajune03pune@gmail.com
🔗 [LinkedIn](https://linkedin.com/in/jadhavshreya03pune) | [GitHub](https://github.com/ShreyaVJadhav3)

---

## 📌 Related Projects

- 🛡️ [Project Sentinel — E-Commerce Fraud Detection](https://github.com/ShreyaVJadhav3/project---sentinel---ecommerce---fraud---detection-) — Detected 586 ghost orders & R$80,860 revenue leakage across 99K transactions using SQL, Python & Power BI
- 🤖 [RAG Pipeline — Personal Document Chatbot](https://github.com/ShreyaVJadhav3/rag-pipeline) — Production-style GenAI pipeline using LangChain, FAISS & ChromaDB
- 🎬 [Movie Recommender System](https://github.com/ShreyaVJadhav3/movie_recommender_system) — Content-based filtering using TF-IDF & Cosine Similarity on 5,000 films
