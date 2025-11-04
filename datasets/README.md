# Advanced E-Commerce Analysis Project

## Dataset  
Due to the large size of the CSV files, they cannot be uploaded directly to this repository. You can download them from the official source:  
[Download the Olist Brazilian E-Commerce Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

**Note:**   
* Make sure the files are named according to what the scripts/ETL process expects (e.g., `dataset_part1.csv`, `dataset_part2.csv`, etc.).  
* If the files are split or compressed, extract them before running the full pipeline.
* **Important:** In the `order_reviews.csv` file, the two review text columns were removed because they contained special characters that caused issues during processing.

**Structure:**  
- **Bronze Layer:** Raw CSV ingestion.  
- **Silver Layer:** Cleaned and transformed data for analytics.  
- **Gold Layer:** High-level analytical views and business insights.  

## Usage  
1. Place the dataset CSV files in the `data/` folder.  
2. Run the ETL scripts for the Bronze → Silver → Gold layers.  
3. Explore the Gold layer views for analytics queries and insights.
