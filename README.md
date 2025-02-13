# Slash Analytics Take Home

Slash is looking to hire a strong analytics engineer. This take home is designed to test your skills in data analysis, modelling, and presentation.

## Setup
  - Clone the repo (and push to a private repo)
  - If you haven't used dbt before, check out the [dbt docs](https://docs.getdbt.com/docs/build/documentation)
  - Install uv 
  - `curl -LsSf https://astral.sh/uv/install.sh | sh`
  - Install python 
      - `uv python install`
  - Install python packages
    - `uv sync`
      - This should also set up your venv. If it doesn't, you can run `uv venv` to create it.
  - Place the 3 csv's in the `seeds` folder (these should be in a google drive folder) 
  - Test that dbt is working
    - `dbt seed && dbt compile && dbt run`
      - This should compile all the models and return no errors. Models should be in the `target/compiled` folder.
  - Run the example.py file to test that your duckdb connection is working. 
    - `python example.py`
      - This should print out the tables in your duckdb database (`dev.duckdb`), and show some sample select statements. 
      - [DuckDB docs](https://duckdb.org/docs/sql/introduction)
  - To run a specific model, you can do `dbt compile && dbt run --select <model_name>`


## Dashboarding tools

Feel free to use any dashboarding tool you are comfortable with. A few options:
- [Metabase](https://www.metabase.com/)
- [Rill](https://www.rilldata.com/)
- [Hex](https://hex.tech/)
- Or even a jupyter notebook

# Overview

## The dataset
You'll be working with three core datasets:

- card_events.csv: Card creation, update, and deletion events. The timestamp represents the time of the event. If there are any `pending_` events, then treat them as the original. (ex. `pending_activation` -> `activation`)
- card_transactions.csv: Detailed transaction data including merchant information and transaction amounts. A negative amount represents a settled transaction, and a postive amount represents a refunded transaction. Note that some amounts are in foreign currency. You'll need to convert them to USD.
- entity.csv: Entity and subaccount relationship data. One entity can have many subaccounts. A single entity is a single business, so we aren't super concerned about subaccount level data.

Once you follow the setup instructions, you'll have a local duckdb instance with all the datasets loaded under `dev.duckdb`. 

## The task(s)

Each of these should be their own separate dashboard.

#### Volume & GMV Metrics
Our north star metric is monthly processing volume (monthly settlement volume minus refund volume). We want to understand:

1. Monthly Processing Volume (GMV)
   - Overall volume trends 
   - Volume Retention 
       - [GMV Retention](https://www.reachcapital.com/2023/10/06/revisiting-retention-how-to-analyze-and-benchmark-marketplace-businesses/)

2. Merchant Analysis
   - Top merchants by volume
   - Customer concentration at top merchants 
   - Average transaction sizes
   - Merchant retention metrics

3. Customer Segmentation
   Slash serves two core customer types (Media Buyers and Ticket Brokers). Using transaction data (merchant descriptions and MCCs), we'll need to assign each entity a customer type (`Ticket Broker`, `Media Buyer`, or `Other`):
   - Customer type distribution over time
   - Spend patterns by customer type
   - Active customer counts by type
   - Retention metrics by customer type
   - Top merchants for each customer type
   - Refund rates by customer type

#### Card Utilization Metrics
Understanding card creation and usage patterns:

1. Card Activity
   - Active vs inactive cards
   - Utilization rates (% of created cards with settled spend)
   - Average spend per active card

2. Entity-Level Card Analysis
   Create an entity-level analysis showing:
   - Total cards created
   - Cards deleted/paused 
   - Overall card utilization
   - Total and average spend per card

3. Card Creation Patterns
   - Who are our highest card creators?
   - What is the relationship between card creation and spend?
   - How does utilization vary across customer segments?

#### SCP Program

Slash's SCP (Slash Card Program) is a tiered program that provides revenue benefits based on annual card spend. There is an additional cost for all cards for entities that are enrolled in the SCP program. If an entity is enrolled in SCP, then all their subsequent cards are automatically enrolled.

Here is a breakdown of the costs and benefits:

Enrollment Costs (Monthly card creation basis):
- $1.12 per card if < 50,000 cards enrolled in that calendar month
- $0.76 per card if 50,000-100,000 cards enrolled in that calendar month  
- $0.52 per card if 100,000-150,000 cards enrolled in that calendar month
- $0.34 per card if > 150,000 cards enrolled in that calendar month

Revenue Benefits (Calendar year spend basis):
- Tier 1 ($20k-$40k annual spend): +0.15% revenue per transaction
- Tier 2 ($40k-$100k annual spend): +0.20% revenue per transaction  
- Tier 3 ($100k-$250k annual spend): +0.30% revenue per transaction
- Tier 4 ($250k+ annual spend): +0.35% revenue per transaction

Important notes:
- When a user is upgraded to SCP, all their cards are enrolled at once
- Annual spend is calculated on calendar year basis
- Enrollment costs are based on monthly card creation amounts

**_Task:_** We need to balance costs and revenue to determine the optimal SCP enrollment strategy. We need to answer the following questions:
- What entities should be enrolled in SCP? How much would it cost, and how much additional revenue would it generate?
- What is the optimal SCP enrollment strategy?
    - ex. is it better to enroll everyone right away? as soon as they cross a certain tier? or some other strategy?
- What entities spend a lot, but should _not_ be enrolled in SCP?

# Evaluation

The challenge should take just a few hours. We’ll be primarily evaluating you based on your SQL / Python code (Python is not required, it is optional), data modelling, dashboarding, and presentation skills.

Please send us a link to your private repo once you're done, and the team will set up a call to review your work and give you a chance to present your findings. We'd love to see any scratch work or exploratory analysis you did to get to your final answers.

Good luck!
