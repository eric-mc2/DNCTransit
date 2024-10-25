DNC Effect on Transit
==============================

**Motivation**

This is a portfolio project to learn a lot about econometric panel and time series models.

**Research Question**

I pose the specious question "Do Democrats really use transit?", and try to estimate the effect of the 2024 Chicago DNC on transit ridership, including rideshares.

**Theory of Change**

We assume the DNC is an exogeneous shock that inserts tens of thousands of Democrats into Chicago. Additionally, we assume they have to get to the DNC event locations at the United Center and McCormick Place. We assume these politically active Democrats are pro-transit and will choose public transit during their stay. Leading to a measurable spike in ridership during the event, and especially in the catchement of event locations.

**Modeling Threats**

There are numerous reasons not to believe this model. It's just an exercise after all. I try to list and test them in the accompanying notebooks.

**Tags:**
econometrics, time series modeling, Socrata, Amazon S3, open data, panel data, auto-regression, spatial data

**Replication**

1. Install scripts in `src` as a local package using `pip install -e .` from the project root directory.

Project Structure
-----------------

```
.
├── AUTHORS.md
├── LICENSE
├── README.md
├── data                    <- (not tracked by git. maybe tracked by DVC)
│   ├── raw                 <- The original, immutable data dumps from primary or third-party sources.
│   ├── interim             <- Intermediate data that has been transformed.
│   ├── final               <- The final, canonical data sets for modeling.
│   └── replication         <- Final data that was used in a paper, talk, etc.
|       └── my-paper        <- One folder per task/presentation
├── docs                    <- Documentation, e.g. data dictionaries, memos, project notes
├── notebooks               <- Interactive python or R notebooks
│   ├── exploratory         <- One-offs (one script per task)
│   └── replication         <- Report-generating scripts that were used in a paper, talk, etc. (one script per presentation)
├── pipeline                <- The actual data ETL process (a NUMBERED mix of scripts and notebooks)
├── reports                 <- For all non-data project outputs
|   ├── exploratory         <- One-offs
|   |   └── my-task         <- One folder per task/presentation
|   |       ├── figures     <- Visualizations
|   |       ├── tables      <- Descriptive tables
|   |       └── regressions <- Model outputs
│   └── replication         <- Outputs that were presented in a talk, paper, etc. (promote exploratory subfolders into here)
├── setup.py                <- Allows importing python files from src into notebooks
└── src                     <- Refactored helper code
    ├── data                <- Resusable data ETL steps
    ├── stats               <- Source code for modeling and statistics
    ├── viz                 <- Scripts for visualisation of your results, e.g., matplotlib, ggplot2 related.
    └── util                <- Any helper scripts go here
```