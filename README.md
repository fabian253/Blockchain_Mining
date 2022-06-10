# Blockchain Mining

This project deals with the analysis of the Ethereum Blockchain. For this purpose, three research questions are addressed:

1. How is attending POAP events correlated with owning NFTs?
2. Which communities can be observed based on transactional data and what are their properties?
3. How predictive are economic features regarding social features?

For each of the research quesions there are different files used.

## Packages

There are several packages that are required. The latest version should always be used.

- numpy
- pandas
- matplotlib
- seaborn
- networkit
- tabulate

## Config

There is a "config.py" file included in the git. However, the following attributes must still be adjusted in this file:

- MYSQL_DB_HOST -> mysql host ("localhost")
- MYSQL_DB_USER -> mysql user ("user")
- MYSQL_DB_PASSWORD -> mysql password
- MORALIS_API_URL -> use your own api key (https://moralis.io)

## Data Collection
There are different python files for collecting data. These store the data in a MySql database. A backup of this database can be found under the following Google Drive link. You can import this backup with the help of "mysqldump" (https://dev.mysql.com/doc/refman/8.0/en/mysqldump.html). The name of the backup file is: "blockchain_mining_db.sql"

Google Drive: https://drive.google.com/drive/folders/1EkpMpJ4u1mlO9_Me9m5CosBc1lwxzFEm?usp=sharing

You can replace the folders "data" and "datasets" in the repository directly with the folders from Google Drive to have the needed data for research question 2 and 3.

The following python scripts are for data collection:

- "nft_data_collection.py" -> collect the nft owner data
- "poap_data_collection.py" -> collect the poap owner data
- "ft_balance_data_collection.py" -> collect the ft balances
- "nft_balance_data_collection.py" -> collect the nft balances
- "ft_transfer_data_collection.py" -> collect the ft transfers
- "nft_transfer_data_collection.py" -> collect the nft transfers
- "poap_transfer_data_collection.py" -> collect the nft transfer of poap owners


## Research Question 1

The following python scripts are used for research question 1:

- "poap_nft_analysis.py" -> perform analysis and export to results folder

## Research Question 2

The following python scripts are used for research question 2:

- "correlation_jaccard.py" -> used for jaccard correlation
- "community_detection.py" -> used for detecting commuities
- "comminity_inspection.py" -> used for inspecting communities

In addition, there are files that store the graphs:

- "graph_ERC20.gml" -> stores the ft graph
- "graph_ERC721.gml" -> stores the nft graph

## Research Question 3

The following python scripts are used for research question 3:

- "ft_nft_recommender.py" -> contains a class wit the NftRecommender
- "ft_nft_recommender_evaluation.py" -> perform hyperparameter optimization and evaluation of NftRecommender


