# Blockchain Mining

This project deals with the analysis of the Ethereum Blockchain. For this purpose, three research questions are addressed:

1. How is attending POAP events correlated with owning NFTs?
2. Which communities can be observed based on transactional data and what are their properties?
3. How predictive are economic features regarding social features?

For each of the research quesions there are different files used.

## Data Collection
There are different python files for collecting data. These store the data in a MySql database. A backup of this database can be found under the following Google Drive link. You can import this backup with the help of "mysqldump" (https://dev.mysql.com/doc/refman/8.0/en/mysqldump.html). The name of the backup file is: "blockchain_mining_db.sql"

Google Drive: https://drive.google.com/drive/folders/1EkpMpJ4u1mlO9_Me9m5CosBc1lwxzFEm?usp=sharing

You can replace the folders "data" and "datasets" in the repository directly with the folders from Google Drive to have the needed data for research question 2 and 3.

The following python scripts are for data collection:

- "nft_data_collection" -> collect the nft owner data
- "poap_data_collection" -> collect the poap owner data
- "ft_balance_data_collection.py" -> collect the ft balances
- "nft_balance_data_collection" -> collect the nft balances
- "ft_transfer_data_collection" -> collect the ft transfers
- "nft_transfer_data_collection" -> collect the nft transfers
- "poap_transfer_data_collection" -> collect the nft transfer of poap owners


## Research Question 1

## Research Question 2

## Research Question 3
