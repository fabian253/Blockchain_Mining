import config
from connectors.nft_database_connector import NftDatabaseConnector
import pandas as pd
import json
import flatdict

if __name__ == "__main__":
    nft_db_cnx = NftDatabaseConnector(
        config.MYSQL_DB_HOST, config.MYSQL_DB_USER, config.MYSQL_DB_PASSWORD, config.MYSQL_DB_NAME)

    nft_data_df = nft_db_cnx.query_all_nft_data()

    sample_size = 100000
    # build sample
    nft_data_sample = nft_data_df.sample(sample_size)

    metadata_list = []
    # iterate through all indeces in sample
    for i, row in nft_data_sample.iterrows():
        metadata = row["metadata"]

        # remove line breaks (line feeds (LF)) from metadata
        metadata = " ".join(metadata.splitlines())

        # only use nfts where metadata is present
        if metadata != "None":
            try:
                # get metadata
                metadata_dict = json.loads(metadata)
                # insert name of nft collection into metadata
                metadata_dict["nft_collection"] = nft_data_sample["name"][i]
                # flatten dict (with lists)
                flat_metadata_dict = flatdict.FlatterDict(
                    metadata_dict, delimiter=".")

                pre_processed_metadata_dict = {}
                # iterate through all items in dict
                for key, value in flat_metadata_dict.items():
                    # split sub keys (relevant for attributes with traittypes)
                    sub_keys = str(key).split(".")
                    # rebuild keys by removing numbers and inserting key names from trait_types
                    if sub_keys[0] == "attributes" and len(sub_keys) > 1:
                        if sub_keys[1].isnumeric():
                            if sub_keys[2] == "trait_type":
                                pre_processed_metadata_dict[f"{sub_keys[0]}.{value}".lower(
                                )] = flat_metadata_dict[f"{sub_keys[0]}.{sub_keys[1]}.value"]
                    else:
                        pre_processed_metadata_dict[key.lower()] = value
                # append new dict to list
                metadata_list.append(pre_processed_metadata_dict)
            except json.decoder.JSONDecodeError as err:
                # catch exception from decoding json
                print(f"Unexpected {err=}, {type(err)=}")
                char_num = err.pos
                print(f"acsii code: {ord(metadata[char_num:char_num+1])}")
                # print(metadata)
            except TypeError:
                print(f"Error index: {i}")

    # build dataframe from pre-processed metadata (list of dicts)
    metadata_df = pd.DataFrame(metadata_list)

    percent_missing = metadata_df.isnull().sum() * 100 / len(metadata_df)
    missing_value_df = pd.DataFrame({'column_name': metadata_df.columns,
                                     'percent_missing': percent_missing})
    missing_value_df.sort_values('percent_missing', inplace=True)
    
    print(missing_value_df)

    # print(metadata_df)

    if False:
        with open(f"metadata_test.json", "w") as f:
            metadata_df.to_json(path_or_buf=f, orient="records")
            #json.dump(metadata_df.to_dict("records"), f)
