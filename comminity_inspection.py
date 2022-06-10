import pandas as pd


# ERC721-----------
df_mod = pd.read_csv("./data/Modularity_class.csv")
df = pd.read_csv("./data/Top_10_PFP_holder_all.csv", sep=",", index_col=[0])

# create list of modularity classes
modClasses = df_mod["modularity_class"].unique().tolist()

# get users for each modularity class
final_list = []
for modclass in modClasses:
    df_curr = df_mod[df_mod["modularity_class"] == modclass]
    current_list = []
    for user in df_curr["Id"]:
        address = df_curr["Id"].values
        current_list.append(address)
    final_list.append(current_list)

print(len(df_mod))


# merge modularity class with users
df_merged = df.merge(df_mod, how="left", left_on="owner_of", right_on="Id")
df_merged.drop(columns=["Id"], inplace=True)

# create dataframe with current holdings (list all users foreach modularity class and sum up current pfp holdings)
class_mod_df = df_merged.groupby(["modularity_class", "name"]).size().unstack(fill_value=0)
class_mod_df["PFP_sum"] = class_mod_df.sum(axis=1)
class_mod_df.sort_values("PFP_sum", ascending=False)
class_mod_df["%_PFP_Hold"] = class_mod_df["PFP_sum"] / 127530 * 100
sorted_df = class_mod_df.sort_values("%_PFP_Hold", ascending=False)
print(sorted_df.to_string())


# ERC20-----------
df_mod = pd.read_csv("./data/Modularity_class_erc20.csv")
df = pd.read_csv("./data/Top_10_PFP_holder_all.csv", sep=",", index_col=[0])

# create list of modularity classes
modClasses = df_mod["modularity_class"].unique().tolist()

# get users for each modularity class
final_list = []
for modclass in modClasses:
    df_curr = df_mod[df_mod["modularity_class"] == modclass]
    current_list = []
    for user in df_curr["Id"]:
        address = df_curr["Id"].values
        current_list.append(address)
    final_list.append(current_list)

print(len(df_mod))


# merge modularity class with users
df_merged = df.merge(df_mod, how="left", left_on="owner_of", right_on="Id")
df_merged.drop(columns=["Id"], inplace=True)

# create dataframe with current holdings (list all users foreach modularity class and sum up current pfp holdings)
class_mod_df = df_merged.groupby(["modularity_class", "name"]).size().unstack(fill_value=0)
class_mod_df["PFP_sum"] = class_mod_df.sum(axis=1)
class_mod_df.sort_values("PFP_sum", ascending=False)
class_mod_df["%_PFP_Hold"] = class_mod_df["PFP_sum"] / 127530 * 100
sorted_df = class_mod_df.sort_values("%_PFP_Hold", ascending=False)
print(sorted_df.to_string())
