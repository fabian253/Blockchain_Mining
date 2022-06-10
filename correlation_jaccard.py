import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sn


df = pd.read_csv("./data/Top_10_PFP_holders.csv", sep=",", index_col=[0])

# get unique Users
df_uniqueUsers = df["owner_of"]
df_uniqueUsers = df_uniqueUsers.drop_duplicates()

# get names of unique collections
uniqueCollections = df["name"].unique()

# create dataframe for each collection

name_df = df["token_address"].nunique()

for collection in uniqueCollections:
    name_df = df[df["name"] == collection]
    collection = str(collection)
    collection = collection.replace(" ", "_")
    print("Dataframe created for:", collection)
    globals()[collection] = name_df

# build matrices
df = df.drop_duplicates()

finallist = []
# build matrix without normalization
for collection in uniqueCollections:
    df1 = df[df["name"] == collection]
    currentList = []
    print("Collection: {}\t Unique Users: {}".format(collection, len(df1)))

    for i, collection2 in enumerate(uniqueCollections):
        df2 = df[df["name"] == collection2]
        print("Compare {}({}) with {}({}) ".format(collection, len(df1), collection2, len(df2)))
        merged = pd.merge(df1, df2, how="inner", on=["owner_of"])
        mergedCount = len(merged)
        print(mergedCount)
        currentList.append(mergedCount)
    finallist.append(currentList)

matrix = np.matrix(finallist)
print(matrix)

# build normalized matrix based on jaccard similarity
finallist = []

for collection in uniqueCollections:
    df1 = df[df["name"] == collection]
    currentList = []
    print("Collection: {}\t Unique Users: {}".format(collection, len(df1)))

    for i, collection2 in enumerate(uniqueCollections):
        df2 = df[df["name"] == collection2]
        print("Compare {}({}) with {}({}) ".format(collection, len(df1), collection2, len(df2)))
        merged = pd.merge(df1, df2, how="inner", on=["owner_of"])
        mergedCount = len(merged)
        len_all = len(df1) + len(df2)
        print(mergedCount)
        currentList.append(2 * mergedCount / len_all)
    finallist.append(currentList)

corrmatrix = np.matrix(finallist)
print(corrmatrix)


# plot correlation heatmap
sn.heatmap(corrmatrix, annot=True)
plt.gcf().set_size_inches(15, 10)
plt.xlabel("Collection")
plt.show()
