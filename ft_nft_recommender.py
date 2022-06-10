from connectors.blockchain_database_connector import BlockchainDatabaseConnector
from sklearn.metrics.pairwise import pairwise_distances
from sklearn.model_selection import train_test_split
import pandas as pd
import numpy as np


class NftRecommender:

    def __init__(self) -> None:
        self.ft_balance_data_df: pd.DataFrame = None
        self.nft_balance_data_df: pd.DataFrame = None
        self.wallet_similarity_matrix: pd.DataFrame = None

    # load ft balance data from db
    def load_ft_balance_data_from_db(self, db_cnx: BlockchainDatabaseConnector, table_name):
        self.ft_balance_data_df = db_cnx.query_ft_balance_data(table_name)

    # load ft balance data from pkl file
    def load_ft_balance_data_from_pkl(self, file_name):
        self.ft_balance_data_df = pd.read_pickle(file_name)

    # load nft balance data from db
    def load_nft_balance_data_from_db(self, db_cnx: BlockchainDatabaseConnector, table_name):
        self.nft_balance_data_df = db_cnx.query_nft_balance_data(table_name)

    # load nft balance data from pkl file
    def load_nft_balance_data_from_pkl(self, file_name):
        self.nft_balance_data_df = pd.read_pickle(file_name)

    # save ft balance data to pkl file
    def save_ft_balance_data_to_pkl(self, file_name):
        self.ft_balance_data_df.to_pickle(file_name)

    # save nft balance data to pkl file
    def save_nft_balance_data_to_pkl(self, file_name):
        self.nft_balance_data_df.to_pickle(file_name)

    # preprocess ft and nft balance data
    def preprocess_data(self, ft_token_count_threshold=5, nft_token_count_threshold=5, ft_wallet_count_threshold=2, nft_wallet_count_threshold=2):
        # filter tokens that are not hold by more than threshold wallets
        ft_token_count_df = self.ft_balance_data_df.groupby(
            ["token_address"], as_index=False).size()
        nft_token_count_df = self.nft_balance_data_df.groupby(
            ["token_address"], as_index=False).size()

        ft_token_count_filter = ft_token_count_df[ft_token_count_df["size"]
                                                  >= ft_token_count_threshold]
        nft_token_count_filter = nft_token_count_df[nft_token_count_df["size"]
                                                    >= nft_token_count_threshold]

        preprocessed_ft_balance_df = self.ft_balance_data_df[self.ft_balance_data_df["token_address"].isin(
            ft_token_count_filter["token_address"])]
        preprocessed_nft_balance_df = self.nft_balance_data_df[self.nft_balance_data_df["token_address"].isin(
            nft_token_count_filter["token_address"])]

        # filter wallets who don't own a specific number of tokens
        ft_wallet_count_df = preprocessed_ft_balance_df.groupby(
            ["owner_of"], as_index=False).size()
        nft_wallet_count_df = preprocessed_nft_balance_df.groupby(
            ["owner_of"], as_index=False).size()

        ft_wallet_count_filter = ft_wallet_count_df[ft_wallet_count_df["size"]
                                                    >= ft_wallet_count_threshold]
        nft_wallet_count_filter = nft_wallet_count_df[nft_wallet_count_df["size"]
                                                      >= nft_wallet_count_threshold]

        preprocessed_ft_balance_df = preprocessed_ft_balance_df[preprocessed_ft_balance_df["owner_of"].isin(
            ft_wallet_count_filter["owner_of"])]
        preprocessed_nft_balance_df = preprocessed_nft_balance_df[preprocessed_nft_balance_df["owner_of"].isin(
            nft_wallet_count_filter["owner_of"])]

        # filter data for wallets which are in both sets
        ft_distinct_wallets = preprocessed_ft_balance_df["owner_of"].unique()
        nft_distinct_wallets = preprocessed_nft_balance_df["owner_of"].unique()

        distinct_wallet_filter = list(
            set(ft_distinct_wallets) & set(nft_distinct_wallets))

        preprocessed_ft_balance_df = preprocessed_ft_balance_df[preprocessed_ft_balance_df["owner_of"].isin(
            distinct_wallet_filter)]
        preprocessed_nft_balance_df = preprocessed_nft_balance_df[preprocessed_nft_balance_df["owner_of"].isin(
            distinct_wallet_filter)]

        self.ft_balance_data_df = preprocessed_ft_balance_df
        self.nft_balance_data_df = preprocessed_nft_balance_df

    # build wallet similarity matrix based on ft balance data
    def build_wallet_similarity_matrix(self):
        ft_token_wallet_df = self.ft_balance_data_df.groupby(
            ["owner_of", "token_address"], as_index=False).size()

        wallet_ft_matrix = ft_token_wallet_df.pivot(
            index="token_address", columns="owner_of", values="size").fillna(0)

        # wallet similarity matrix
        wallet_correlation_matrix = 1 - \
            pairwise_distances(wallet_ft_matrix.T, metric="cosine")

        wallet_correlation_matrix[np.isnan(wallet_correlation_matrix)] = 0

        distinct_wallets = ft_token_wallet_df["owner_of"].unique()

        self.wallet_similarity_matrix = pd.DataFrame(
            wallet_correlation_matrix, index=distinct_wallets, columns=distinct_wallets)

    # get similar wallets by "top k" method
    def __get_similar_wallets_by_k(self, wallet_address, k=10) -> pd.DataFrame:
        similar_wallet_df = self.wallet_similarity_matrix.nlargest(
            k+1, wallet_address)[wallet_address]

        similar_wallet_df = similar_wallet_df.drop(wallet_address, axis=0)

        return similar_wallet_df

    # get similar wallets by "similarity threshold" method
    def __get_top_similar_owners_by_threshold(self, wallet_address, t=0.2) -> pd.DataFrame:
        similar_wallet_df = self.wallet_similarity_matrix[
            self.wallet_similarity_matrix[wallet_address] >= t][wallet_address]

        similar_wallet_df = similar_wallet_df.drop(wallet_address, axis=0)

        return similar_wallet_df

    # get top k nft projects by similar wallets
    def __get_top_k_nft(self, wallet_address, similar_wallet_df: pd.DataFrame, k=10) -> pd.DataFrame:
        nft_token_wallet_df = self.nft_balance_data_df.groupby(
            ["owner_of", "token_address"], as_index=False).size()

        similar_wallets = similar_wallet_df.index.to_list()

        top_k_nft_df = nft_token_wallet_df[nft_token_wallet_df["owner_of"].isin(
            similar_wallets)]

        top_k_nft_df = top_k_nft_df.merge(
            similar_wallet_df, how="left", left_on="owner_of", right_index=True)

        top_k_nft_df = top_k_nft_df.rename(
            columns={wallet_address: "similarity"})

        top_k_nft_df["sim_rating"] = top_k_nft_df["size"] * \
            top_k_nft_df["similarity"]

        top_k_nft_df = top_k_nft_df.groupby(
            ["token_address"], as_index=False).sum()

        top_k_nft_df = top_k_nft_df.nlargest(k, "sim_rating")

        nft_name_mapping_dict = dict(
            self.nft_balance_data_df[["token_address", "name"]].values)

        top_k_nft_df["token_name"] = top_k_nft_df["token_address"].map(
            nft_name_mapping_dict)

        return top_k_nft_df

    # predict nft recommendations
    def predict_nft_recommendations(self, wallet_address, owner_method="top_k", similar_owners_k=10, similarity_threshold=0.2, nft_projects_k=10):
        if owner_method == "top_k":
            similar_wallet_df = self.__get_similar_wallets_by_k(
                wallet_address, similar_owners_k)
        elif owner_method == "threshold":
            similar_wallet_df = self.__get_top_similar_owners_by_threshold(
                wallet_address, similarity_threshold)

        top_k_nft_df = self.__get_top_k_nft(
            wallet_address, similar_wallet_df, nft_projects_k)

        return top_k_nft_df

    # predict random nft recommendations
    def predict_random_nft_recommendations(self, nft_projects_k=10) -> pd.DataFrame:
        distinct_nft_df = self.nft_balance_data_df.groupby(
            ["token_address", "name"], as_index=False).size()

        distinct_nft_df = distinct_nft_df.rename(
            {"name": "token_name"}, axis=1)

        distinct_nft_df = distinct_nft_df[["token_address", "token_name"]]

        random_nft_recommendations = distinct_nft_df.sample(nft_projects_k)

        return random_nft_recommendations

    # get wallet testset, use random_state to get same set
    def get_wallet_testset(self, test_size=0.1, random_state=-1):
        distinct_wallets = self.ft_balance_data_df["owner_of"].unique()

        if random_state == -1:
            train_wallets, test_wallets = train_test_split(
                distinct_wallets, test_size=test_size)
        else:
            train_wallets, test_wallets = train_test_split(
                distinct_wallets, test_size=test_size, random_state=random_state)

        return test_wallets

    # calc recommendation hit metrics (hit rate, hit ratio) -> return both
    def calc_recommendation_hit_metrics(self, wallet_address, nft_recommendation_df: pd.DataFrame):
        nft_wallet_holding = self.nft_balance_data_df[self.nft_balance_data_df["owner_of"] == wallet_address]

        nft_wallet_holding = nft_wallet_holding.groupby(
            ["token_address", "name"], as_index=False).size()

        nft_recommendation_list = nft_recommendation_df["token_address"].to_list(
        )

        nft_holding_list = nft_wallet_holding["token_address"].to_list()

        nft_hits = [
            nft for nft in nft_recommendation_list if nft in nft_holding_list]

        if len(nft_recommendation_list) < len(nft_holding_list):
            max_possible_hits = len(nft_recommendation_list)
        else:
            max_possible_hits = len(nft_holding_list)

        hit_ratio = len(nft_hits) / max_possible_hits

        return len(nft_hits), hit_ratio
