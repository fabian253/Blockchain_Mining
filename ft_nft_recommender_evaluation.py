from connectors.blockchain_database_connector import BlockchainDatabaseConnector
from ft_nft_recommender import NftRecommender
import pandas as pd
import config
import json

# hyperparameters
similar_wallets_k = [10, 50, 100, 200]
similarity_threshold_t = [0.1, 0.2, 0.3]
wallet_sim_method = ["top_k", "threshold"]
nft_projects_k = [5, 10, 20, 100]
test_size = 0.1
random_state = 42


# perform hyperparameter optimization (write to file)
def perform_hyperparameter_optimization(nft_recommender: NftRecommender, test_wallet_list: list, hyperparameter_data_file_name):
    # create new file (clear old)
    with open(hyperparameter_data_file_name, "w") as f:
        json.dump([], f)

    # iterate through wallets from testset
    for idx, wallet in enumerate(test_wallet_list):

        output_data = []

        for nft_k in nft_projects_k:
            for method in wallet_sim_method:
                if method == "top_k":
                    for owner_k in similar_wallets_k:
                        try:
                            nft_recommendations = nft_recommender.predict_nft_recommendations(
                                wallet, method, owner_k, 0, nft_k)
                            hit_rate, hit_ratio = nft_recommender.calc_recommendation_hit_metrics(
                                wallet, nft_recommendations)
                            output = {"wallet_address": wallet,
                                      "nft_projects_k": nft_k,
                                      "wallet_sim_method": method,
                                      "similar_wallet_k": owner_k,
                                      "similarity_threshold": 0,
                                      "hit_rate": hit_rate,
                                      "hit_ratio": hit_ratio}
                            output_data.append(output)
                        except Exception:
                            output = {"wallet_address": wallet,
                                      "nft_projects_k": nft_k,
                                      "wallet_sim_method": method,
                                      "similar_wallet_k": owner_k,
                                      "similarity_threshold": 0,
                                      "error": True}
                            output_data.append(output)

                elif method == "threshold":
                    for threshold in similarity_threshold_t:
                        try:
                            nft_recommendations = nft_recommender.predict_nft_recommendations(
                                wallet, method, 0, threshold, nft_k)
                            hit_rate, hit_ratio = nft_recommender.calc_recommendation_hit_metrics(
                                wallet, nft_recommendations)
                            output = {"wallet_address": wallet,
                                      "nft_projects_k": nft_k,
                                      "wallet_sim_method": method,
                                      "similar_wallet_k": 0,
                                      "similarity_threshold": threshold,
                                      "hit_rate": hit_rate,
                                      "hit_ratio": hit_ratio}
                            output_data.append(output)
                        except Exception:
                            output = {"wallet_address": wallet,
                                      "nft_projects_k": nft_k,
                                      "wallet_sim_method": method,
                                      "similar_wallet_k": 0,
                                      "similarity_threshold": threshold,
                                      "error": True}
                            output_data.append(output)

        with open(hyperparameter_data_file_name, "r") as f:
            out_data = json.load(f)

        out_data.extend(output_data)

        with open(hyperparameter_data_file_name, "w") as f:
            json.dump(out_data, f)

        print(f"wallet: {wallet} done [{idx+1}/{len(test_wallet_list)}]")


# evaluate nft recommender by hyperparameter optimization (save to file)
def evaluate_nft_recommender(hyperparameter_data_file_name, nft_recommender_eval_file_name):
    with open(hyperparameter_data_file_name, "r") as f:
        hyperparameter_opt_data = json.load(f)

    hyperparameter_eval_data = [
        data for data in hyperparameter_opt_data if not "error" in data]

    hyperparameter_eval_df = pd.DataFrame(hyperparameter_eval_data)

    hyperparameter_eval_df = hyperparameter_eval_df.groupby(
        ["nft_projects_k", "wallet_sim_method", "similar_wallet_k", "similarity_threshold"], as_index=False).mean()

    hyperparameter_eval_df = hyperparameter_eval_df.sort_values(
        by=["nft_projects_k", "hit_ratio"], ascending=False)

    hyperparameter_eval_df.to_csv(nft_recommender_eval_file_name)


# evaluate nft random recommender (baseline for comparison)
def evaluate_random_nft_recommender(nft_recommender: NftRecommender, test_wallet_list: list, nft_random_recommender_eval_file_name):
    evaluation_data = []

    # iterate through wallets from testset
    for idx, wallet in enumerate(test_wallet_list):

        for nft_k in nft_projects_k:
            nft_recommendations = nft_recommender.predict_random_nft_recommendations(
                nft_k)
            hit_rate, hit_ratio = nft_recommender.calc_recommendation_hit_metrics(
                wallet, nft_recommendations)
            output = {"wallet_address": wallet,
                      "nft_projects_k": nft_k,
                      "hit_rate": hit_rate,
                      "hit_ratio": hit_ratio}
            evaluation_data.append(output)

        print(f"wallet: {wallet} done [{idx+1}/{len(test_wallet_list)}]")

    evaluation_df = pd.DataFrame(evaluation_data)

    evaluation_df = evaluation_df.groupby(
        ["nft_projects_k"], as_index=False).mean()

    evaluation_df = evaluation_df.sort_values(
        by=["nft_projects_k", "hit_ratio"], ascending=False)

    evaluation_df.to_csv(nft_random_recommender_eval_file_name)


if __name__ == "__main__":
    # setup db connection
    db_cnx = BlockchainDatabaseConnector(
        config.MYSQL_DB_HOST,
        config.MYSQL_DB_USER,
        config.MYSQL_DB_PASSWORD,
        config.MYSQL_DB_NAME
    )

    # setup nft recommender
    nft_recommender = NftRecommender()

    nft_recommender.load_ft_balance_data_from_pkl(
        "datasets/pp_ft_balance_dataset.pkl")
    nft_recommender.load_nft_balance_data_from_pkl(
        "datasets/pp_nft_balance_dataset.pkl")
    nft_recommender.build_wallet_similarity_matrix()

    # get wallet testset
    test_wallet_list = nft_recommender.get_wallet_testset(
        test_size, random_state)

    #file names
    hyperparameter_data_file_name = "results/hyperparameter_optimization_data.json"
    nft_recommender_eval_file_name = "results/nft_recommender_eval.csv"
    nft_random_recommender_eval_file_name = "results/nft_random_recommender_eval.csv"

    # perform hyperparameter optimization
    perform_hyperparameter_optimization(nft_recommender, test_wallet_list, hyperparameter_data_file_name)

    # evaluate nft recommender
    evaluate_nft_recommender(hyperparameter_data_file_name, nft_recommender_eval_file_name)

    # evaluate random recommender
    evaluate_random_nft_recommender(nft_recommender, test_wallet_list, nft_random_recommender_eval_file_name)
