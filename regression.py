import numpy as np
import pandas as pd
from sklearn.linear_model import LassoCV, RidgeCV, LinearRegression, LogisticRegressionCV, LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import root_mean_squared_error
from scipy.stats import ttest_1samp


def regression(data: pd.DataFrame, label="1D Price", test_size=0.2):
    print("-"*50)
    print(f"Regression on {label}")
    columns = ["EBITDA", "Revenue", "Operating Margin", "ROA",
               "Debt-to-Equity", "P/E Ratio", "IPO Price", "Sentiment Score Before", "Sentiment Score All"]

    data = data[columns + [label]].copy()
    data["Log Return"] = np.log(data[label]/data["IPO Price"])

    data = data.replace([np.inf, -np.inf], np.nan)
    data = data.dropna().reset_index(drop=True)

    X = data.drop(columns=[label, "Log Return"])
    y = data["Log Return"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, shuffle=False)

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    lasso = LassoCV(n_alphas=10000, max_iter=100000,
                    n_jobs=-1, fit_intercept=False)
    lasso.fit(X_train_scaled, y_train)
    y_test_predicted = lasso.predict(X_test_scaled)

    rmse = root_mean_squared_error(y_test, y_test_predicted)
    print(f"RMSE: {rmse}")

    coef = {x: y for x, y in zip(columns, lasso.coef_)}
    print(f"Coefficients: {coef}")

    print(f"Alpha: {lasso.alpha_}")

    label_day_mapping = {
        "1D Price": 256,
        "1W Price": 52,
        "1M Price": 12,
        "3M Price": 4,
        "1Y Price": 1
    }
    y_test_predicted_annuailzed = y_test_predicted * label_day_mapping[label]
    actual_returns = (y_test * np.sign(y_test_predicted)
                      )[np.abs(y_test_predicted_annuailzed) >= np.log(1.1)]
    sharpe = np.mean(actual_returns) / np.std(actual_returns)
    print(f"Annualized Sharpe: {sharpe * np.sqrt(label_day_mapping[label])}")
    print(f"p-value: {ttest_1samp(actual_returns, 0)[1]}")
    print("-"*50)


if __name__ == "__main__":
    fundamental = pd.read_csv("data/fundamental_data.csv")
    fundamental = fundamental.drop(columns=["FCF"])
    sentiment_before = pd.read_csv("data/sentiment_data_before.csv").fillna(0)
    sentiment_all = pd.read_csv("data/sentiment_data_all.csv").fillna(0)
    joined_sentiment = pd.merge(
        sentiment_before,
        sentiment_all,
        how="inner",
        on="Symbol",
        suffixes=(" Before", " All"))
    joined_data = pd.merge(
        fundamental,
        joined_sentiment,
        how="inner",
        on="Symbol")

    regression(joined_data, "1D Price")
    regression(joined_data, "1W Price")
    regression(joined_data, "1M Price")
    regression(joined_data, "3M Price")
    regression(joined_data, "1Y Price")
