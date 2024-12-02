import numpy as np
import pandas as pd
from sklearn.linear_model import LassoCV, RidgeCV, LinearRegression, LogisticRegressionCV
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score


def regression(data: pd.DataFrame, label="1D Price", test_size=0.2):
    data = data[["EBITDA", "Revenue", "Operating Margin", "ROA",
                 "Debt-to-Equity", "P/E Ratio", "IPO Price", "Sentiment Score Before", "Sentiment Score All", label]].copy()
    data["Percent Return"] = (
        data[label] - data["IPO Price"]) / data["IPO Price"]

    data = data.replace([np.inf, -np.inf], np.nan)
    data = data.dropna().reset_index(drop=True)

    X = data.drop(columns=[label, "Percent Return"])
    y = data["Percent Return"]
    y[y >= 0] = 1
    # y[y == 0] = 0.5
    y[y < 0] = 0

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, shuffle=False)

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    lasso = LogisticRegressionCV(
        penalty="l1", solver="liblinear", max_iter=100000, n_jobs=-1)
    lasso.fit(X_train_scaled, y_train)
    y_test_predicted_lasso = lasso.predict(X_test_scaled)

    ridge = LogisticRegressionCV(penalty="l2")
    ridge.fit(X_train_scaled, y_train)
    y_test_predicted_ridge = ridge.predict(X_test_scaled)

    ols = LogisticRegressionCV()
    ols.fit(X_train_scaled, y_train)
    y_test_predicted_ols = ols.predict(X_test_scaled)

    print(accuracy_score(y_test, y_test_predicted_lasso))
    print(accuracy_score(y_test, y_test_predicted_ridge))
    print(accuracy_score(y_test, y_test_predicted_ols))


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
