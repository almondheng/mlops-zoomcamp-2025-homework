from prefect import flow, task
from prefect.logging import get_run_logger
import pandas as pd
import mlflow
from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LinearRegression

mlflow.set_tracking_uri("http://127.0.0.1:5000")
mlflow.set_experiment("nyc-taxi-experiment")
mlflow.autolog()


@task
def read_dataframe(filename):
    df = pd.read_parquet(filename)

    df["duration"] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df.duration = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)]

    categorical = ["PULocationID", "DOLocationID"]
    df[categorical] = df[categorical].astype(str)

    return df


@task
def train_model(df):
    with mlflow.start_run():
        categorical = ["PULocationID", "DOLocationID"]
        df[categorical] = df[categorical].astype(str)
        train_dicts = df[categorical].to_dict(orient="records")

        dv = DictVectorizer()
        X_train = dv.fit_transform(train_dicts)
        y_train = df["duration"].values

        model = LinearRegression()
        model.fit(X_train, y_train)

        return dv, model


@flow
def main():
    logger = get_run_logger()
    df = read_dataframe("data/yellow_tripdata_2023-03.parquet")
    logger.info(len(df))
    dv, model = train_model(df)
    logger.info(model.intercept_)
    return


if __name__ == "__main__":
    main()
