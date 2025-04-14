#!/opt/conda/envs/dsenv/bin/python
import os, sys
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.model_selection import train_test_split
from sklearn.metrics import log_loss
import mlflow, mlflow.sklearn


train_path = sys.argv[1]
solver = sys.argv[2]
numeric_features = ["if"+str(i) for i in range(1,14)]
categorical_features = ["cf"+str(i) for i in range(1,27)]
fields = ["id", "label"] + numeric_features + categorical_features

num_transformer = Pipeline([
    ('imp_mean', SimpleImputer(strategy='mean')),
    ('scaler', StandardScaler()),
])
cat_transformer = Pipeline(steps=[
    ('imp_most_freq', SimpleImputer(strategy='constant',fill_value='missing')),
    ('ohe', OneHotEncoder(handle_unknown='ignore'))
])
ct = ColumnTransformer(
    transformers=[
        ('num', num_transformer, numeric_features),
       ('cat', cat_transformer, categorical_features[:14]),
    ]
)

model = Pipeline([
    ('ct', ct),
    ('model',  LogisticRegression(solver=solver)),
])



read_table_opts = dict(sep="\t", names=fields, index_col=False)
df = pd.read_table(train_path, **read_table_opts)
target = df.label
X = df.drop(columns=["id","label"])


X_train, X_test, y_train, y_test = train_test_split(
    X, target, test_size=0.33, random_state=42
)
with mlflow.start_run():
    mlflow.log_param("solver", solver)
    model.fit(X_train, y_train)
    y_proba = model.predict_proba(X_test)
    loss = log_loss(y_test, y_proba)
    mlflow.log_metric("log_loss", loss)
    mlflow.sklearn.log_model(model, artifact_path="models")



