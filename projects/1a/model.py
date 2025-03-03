from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer


numeric_features = ["if"+str(i) for i in range(1,14)]
categorical_features = ["cf"+str(i) for i in range(1,27)]
fields = ["id", "label"] + numeric_features + categorical_features

num_transformer = Pipeline([
    ('imp_mean', SimpleImputer(strategy='mean')),
    ('scaler', StandardScaler()),
])
cat_transformer = Pipeline(steps=[
    ('imp_most_freq', SimpleImputer(strategy='most_frequent')),
    ('ohe', OneHotEncoder(handle_unknown='ignore'))
])
ct = ColumnTransformer(
    transformers=[
        ('num', num_transformer, numeric_features),
       #('cat', cat_transformer, categorical_features),
    ]
)

model = Pipeline([
    ('ct', ct),
    ('model',  LogisticRegression(solver="newton-cholesky")),
])
