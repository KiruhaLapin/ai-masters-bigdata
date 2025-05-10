#!/usr/bin/env python3
import argparse
import os
import glob
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier

import pandas as pd
import argparse
import glob
import os

parser = argparse.ArgumentParser()
parser.add_argument("--train-in", type=str, required=True)
parser.add_argument("--sklearn-model-out", type=str, required=True)
args = parser.parse_args()

# Читаем все CSV-файлы из директории
csv_files = glob.glob(os.path.join(args.train_in, "*.csv"))
df = pd.concat([pd.read_csv(f) for f in csv_files])

target_col = 'label'

X = df.drop(columns=[target_col, "id"])
y = df[target_col]

model = RandomForestClassifier(random_state=42, n_jobs=-1)
model.fit(X, y)
joblib.dump(model, args.sklearn_model_out)

