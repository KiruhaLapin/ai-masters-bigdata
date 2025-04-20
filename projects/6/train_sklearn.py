#!/usr/bin/env python3
import argparse
import os
import glob
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier

parser = argparse.ArgumentParser(description="Train sklearn RandomForestClassifier on CSV data with target column 'label'")
parser.add_argument("--train-in", type=str, required=True, help="Путь к входным данным (CSV-файл или директория с CSV)")
parser.add_argument("--sklearn-model-out", type=str, required=True, help="Путь для сохранения обученной модели (joblib)")
args = parser.parse_args()

df = pd.read_csv(args.train_in)

target_col = 'label'

X = df.drop(columns=[target_col])
y = df[target_col]

model = RandomForestClassifier(random_state=42, n_jobs=-1)
model.fit(X, y)
joblib.dump(model, args.sklearn_model_out)

