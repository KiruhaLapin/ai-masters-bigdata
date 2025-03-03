import sys
import pandas as pd
from joblib import load
from model import fields

model = load("1a.joblib")
input_data = sys.stdin.read()
data = [line.split("\t") for line in input_data.strip().split("\n")]
df = pd.DataFrame(data,columns = fields[0:1]+fields[2:])
predictions = model.predict(df)

for pred in predictions:
    print(pred)

