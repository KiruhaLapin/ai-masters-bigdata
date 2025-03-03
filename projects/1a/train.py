from model import model
import sys
import pandas as pd
from joblib import dump
from model import fields

def train_and_save_model(project_id, dataset_path):
    print(dataset_path)
    data = pd.read_csv(dataset_path, delimiter='\t', names=fields)
    target = data.label
    X = data.drop(columns=['id','label'])
    model.fit(X, target)
    model_filename = f"{project_id}.joblib"
    dump(model, model_filename)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Использование: python script.py <номер_проекта> <путь_к_файлу_с_данными>")
    else:
        project_id = sys.argv[1]
        dataset_path = sys.argv[2]
        train_and_save_model(project_id, dataset_path)

