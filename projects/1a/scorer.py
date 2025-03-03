from sklearn.metrics import log_loss
import pandas as pd


def find_score(true_target_path, predicted_target_path):
    y_true = pd.read_csv(true_target_path, header=None)
    y_pred = pd.read_csv(predicted_target_path, header=None)
    score = log_loss(y_true, y_pred)
    return score 


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Ошибка")
    else:
        true_target_path = sys.argv[1]
        predicted_target_path = sys.argv[2]
        score = find_score(true_target_path, predicted_target_path)
	print(score)
