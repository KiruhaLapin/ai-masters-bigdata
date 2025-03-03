# Проверяем, что переданы два аргумента
if [ "$#" -ne 2 ]; then
    echo "Использование: $0 <номер_проекта> <путь_к_файлу_с_данными>"
    exit 1
fi

# Устанавливаем переменные
PROJECT_ID=$1
DATASET_PATH=$2

# Определяем путь к корню репозитория
REPO_ROOT=$(cd "$(dirname "$0")/../.." && pwd)

# Путь к скрипту train.py
TRAIN_SCRIPT="$REPO_ROOT/projects/$PROJECT_ID/train.py"

if [[ "$DATASET_PATH" = /* ]]; then
    FINAL_PATH="$DATASET_PATH"
else
    FINAL_PATH="$REPO_ROOT/$DATASET_PATH"
fi

python3 "$TRAIN_SCRIPT" "$PROJECT_ID" "$FINAL_PATH"


