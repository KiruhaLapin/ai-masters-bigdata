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

# Запускаем Python-скрипт с переданными аргументами
python3 "$TRAIN_SCRIPT" "$PROJECT_ID" "$REPO_ROOT/$DATASET_PATH"
