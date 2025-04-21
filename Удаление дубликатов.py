import pandas as pd

# Загрузка данных из CSV файла
print("Загрузка данных...")
df = pd.read_csv('result_optimized.csv')

# Вывод информации о колонках
print("Доступные колонки в данных:")
print(df.columns.tolist())

# Вывод информации о количестве строк до очистки
print(f"Количество строк до очистки: {len(df)}")

# Удаление дубликатов по колонке id
print("Удаление дубликатов по id пользователя...")
df_cleaned = df.drop_duplicates(subset=['Id'])

# Вывод информации о количестве строк после очистки
print(f"Количество строк после очистки: {len(df_cleaned)}")
print(f"Удалено {len(df) - len(df_cleaned)} дубликатов")

# Сохранение очищенных данных
print("Сохранение очищенных данных...")
df_cleaned.to_csv('result_optimized_cleaned.csv', index=False)

print("Готово! Очищенные данные сохранены в файл result_optimized_cleaned.csv")
