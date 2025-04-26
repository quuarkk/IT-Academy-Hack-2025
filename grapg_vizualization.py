import pandas as pd
import glob
import os
import json
from tqdm import tqdm
import matplotlib.pyplot as plt  # Для визуализации графиков

# Пути к данным (папка с датасетом находится в текущей директории проекта)
dataset_path = './telecom1000k'  # Измените на 'telecom1000k' если папка в корне проекта

# Загрузка файла с ID для отрисовки графиков
print("Загрузка файла result_optimized_cleaned2.csv...")
ids_file_path = os.path.join(dataset_path, 'result_optimized_cleaned2.csv')
if not os.path.exists(ids_file_path):
    raise FileNotFoundError(f"Файл с ID для отрисовки графиков не найден: {ids_file_path}")

ids_df = pd.read_csv(ids_file_path)
ids_to_plot = ids_df['Id'].unique()  # Список ID для построения графиков

# Проверка существования папки с данными
if not os.path.exists(dataset_path):
    raise FileNotFoundError(f"Папка с датасетом не найдена: {dataset_path}")

# Загрузка psxattrs для сопоставления PSX и типа
print("Загрузка psxattrs.csv...")
psxattrs_path = os.path.join(dataset_path, 'psxattrs.csv')
psxattrs = pd.read_csv(psxattrs_path)
psxattrs['PSX'] = psxattrs['PSX'].astype(str)
psxattrs = psxattrs.set_index('PSX')

# Загрузка client.parquet
print("Загрузка client.parquet...")
client_path = os.path.join(dataset_path, 'client.parquet')
client_df = pd.read_parquet(client_path)
client_df = client_df.rename(columns={"Id": "UID"})

# Загрузка plan.json
print("Загрузка plan.json...")
plan_path = os.path.join(dataset_path, 'plan.json')
try:
    with open(plan_path, 'r') as f:
        plan_data = json.load(f)
    plan_df = pd.DataFrame(plan_data)
except:
    plan_df = pd.read_json(plan_path)
plan_df = plan_df.rename(columns={"Id": "IdPlan"})

# Остальной код остается без изменений, но с корректировкой путей
# Создаем заглушку для client_type_df на случай, если файлы типов отсутствуют
client_type_df = pd.DataFrame(columns=['UID', 'Type'])

# Определяем тип клиента на основе доступных данных
print("Определение типа клиента...")
try:
    # Проверяем наличие файла physical.parquet
    physical_path = os.path.join(dataset_path, 'physical.parquet')
    if not os.path.exists(physical_path):
        physical_path = os.path.join(dataset_path, 'phisical.parquet')

    if os.path.exists(physical_path):
        print(f"Загрузка {physical_path}...")
        physical_df = pd.read_parquet(physical_path)
        if 'UID' not in physical_df.columns and 'Id' in physical_df.columns:
            physical_df = physical_df.rename(columns={"Id": "UID"})
        physical_df['Type'] = 'P'  # Физические лица
        client_type_df = pd.concat([client_type_df, physical_df[['UID', 'Type']]], ignore_index=True)
    else:
        print("Файл physical.parquet не найден.")

    # Проверяем наличие файла company.parquet
    company_path = os.path.join(dataset_path, 'company.parquet')
    if os.path.exists(company_path):
        print(f"Загрузка {company_path}...")
        company_df = pd.read_parquet(company_path)
        if 'UID' not in company_df.columns and 'Id' in company_df.columns:
            company_df = company_df.rename(columns={"Id": "UID"})
        company_df['Type'] = 'C'  # Компании
        client_type_df = pd.concat([client_type_df, company_df[['UID', 'Type']]], ignore_index=True)
    else:
        print("Файл company.parquet не найден.")

    if client_type_df.empty:
        print("Не удалось загрузить информацию о типах клиентов. Все клиенты будут считаться физическими лицами.")
except Exception as e:
    print(f"Ошибка при определении типа клиента: {str(e)}")
    print("Все клиенты будут считаться физическими лицами (тип 'P').")

# Загрузка subscribers.csv
print("Загрузка subscribers.csv...")
subsc_path = os.path.join(dataset_path, 'subscribers.csv')
subsc_df = pd.read_csv(subsc_path)
subsc_df = subsc_df.rename(columns={"IdClient": "UID", "IdOnPSX": "IdSubscriber"})

# Создаем связь между IdSubscriber и PSX
print("Построение связи между IdSubscriber и PSX...")
merged_subsc_client_df = subsc_df.merge(client_df, on="UID", how="left")

# Собираем все .csv и .txt файлы
print("Сбор файлов данных...")
all_files = glob.glob(os.path.join(dataset_path, "*.csv")) + glob.glob(os.path.join(dataset_path, "*.txt"))
df_list = []

for f in tqdm(all_files, desc="Обработка файлов", unit="файл"):
    if os.path.getsize(f) == 0:
        continue
    fname = os.path.basename(f)
    if fname in ['psxattrs.csv', 'subscribers.csv'] or fname.endswith('.parquet') or fname.endswith('.json'):
        continue

    psx_code = None
    if '_' in fname:
        parts = fname.split('_')
        if len(parts) > 1 and parts[0] == 'psx':
            psx_code = parts[1]

    if psx_code and psx_code in psxattrs.index:
        delimiter = psxattrs.loc[psx_code, 'Delimiter']
        date_format = psxattrs.loc[psx_code, 'DateFormat']
    else:
        delimiter = ','
        date_format = '%d-%m-%Y %H:%M:%S'

    try:
        temp_df = pd.read_csv(f, delimiter=delimiter)
        if not temp_df.empty:
            temp_df['psx_code'] = psx_code
            temp_df['date_format'] = date_format
            temp_df['IdPSX'] = int(psxattrs.loc[psx_code, 'Id']) if psx_code in psxattrs.index else None
            df_list.append(temp_df)
    except Exception as e:
        print(f"Ошибка при чтении файла {fname}: {str(e)}")
        continue

if not df_list:
    raise Exception("Нет подходящих файлов с данными!")

print("Объединение данных...")
df = pd.concat(df_list, ignore_index=True)


# Векторизированный парсинг времени с индивидуальным форматом
def parse_times(df):
    times = pd.Series([pd.NaT] * len(df))
    for fmt in df['date_format'].unique():
        mask = df['date_format'] == fmt
        times[mask] = pd.to_datetime(df.loc[mask, 'StartSession'], format=fmt, errors='coerce')
    return times


df['TurnOn'] = parse_times(df)

# Traffic
df['UpTx'] = pd.to_numeric(df['UpTx'], errors='coerce').fillna(0)
df['DownTx'] = pd.to_numeric(df['DownTx'], errors='coerce').fillna(0)
df['Traffic'] = df['UpTx'] + df['DownTx']

# Агрегация по часу
print("Агрегация данных по часам...")
df['hour'] = df['TurnOn'].dt.floor('h')
agg_df = df.groupby(['IdSession', 'IdSubscriber', 'hour', 'IdPSX'], observed=True).agg({
    'TurnOn': 'first',
    'Traffic': 'sum',
    'UpTx': 'sum',
    'DownTx': 'sum'
}).reset_index()

# Коэффициент upload/download
print("Определение аномалий...")
agg_df['up_down_ratio'] = agg_df['UpTx'] / agg_df['DownTx'].replace(0, 1)
anomaly_threshold = 0.924
agg_df['Hacked'] = agg_df['up_down_ratio'] > anomaly_threshold

# Поиск streak подряд идущих аномалий
agg_df = agg_df.sort_values(['IdSubscriber', 'hour'])
agg_df['hour_diff'] = agg_df.groupby('IdSubscriber')['hour'].diff().dt.total_seconds().div(3600).fillna(1)
agg_df['new_group'] = ((agg_df['hour_diff'] != 1) | (~agg_df['Hacked'])).cumsum()
agg_df['streak'] = agg_df.groupby(['IdSubscriber', 'new_group'])['Hacked'].cumsum()

# Оставляем только те, где streak >= 1
agg_df['Hacked_long'] = (agg_df['Hacked']) & (agg_df['streak'] >= 1)
agg_df['DateHacked'] = agg_df['TurnOn']

# Генерация графиков аномалий только для ID из файла
print("Генерация графиков аномалий...")
graphs_dir = os.path.join(dataset_path, 'graphs')
os.makedirs(graphs_dir, exist_ok=True)  # Создаем папку для графиков

for subscriber_id in tqdm(ids_to_plot, desc="Генерация графиков"):
    if subscriber_id not in agg_df['IdSubscriber'].values:
        print(f"ID {subscriber_id} отсутствует в данных. Пропуск.")
        continue

    subscriber_data = agg_df[agg_df['IdSubscriber'] == subscriber_id].sort_values('hour')
    anomalies = subscriber_data[subscriber_data['Hacked_long']]

    plt.figure(figsize=(15, 10))

    # График общего трафика
    plt.subplot(3, 1, 1)
    plt.plot(subscriber_data['hour'], subscriber_data['Traffic'], label='Общий трафик', color='purple')
    plt.scatter(anomalies['hour'], anomalies['Traffic'], color='red', label='Аномалия', alpha=0.7)
    plt.title(f'ID {subscriber_id} - Общий трафик', pad=20)
    plt.xlabel('Время')
    plt.ylabel('Байты')
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.legend()

    # График исходящего трафика (Upload)
    plt.subplot(3, 1, 2)
    plt.plot(subscriber_data['hour'], subscriber_data['UpTx'], label='Исходящий трафик', color='green')
    plt.scatter(anomalies['hour'], anomalies['UpTx'], color='red', alpha=0.7)
    plt.title(f'ID {subscriber_id} - Исходящий трафик', pad=20)
    plt.xlabel('Время')
    plt.ylabel('Байты')
    plt.grid(True, linestyle='--', alpha=0.7)

    # График входящего трафика (Download)
    plt.subplot(3, 1, 3)
    plt.plot(subscriber_data['hour'], subscriber_data['DownTx'], label='Входящий трафик', color='blue')
    plt.scatter(anomalies['hour'], anomalies['DownTx'], color='red', alpha=0.7)
    plt.title(f'ID {subscriber_id} - Входящий трафик', pad=20)
    plt.xlabel('Время')
    plt.ylabel('Байты')
    plt.grid(True, linestyle='--', alpha=0.7)

    plt.tight_layout()
    plt.savefig(os.path.join(graphs_dir, f'anomalies_{subscriber_id}.png'))
    plt.close()

print("Графики успешно сохранены в папку 'graphs'.")