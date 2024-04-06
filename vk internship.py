import pandas as pd

data = pd.read_csv("variant2.csv", delimiter="\t")
data[['date', 'time']] = data['timestamp'].str.split('T', expand=True)
data = data[['date', 'value']]

# сортировка по ворастанию ГГГГ-ММ-ДД
data = data.sort_values(by=['date'])

# Нахождение значений для удаления
first_date = str(data['date'].iloc[0])[:7]
last_date = str(data['date'].iloc[-1])[:7]

# Таблица со знаечниями, месяц которых неполноценный (т.е. значения первого из представленных месяцев и последнего)
remove = data[data['date'].str[:7].isin([first_date, last_date])]
# Удаление из таблицы data строк, которые есть в remove и сохранение новой таблицы filtered_data с конечными данными
merged = data.merge(remove, how='outer', indicator=True)
filtered_data = merged[merged['_merge'] == 'left_only']
filtered_data = filtered_data.drop('_merge', axis=1)
filtered_data['date'] = pd.to_datetime(filtered_data['date'])

# Создание нового столбца с месяцем и годом
filtered_data['month'] = filtered_data['date'].dt.to_period('M')
# Группировка данных по месяцу и году и нахождение максимального значения value для каждого месяца и года
max_values = filtered_data.groupby('month').apply(lambda x: x.loc[x['value'].idxmax()])

# Удаление столбца 'month' и изменение формата вывода
max_values = max_values[['date', 'value']].reset_index(drop=True)
max_values.to_csv('max_values.csv', index=False)
print(max_values)

