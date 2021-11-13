# test_project_EuroChem

In models.py there are 3 models: CurrencyRate, AvgTemp, Balance. Each model represents ETL pipline (see docstring).

```
model = CurrencyRate(start_date='2020-12-20', end_date='2020-12-25', loading_path='currency_rate.csv')
model.run()

model = AvgTemp(start_date='2020-12-20', end_date='2020-12-25', loading_path='avg_temp.csv')
model.run()

model = Balance(start_year=2019, end_year=2020, loading_path='balance.csv')
model.run()
```

You can also specify an additional parameter 'by_day' (which means the data is daily) for CurrencyRate, AvgTemp:
```
model = CurrencyRate(start_date='2020-12-20', end_date='2020-12-25', loading_path='currency_rate.csv', by_day=True)
model.run()

model = AvgTemp(start_date='2020-12-20', end_date='2020-12-25', loading_path='avg_temp.csv', by_day=True)
model.run()
```
