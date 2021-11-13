# test_project_EuroChem

In models.py there are 3 models: CurrencyRate, AvgTemp, Balance. Each model represents ETL pipline (see docstring).

```
model = CurrencyRate(start_date='2020-12-20', end_date='2020-12-25', loading_path='currency_rate.csv')
model.run()

model = AvgTemp(start_date='2020-12-20', end_date='2020-12-25', loading_path='currency_rate.csv')
model.run()

model = Balance(start_year=2019, end_year=2020, loading_path='currency_rate.csv')
model.run()
```
