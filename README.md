# Learning Autoregressive Model in LSM-Tree based Store

## How to Execute LSMAR

- Load data into the database.
- Execute the following command in the client of IoTDB, and it will return the coefficients of $AR(2)$ model on time series *root.gas.d0.s0*.

```sql
select ar(s0, "p"="2") from root.gas.d0
where time >= 1970-01-01T08:00:00
  and time <= 1970-01-07T23:59:59
```



