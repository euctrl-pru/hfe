# Horizontal Flight Efficiency
This is the repository for the code responsible to calculater the HFE performance indicators.

## Usage ##
In order to calculate HFE data for a given date interval, execute:

```shell
$ compute.sh "01/02/2015" "02/03/2015"
```


## What is What ##

### `CleanAndRecreateHFE49.sql`
Creates all the different tables

### `Hfe49.pkb and Hfe49.pks`
The main procedure (definition and body)

### `Test5.sql`
It actually generates the results (per flight/portion)

### `InsertHFEdailyValues.sql`
Inserts the aggregated daily values

### `HFEInsertFabStateDaily.sql` and `HFEInsertFabKEADaily.sql`
Generate the table per FAB/State (base for what is sent out every month -- and should be published on Dashboard?)