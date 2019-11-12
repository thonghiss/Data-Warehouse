# ETL Processes for Water Company

## Setup steps

- Clone this project:

```bash
git clone ssh://git.dnpcorp.net:99/da/ns3-etl
```

- Clone `.env.template` to `.env` and modify inside values to fit source and destination databases

```bash
cp .env.template .env
```

- Set environment variables

```bash
export $(xargs <.env)
```

## How to run

### Run manually

- Run ETL for Dimensions

```bash
python run.py --run_dimensions
```

- Run ETL for Facts

```bash
python run.py --run_facts
```

### Schedule for automatically running

Use `crontab`

### Notes for running

- Always run dim_datetime first and only as Dimension or TypeOneSlowlyChangingDimension
- After finish dim_datetime, change class to CachedDimension, and etl_active to False to avoid any change in the future. 
  CachedDimension helps better lookup performance also.
- Change ensure to scdensure when running type one or type two dimension 
- ALWAYS REMEMBER to handle potential null value before run facts or dimensions 