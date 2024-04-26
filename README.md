# Ingestion project for techincal test

This is small project to a company test done in aproximately a day of work.

# Requirements
- docker-compose
- docker
- psql

# Makefile How to use it

- `make clean` - Cleans up directories and stops any running Docker containers, removing volumes as necessary.*
- `make build` - Builds Docker containers using Docker Compose, a **jupyter server and a postgres database**
- `make up` - Starts Docker containers, ensuring no containers are currently running before starting.
- `make down` - Stops and removes Docker containers.
- `make check-stop` - Checks if any Docker containers are currently up and stops them if they are.

#  Folder Structure
- data: where the csv files are
- ingestion: where the python code is located.

# How to run

All this commands are suppose to run on root.

First build and run docker-compose and the docker ingestion file.
```bash
make build # this builds both docker-compose and dockerfile
```

Run the local database:
```bash
make up
```

Run the process to create the first ingestion:
```bash
docker run --net host -e "POSTGRES_PASSWORD=postgres" -v ./data:/app/data ingestion-data -- --csv_path /app/data/original_data.csv
```

Run the process to create something that modifies the table:
```bash
docker run --net host -e "POSTGRES_PASSWORD=postgres" -v ./data:/app/data ingestion-data -- --csv_path app/data/modified_data.csv
```

If you want to check data just go to  [local notebook](http://localhost:8080/notebooks/work/check_data.ipynb) and run all the cells. The pip install was there just so because this is a exercise.

After doing this if you can clean the volumes and docker-compose:
```
make clean
```

# Final comments:

This is quite straight forward just to show that I can develop in python.

Considerations:
 - For the sake of this is just a test I `will not think about complex data types that come from string on csv inference e.g:date, timestamp, ts
- The conversions is not perfect, object type could be converted to numbers in a initial case of nulls
- For simplicity of the code I used pandas types
- This uses a separation of target and source so we can abstract this in the future for other solutions.

Improvements:

- Adding inference for date, timestamp, ts and relative data types using regexp.
- Separate the runner from schema change component.
- Add a failure where schema changes would be sinked to a table for further analysis.
- To handle secrets I would simply spaw this container with kubernetes and mount as a kube secret in the environment varaible or add access role based on the container being spawned (preferrable).
- Generic filepath loading like /path/to/folder/*.csv
- Improve how the path of the file and table name is done. As a constructor seeing quite poor.
- Improve how schema change and capture is handle on the target.
- There is some TODOs in the code, It's not a good practice in my opinion but It feels is the best way to set comments
- Clean the code, adding configuration files and proper logging.
