# Setting up env.sh

Before running the pipeline, configure `bin/env.sh` so it points to the correct paths on your machine.

## 0. Prerequisites
- Python 3 available as `python3`
- Java and Hadoop installed; `hadoop`/`hdfs` CLIs on `PATH`
- `HADOOP_HOME` set to your Hadoop install
- `curl` for fetching InsideAirbnb data

## 1. Locate env.sh
The file lives in:
```
airbnb_map_reduce/bin/env.sh
```

## 2. Open in an editor
```bash
nano bin/env.sh
```

## 3. Update key variables

- **Project root on local machine**
  ```bash
  export PROJ_ROOT="/home/<your-username>/DATA-228-Projects/hw1/airbnb_map_reduce"
  ```
  Replace `<your-username>` with your actual Linux username or the absolute path where you cloned the repo.

- **HDFS namespace**
  ```bash
  export HDFS_NS="/projects/DATA-228-Projects/hw1/airbnb_map_reduce"
  ```
  If your cluster uses a different base path, update accordingly.

- **Hadoop Streaming JAR**
  ```bash
  export HSTREAM_JAR=${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-*.jar
  ```
  Make sure `$HADOOP_HOME` is set in your environment. You can check with:
  ```bash
  echo $HADOOP_HOME
  ```

- **Fetch defaults (new)**
  These control `make fetch` and `bin/fetch_and_put.sh` when no args are provided:
  ```bash
  export COUNTRY_DEFAULT="united-states"
  export STATE_DEFAULT="ca"
  export COUNTY_DEFAULT="san-francisco"
  export DATE_DEFAULT="$(date +%Y-%m-01)"  # YYYY-MM-01
  ```
  You can override them ad‑hoc on the command line, for example:
  ```bash
  COUNTRY_DEFAULT=ireland STATE_DEFAULT=leinster COUNTY_DEFAULT=dublin DATE_DEFAULT=2024-09-01 make fetch
  # or pass explicit args to the script
  bash airbnb_map_reduce/bin/fetch_and_put.sh united-states ca san-francisco 2024-09-01
  ```

- **Log, data, and jobs directories**
  These should normally be fine as-is, but confirm that the paths exist:
  ```bash
  export BIN_DIR="${PROJ_ROOT}/bin"
  export JOBS_DIR="${PROJ_ROOT}/jobs"
  export DATA_DIR="${PROJ_ROOT}/data"
  export LOGS_DIR="${PROJ_ROOT}/logs"
  export TMP_DIR="${PROJ_ROOT}/tmp"
  ```

## 4. Save and source the file
```bash
source bin/env.sh
```

## 5. Verify configuration
Run:
```bash
echo $PROJ_ROOT
echo $HDFS_NS
echo $HSTREAM_JAR
python3 --version
hadoop version | head -n1
hdfs dfs -ls / >/dev/null 2>&1 && echo "HDFS OK" || echo "HDFS not reachable"
```

If these echo the correct paths, your environment is ready.

---

⚡ **Tip:**  
`env.sh` auto-detects the repo root and will source `config/env.local` if present. Copy `config/env.example` to `config/env.local` and override only your machine-specific settings there.
