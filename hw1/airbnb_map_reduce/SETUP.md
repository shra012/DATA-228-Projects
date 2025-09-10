# Setting up env.sh

Before running the pipeline, configure `bin/env.sh` so it points to the correct paths on your machine.

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
```

If these echo the correct paths, your environment is ready.

---

⚡ **Tip:**  
You can copy `config/env.example` to `config/env.local` and override only your machine-specific settings there, while keeping `env.sh` version-controlled and clean.

