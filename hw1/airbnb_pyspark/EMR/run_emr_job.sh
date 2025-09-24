#!/bin/bash

# EMR Airbnb Insights Job Runner
# This script uploads files to S3 and submits the job to EMR

set -e  # Exit on any error

# Resolve project locations now that the script lives under EMR/
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PYSPARK_DIR="$PROJECT_ROOT/pypark"
SCRIPT_ABS_PATH="$SCRIPT_DIR/$(basename "${BASH_SOURCE[0]}")"
SCRIPT_RELPATH="${SCRIPT_ABS_PATH#$PROJECT_ROOT/}"
if [[ "$SCRIPT_RELPATH" == "$SCRIPT_ABS_PATH" ]]; then
    SCRIPT_COMMAND="${0:-run_emr_job.sh}"
else
    SCRIPT_COMMAND="./$SCRIPT_RELPATH"
fi

cd "$PROJECT_ROOT"

S3_BUCKET_SCRIPTS="data228-emr-scripts-bucket-1"
S3_BUCKET_DATA="data228-emr-data-bucket-1"
S3_BUCKET_LOGS="data228-emr-logs-bucket-1"
AWS_REGION="us-east-1"
EC2_KEY_PAIR="emr-default-key"  # Will be auto-created from ~/.ssh/id_rsa.pub
# Airbnb dataset configuration
DATASET_DATE="2025-06-23"
LOCAL_DATA_DIR="data/raw/santa_clara_county/$DATASET_DATE"
DATA_S3_PREFIX="airbnb/raw/santa_clara_county/$DATASET_DATE"
OUTPUT_S3_PREFIX="airbnb/metrics/santa_clara_county/$DATASET_DATE"
CLOUDWATCH_LOG_PREFIX="/aws/emr/airbnb-insights"
# Bootstrap and configuration assets
BOOTSTRAP_SCRIPT="configs/bootstrap.sh"
# Dataset filenames will be resolved dynamically (handles .csv or .csv.gz)
LISTINGS_FILE=""
CALENDAR_FILE=""
REVIEWS_FILE=""
NEIGHBOURHOODS_FILE=""
# Optional: Specify subnet ID if you don't want to use default VPC/subnets
# EC2_SUBNET_ID="subnet-12345678"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

resolve_dataset_file() {
    local base_name="$1"
    local gz_path="$LOCAL_DATA_DIR/${base_name}.csv.gz"
    local csv_path="$LOCAL_DATA_DIR/${base_name}.csv"

    if [ -f "$gz_path" ]; then
        echo "${base_name}.csv.gz"
        return 0
    fi

    if [ -f "$csv_path" ]; then
        echo "${base_name}.csv"
        return 0
    fi

    print_error "Missing dataset file: $LOCAL_DATA_DIR/${base_name}.csv(.gz)"
    return 1
}

initialize_dataset_files() {
    LISTINGS_FILE=$(resolve_dataset_file "listings") || exit 1
    CALENDAR_FILE=$(resolve_dataset_file "calendar") || exit 1
    REVIEWS_FILE=$(resolve_dataset_file "reviews") || exit 1
    NEIGHBOURHOODS_FILE=$(resolve_dataset_file "neighbourhoods") || exit 1

    verify_dataset_contents "$LOCAL_DATA_DIR/$LISTINGS_FILE"
    verify_dataset_contents "$LOCAL_DATA_DIR/$CALENDAR_FILE"
    verify_dataset_contents "$LOCAL_DATA_DIR/$REVIEWS_FILE"
    verify_dataset_contents "$LOCAL_DATA_DIR/$NEIGHBOURHOODS_FILE"

    print_status "Using dataset files:"
    print_status "  Listings: $LISTINGS_FILE"
    print_status "  Calendar: $CALENDAR_FILE"
    print_status "  Reviews: $REVIEWS_FILE"
    print_status "  Neighbourhoods: $NEIGHBOURHOODS_FILE"
}

verify_dataset_contents() {
    local path="$1"
    local first_line

    if [[ "$path" == *.gz ]]; then
        if ! first_line=$(gunzip -c "$path" 2>/dev/null | head -n 1); then
            print_error "Failed to read dataset: $path"
            exit 1
        fi
    else
        if ! first_line=$(head -n 1 "$path" 2>/dev/null); then
            print_error "Failed to read dataset: $path"
            exit 1
        fi
    fi

    if [[ "$first_line" == "version https://git-lfs.github.com/spec/v1"* ]]; then
        local rel_path="$path"
        if [[ "$rel_path" == "$PROJECT_ROOT/"* ]]; then
            rel_path="${rel_path#$PROJECT_ROOT/}"
        fi

        print_warning "Dataset file $rel_path is a Git LFS pointer; attempting to fetch real contents (git lfs pull --include=$rel_path)."
        if fetch_git_lfs_object "$rel_path"; then
            if [[ "$path" == *.gz ]]; then
                first_line=$(gunzip -c "$path" 2>/dev/null | head -n 1 || true)
            else
                first_line=$(head -n 1 "$path" 2>/dev/null || true)
            fi
        fi

        if [[ "$first_line" == "version https://git-lfs.github.com/spec/v1"* || -z "$first_line" ]]; then
            print_error "Failed to hydrate Git LFS object for $rel_path. Run 'git lfs pull --include=$rel_path' manually and retry."
            exit 1
        fi
    fi
}

fetch_git_lfs_object() {
    local rel_path="$1"

    if ! command -v git >/dev/null 2>&1; then
        print_warning "git command not available; cannot fetch LFS object automatically."
        return 1
    fi

    if ! git lfs version >/dev/null 2>&1; then
        print_warning "git-lfs not installed; cannot fetch LFS object automatically."
        return 1
    fi

    if [ "${GIT_LFS_BOOTSTRAPPED:-0}" -eq 0 ]; then
        git lfs install --local >/dev/null 2>&1 || true
        GIT_LFS_BOOTSTRAPPED=1
    fi

    if git lfs pull --include="$rel_path" >/dev/null 2>&1; then
        print_status "Fetched Git LFS content for $rel_path"
        return 0
    fi

    return 1
}

# Check if AWS CLI is configured
check_aws_config() {
    print_status "Checking AWS configuration..."
    if ! aws sts get-caller-identity > /dev/null 2>&1; then
        print_error "AWS CLI is not configured or credentials are invalid"
        print_error "Please run 'aws configure' first"
        exit 1
    fi
    print_status "AWS CLI configured successfully"
}

check_or_create_key_pair() {
    print_status "Checking EC2 key pair: $EC2_KEY_PAIR"
    
    if aws ec2 describe-key-pairs --key-names "$EC2_KEY_PAIR" --region "$AWS_REGION" > /dev/null 2>&1; then
        print_status "Key pair '$EC2_KEY_PAIR' already exists in AWS"
        return 0
    fi
    
    print_status "Key pair '$EC2_KEY_PAIR' not found in AWS, creating it..."
    
    if [ ! -f ~/.ssh/id_rsa.pub ]; then
        print_error "Local SSH public key not found at ~/.ssh/id_rsa.pub"
        print_error "Please generate SSH keys first: ssh-keygen -t rsa -b 2048"
        exit 1
    fi
    
    if aws ec2 import-key-pair \
        --key-name "$EC2_KEY_PAIR" \
        --public-key-material fileb://~/.ssh/id_rsa.pub \
        --region "$AWS_REGION" > /dev/null 2>&1; then
        print_status "Successfully imported key pair '$EC2_KEY_PAIR' to AWS"
    else
        print_error "Failed to import key pair to AWS"
        exit 1
    fi
}

# Check and create EMR default roles if needed
check_or_create_emr_roles() {
    print_status "Checking EMR default roles..."
    
    # Check if both roles exist
    if aws iam get-role --role-name EMR_DefaultRole --region "$AWS_REGION" > /dev/null 2>&1 && \
       aws iam get-role --role-name EMR_EC2_DefaultRole --region "$AWS_REGION" > /dev/null 2>&1; then
        print_status "EMR default roles already exist"
        return 0
    fi
    
    print_status "EMR default roles missing, creating them..."
    
    if aws emr create-default-roles --region "$AWS_REGION" > /dev/null 2>&1; then
        print_status "Successfully created EMR default roles"
        # Wait a moment for roles to propagate
        sleep 10
    else
        print_error "Failed to create EMR default roles"
        print_error "You may need to create them manually: aws emr create-default-roles"
        exit 1
    fi
}

# Create CloudWatch log groups if they don't exist
create_cloudwatch_log_groups() {
    print_status "Creating CloudWatch log groups..."
    
    local log_groups=(
        "/aws/emr/airbnb-insights/spark-driver"
        "/aws/emr/airbnb-insights/spark-executor" 
        "/aws/emr/airbnb-insights/hadoop-yarn"
        "/aws/emr/airbnb-insights/hadoop-mapreduce"
        "/aws/emr/airbnb-insights/steps"
        "/aws/emr/airbnb-insights/yarn-stdout"
        "/aws/emr/airbnb-insights/yarn-stderr"
    )
    
    for log_group in "${log_groups[@]}"; do
        if ! aws logs describe-log-groups --log-group-name-prefix "$log_group" --region "$AWS_REGION" --query 'logGroups[0].logGroupName' --output text 2>/dev/null | grep -q "$log_group"; then
            print_status "Creating CloudWatch log group: $log_group"
            aws logs create-log-group --log-group-name "$log_group" --region "$AWS_REGION"
            
            # Set retention policy (7 days for cost efficiency)
            aws logs put-retention-policy --log-group-name "$log_group" --retention-in-days 7 --region "$AWS_REGION"
        else
            print_status "CloudWatch log group already exists: $log_group"
        fi
    done
}

# Create S3 buckets if they don't exist
create_s3_buckets() {
    print_status "Creating S3 buckets if they don't exist..."
    
    for bucket in "$S3_BUCKET_SCRIPTS" "$S3_BUCKET_DATA" "$S3_BUCKET_LOGS"; do
        if aws s3 ls "s3://$bucket" >/dev/null 2>&1; then
            print_status "Bucket already exists: $bucket"
            continue
        fi

        print_status "Creating bucket: $bucket"
        if [ "$AWS_REGION" = "us-east-1" ]; then
            if ! aws s3api create-bucket --bucket "$bucket" --region "$AWS_REGION" >/dev/null 2>&1; then
                print_warning "Create bucket returned non-zero status (possibly due to a concurrent operation)."
            fi
        else
            if ! aws s3api create-bucket \
                --bucket "$bucket" \
                --region "$AWS_REGION" \
                --create-bucket-configuration LocationConstraint="$AWS_REGION" >/dev/null 2>&1; then
                print_warning "Create bucket returned non-zero status (possibly due to a concurrent operation)."
            fi
        fi

        # Wait for bucket to become visible
        local attempts=0
        while ! aws s3 ls "s3://$bucket" >/dev/null 2>&1; do
            attempts=$((attempts + 1))
            if [ $attempts -ge 5 ]; then
                print_error "Failed to confirm bucket creation: $bucket"
                exit 1
            fi
            print_warning "Waiting for bucket $bucket to become available (attempt $attempts)..."
            sleep 3
        done
        print_status "Bucket ready: $bucket"
    done
}

# Upload files to S3
upload_files() {
    print_status "Uploading application assets to S3..."

    if [ ! -d "$LOCAL_DATA_DIR" ]; then
        print_error "Local data directory not found: $LOCAL_DATA_DIR"
        exit 1
    fi

    local script_bucket_path="s3://$S3_BUCKET_SCRIPTS"
    local data_bucket_path="s3://$S3_BUCKET_DATA/$DATA_S3_PREFIX"
    local package_zip="$PROJECT_ROOT/airbnb_package.zip"

    if [ ! -f "$BOOTSTRAP_SCRIPT" ]; then
        print_error "Bootstrap script not found: $BOOTSTRAP_SCRIPT"
        exit 1
    fi

    if [ ! -f "$PYSPARK_DIR/airbnb_insights_job.py" ]; then
        print_error "PySpark entrypoint not found: $PYSPARK_DIR/airbnb_insights_job.py"
        exit 1
    fi

    aws s3 cp "$PYSPARK_DIR/airbnb_insights_job.py" "$script_bucket_path/"
    print_status "Uploaded airbnb_insights_job.py"

    print_status "Packaging shared Airbnb modules..."
    rm -f "$package_zip"
    (
        cd "$PYSPARK_DIR"
        zip -r "$package_zip" airbnb -x "*.DS_Store" -x "__pycache__/*" >/dev/null
    )
    aws s3 cp "$package_zip" "$script_bucket_path/"
    rm -f "$package_zip"
    print_status "Uploaded airbnb package archive"

    print_status "Syncing dataset to $data_bucket_path"
    aws s3 sync "$LOCAL_DATA_DIR/" "$data_bucket_path/" --delete
    print_status "Dataset sync completed"

    print_status "Uploading bootstrap action script"
    aws s3 cp "$BOOTSTRAP_SCRIPT" "$script_bucket_path/bootstrap.sh"
    print_status "Uploaded bootstrap.sh"
}

# Submit EMR job
submit_emr_job() {
    print_status "Submitting EMR job..."
    
    # Generate a unique cluster name with timestamp
    TIMESTAMP=$(date +%Y%m%d-%H%M%S)
    CLUSTER_NAME="emr-airbnb-insights-$TIMESTAMP"
    
    # Build EC2 attributes with optional subnet
    EC2_ATTRIBUTES="InstanceProfile=EMR_EC2_DefaultRole,KeyName=$EC2_KEY_PAIR"
    if [ -n "${EC2_SUBNET_ID:-}" ]; then
        EC2_ATTRIBUTES="$EC2_ATTRIBUTES,SubnetId=$EC2_SUBNET_ID"
    fi
    
    # Submit the cluster with job steps and CloudWatch logging
    CLUSTER_ID=$(aws emr create-cluster \
        --name "$CLUSTER_NAME" \
        --release-label emr-7.10.0 \
        --applications Name=Spark Name=Hadoop \
        --instance-groups \
            InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m4.large \
            InstanceGroupType=CORE,InstanceCount=2,InstanceType=m4.large \
        --service-role EMR_DefaultRole \
        --ec2-attributes "$EC2_ATTRIBUTES" \
        --log-uri "s3://$S3_BUCKET_LOGS/logs/" \
        --bootstrap-actions Path="s3://$S3_BUCKET_SCRIPTS/bootstrap.sh" \
        --steps file://job_step.json \
        --auto-terminate \
        --region "$AWS_REGION" \
\
        --query 'ClusterId' \
        --output text)
    
    print_status "EMR Cluster created with ID: $CLUSTER_ID"
    
    # Manage cluster ID file (handle old clusters and save new one)
    manage_cluster_id "$CLUSTER_ID"
    
    return 0
}

# Create job step configuration
create_job_step() {
    print_status "Creating job step configuration..."
    
    cat > job_step.json << EOF
[
    {
        "Name": "Airbnb Insights PySpark Job",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "Jar": "command-runner.jar",
        "Args": [
            "spark-submit",
            "--deploy-mode", "cluster",
            "--master", "yarn",
            "--conf", "spark.yarn.submit.waitAppCompletion=true",
            "--py-files", "s3://$S3_BUCKET_SCRIPTS/airbnb_package.zip",
            "s3://$S3_BUCKET_SCRIPTS/airbnb_insights_job.py",
            "--listings", "s3://$S3_BUCKET_DATA/$DATA_S3_PREFIX/$LISTINGS_FILE",
            "--calendar", "s3://$S3_BUCKET_DATA/$DATA_S3_PREFIX/$CALENDAR_FILE",
            "--reviews", "s3://$S3_BUCKET_DATA/$DATA_S3_PREFIX/$REVIEWS_FILE",
            "--neighbourhoods", "s3://$S3_BUCKET_DATA/$DATA_S3_PREFIX/$NEIGHBOURHOODS_FILE",
            "--output", "s3://$S3_BUCKET_DATA/$OUTPUT_S3_PREFIX",
            "--output-format", "parquet",
            "--coalesce", "1"
        ]
    }
]
EOF
}

# Monitor job progress
monitor_job() {
    print_status "Monitoring job progress..."
    
    if [ ! -f cluster_id.txt ]; then
        print_error "Cluster ID file not found"
        return 1
    fi
    
    CLUSTER_ID=$(cat cluster_id.txt)
    print_status "Monitoring cluster: $CLUSTER_ID"
    
    while true; do
        STATE=$(aws emr describe-cluster --cluster-id "$CLUSTER_ID" --query 'Cluster.Status.State' --output text --region "$AWS_REGION")
        
        case $STATE in
            "STARTING"|"BOOTSTRAPPING"|"RUNNING")
                print_status "Cluster state: $STATE"
                sleep 30
                ;;
            "WAITING")
                print_status "Cluster is waiting for steps to complete"
                sleep 30
                ;;
            "TERMINATING")
                print_status "Cluster is terminating"
                sleep 30
                ;;
            "TERMINATED")
                print_status "Cluster has terminated successfully"
                break
                ;;
            "TERMINATED_WITH_ERRORS")
                print_error "Cluster terminated with errors"
                break
                ;;
            *)
                print_warning "Unknown cluster state: $STATE"
                sleep 30
                ;;
        esac
    done
}

# Check results
check_results() {
    print_status "Checking job results..."
    
    print_status "Output files in S3:"
    aws s3 ls --recursive "s3://$S3_BUCKET_DATA/$OUTPUT_S3_PREFIX/" || true
    
    print_status "To download results:"
    echo "aws s3 sync s3://$S3_BUCKET_DATA/$OUTPUT_S3_PREFIX/ ./results/"
}

# View CloudWatch logs
view_cloudwatch_logs() {
    print_status "Viewing recent CloudWatch logs..."
    
    local log_groups=(
        "/aws/emr/airbnb-insights/spark-driver"
        "/aws/emr/airbnb-insights/spark-executor"
        "/aws/emr/airbnb-insights/hadoop-yarn"
        "/aws/emr/airbnb-insights/hadoop-mapreduce"
        "/aws/emr/airbnb-insights/steps"
        "/aws/emr/airbnb-insights/yarn-stdout"
        "/aws/emr/airbnb-insights/yarn-stderr"
    )
    
    for log_group in "${log_groups[@]}"; do
        print_status "=== Logs from $log_group ==="
        aws logs describe-log-groups --log-group-name-prefix "$log_group" --region "$AWS_REGION" --query 'logGroups[0].logGroupName' --output text 2>/dev/null | \
        if grep -q "$log_group"; then
            aws logs tail "$log_group" --since 1h --region "$AWS_REGION" 2>/dev/null || \
            print_warning "No recent logs found in $log_group"
        else
            print_warning "Log group $log_group not found"
        fi
        echo ""
    done
    
    print_status "To view live logs:"
    echo "aws logs tail /aws/emr/airbnb-insights/spark-driver --follow --region $AWS_REGION"
}

# Download and extract S3 logs locally
download_cluster_logs() {
    print_status "Downloading cluster logs from S3..."
    
    # Check if cluster ID file exists
    if [ ! -f cluster_id.txt ]; then
        print_error "No cluster_id.txt found. Run '$SCRIPT_COMMAND' first or '$SCRIPT_COMMAND monitor' to check current cluster."
        exit 1
    fi
    
    local cluster_id=$(cat cluster_id.txt)
    print_status "Downloading logs for cluster: $cluster_id"
    
    # Create logs directory
    local logs_dir="./logs/$cluster_id"
    mkdir -p "$logs_dir"
    
    # Download all logs for this cluster from S3
    local s3_logs_path="s3://$S3_BUCKET_LOGS/logs/$cluster_id/"
    
    print_status "Checking available logs in S3..."
    if ! aws s3 ls "$s3_logs_path" --recursive >/dev/null 2>&1; then
        print_error "No logs found for cluster $cluster_id in S3"
        print_warning "Logs may not be available yet or cluster may not have started properly"
        exit 1
    fi
    
    print_status "Downloading logs to $logs_dir..."
    aws s3 sync "$s3_logs_path" "$logs_dir/" --region "$AWS_REGION"
    
    if [ $? -eq 0 ]; then
        print_status "Logs downloaded successfully!"
        
        # Find and extract all .gz files
        print_status "Extracting compressed log files..."
        local gz_count=0
        
        find "$logs_dir" -name "*.gz" -type f | while read -r gz_file; do
            gz_count=$((gz_count + 1))
            local base_name=$(basename "$gz_file" .gz)
            local dir_name=$(dirname "$gz_file")
            
            print_status "Extracting: $(basename "$gz_file")"
            gunzip -c "$gz_file" > "$dir_name/$base_name" 2>/dev/null
            
            if [ $? -eq 0 ]; then
                # Keep the original .gz file and create extracted version
                echo "  -> Created: $dir_name/$base_name"
            else
                print_warning "Failed to extract: $gz_file"
            fi
        done
        
        # Show summary
        print_status "=== Download Summary ==="
        echo "Cluster ID: $cluster_id"
        echo "Local logs directory: $logs_dir"
        echo "Total files downloaded: $(find "$logs_dir" -type f | wc -l)"
        echo "Compressed files found: $(find "$logs_dir" -name "*.gz" | wc -l)"
        echo "Extracted files created: $(find "$logs_dir" -type f ! -name "*.gz" | wc -l)"
        
        print_status "=== Log Categories Available ==="
        if [ -d "$logs_dir/steps" ]; then
            echo "Step logs: $logs_dir/steps/"
        fi
        if [ -d "$logs_dir/containers" ]; then
            echo "Container logs: $logs_dir/containers/"
        fi
        if [ -d "$logs_dir/hadoop-mapreduce" ]; then
            echo "MapReduce logs: $logs_dir/hadoop-mapreduce/"
        fi
        if [ -d "$logs_dir/spark" ]; then
            echo "Spark logs: $logs_dir/spark/"
        fi
        
        print_status "=== Quick Access Commands ==="
        echo "View step errors: find $logs_dir/steps -name 'stderr' -exec cat {} \;"
        echo "View container logs: find $logs_dir/containers -name 'stdout' -exec cat {} \;"
        echo "Search for errors: grep -r 'ERROR\\|Exception\\|Failed' $logs_dir/"
        
    else
        print_error "Failed to download logs from S3"
        exit 1
    fi
}

# Clean up
cleanup() {
    print_status "Cleaning up temporary files..."
    rm -f job_step.json
    # Note: cluster_id.txt is kept for monitoring purposes
    # Note: ./logs/ directory is kept for log analysis
}

# Manage cluster ID file for monitoring
manage_cluster_id() {
    local new_cluster_id=$1
    
    # Check if there's an old cluster ID file
    if [ -f cluster_id.txt ]; then
        local old_cluster_id=$(cat cluster_id.txt)
        print_status "Found existing cluster ID: $old_cluster_id"
        
        # Check if old cluster is still running
        local old_state=$(aws emr describe-cluster --cluster-id "$old_cluster_id" --query 'Cluster.Status.State' --output text --region "$AWS_REGION" 2>/dev/null || echo "NOT_FOUND")
        
        if [[ "$old_state" == "STARTING" || "$old_state" == "BOOTSTRAPPING" || "$old_state" == "RUNNING" || "$old_state" == "WAITING" ]]; then
            print_warning "Old cluster $old_cluster_id is still active (state: $old_state)"
            print_warning "You may want to terminate it: aws emr terminate-clusters --cluster-ids $old_cluster_id --region $AWS_REGION"
        elif [[ "$old_state" != "NOT_FOUND" ]]; then
            print_status "Old cluster $old_cluster_id is in state: $old_state (no action needed)"
        fi
    fi
    
    # Save new cluster ID
    echo "$new_cluster_id" > cluster_id.txt
    print_status "Saved new cluster ID $new_cluster_id to cluster_id.txt"
}

# Show help information
show_help() {
    cat << EOF
EMR Airbnb Insights Job Runner

DESCRIPTION:
    This script automates the process of running the Airbnb insights PySpark workload on AWS EMR.
    It handles cluster provisioning, Spark submission, log wiring, and automatic cleanup.

USAGE:
    $SCRIPT_COMMAND [COMMAND]

COMMANDS:
    (no arguments)  Submit a new EMR job and run the Airbnb insights pipeline
    monitor         Monitor the progress of the current EMR cluster
    results         Check and display job results from S3
    logs            View recent CloudWatch logs from EMR cluster
    dlogs           Download and extract all S3 logs for current cluster to ./logs/
    cleanup         Clean up temporary files
    --help, -h      Show this help message

CONFIGURATION:
    Edit the script variables at the top to customize:
    - S3_BUCKET_SCRIPTS: S3 bucket for storing job scripts
    - S3_BUCKET_DATA: S3 bucket for input/output data  
    - S3_BUCKET_LOGS: S3 bucket for EMR logs
    - AWS_REGION: AWS region to use (currently: $AWS_REGION)
    - EC2_KEY_PAIR: EC2 key pair name (auto-created from ~/.ssh/id_rsa.pub)
    - DATASET_DATE: Inside Airbnb snapshot to stage and process

PREREQUISITES:
    - AWS CLI configured with proper credentials
    - airbnb_insights_job.py and the ./airbnb/ helper package available locally (under pypark/)
    - Inside Airbnb extracts under $LOCAL_DATA_DIR (listings.csv.gz, calendar.csv.gz, reviews.csv.gz, neighbourhoods.csv)
    - configs/bootstrap.sh present (uploaded as the cluster bootstrap action)
    - SSH key pair at ~/.ssh/id_rsa.pub (auto-imported if missing)

WORKFLOW:
    1. Validates AWS configuration and creates EMR roles if needed
    2. Creates/imports EC2 key pair from your SSH keys
    3. Creates CloudWatch log groups and S3 buckets if they don't exist
    4. Packages code + data assets and uploads them to S3
    5. Submits an EMR cluster with the PySpark job and waits for completion
    6. Auto-terminates the cluster after job completion

EXAMPLES:
    $SCRIPT_COMMAND                    # Run the Airbnb insights job with default config
    $SCRIPT_COMMAND monitor            # Monitor current job progress
    $SCRIPT_COMMAND results            # View job outputs in S3
    $SCRIPT_COMMAND logs               # View CloudWatch logs
    $SCRIPT_COMMAND dlogs              # Download all S3 logs locally
    aws s3 sync s3://$S3_BUCKET_DATA/$OUTPUT_S3_PREFIX/ ./results/  # Download results

EMR CONFIGURATION:
    - Release: emr-7.10.0
    - Instance Type: m4.large (master + 2 core nodes)
    - Applications: Spark, Hadoop
    - Auto-terminate: enabled
    - CloudWatch logging: enabled with 7-day retention
    
CLOUDWATCH LOG GROUPS:
    - /aws/emr/airbnb-insights/spark-driver
    - /aws/emr/airbnb-insights/spark-executor
    - /aws/emr/airbnb-insights/hadoop-yarn
    - /aws/emr/airbnb-insights/hadoop-mapreduce
    - /aws/emr/airbnb-insights/steps
    - /aws/emr/airbnb-insights/yarn-stdout
    - /aws/emr/airbnb-insights/yarn-stderr

For more information, see the project README.md
EOF
}

# Main execution
main() {
    print_status "Starting EMR Airbnb Insights Job"
    
    # Check if required files exist
    if [ ! -f "$PYSPARK_DIR/airbnb_insights_job.py" ]; then
        print_error "airbnb_insights_job.py not found under $PYSPARK_DIR"
        exit 1
    fi

    if [ ! -d "$PYSPARK_DIR/airbnb" ]; then
        print_error "airbnb helper package directory not found under $PYSPARK_DIR"
        exit 1
    fi

    if [ ! -d "$LOCAL_DATA_DIR" ]; then
        print_error "Dataset directory not found: $LOCAL_DATA_DIR"
        print_error "Download the Inside Airbnb extracts before running the job"
        exit 1
    fi

    initialize_dataset_files

    # Execute job workflow
    check_aws_config
    check_or_create_key_pair
    check_or_create_emr_roles
    create_cloudwatch_log_groups
    create_s3_buckets
    upload_files
    create_job_step
    submit_emr_job
    
    print_status "Job submitted successfully!"
    print_status "Cluster ID saved to cluster_id.txt"
    print_status ""
    print_status "To monitor the job:"
    print_status "  $SCRIPT_COMMAND monitor"
    print_status ""
    print_status "To check results after completion:"
    print_status "  $SCRIPT_COMMAND results"
    
    cleanup
}

# Handle command line arguments
case "${1:-}" in
    "monitor")
        monitor_job
        ;;
    "results")
        check_results
        ;;
    "logs")
        view_cloudwatch_logs
        ;;
    "dlogs")
        download_cluster_logs
        ;;
    "cleanup")
        cleanup
        ;;
    "--help"|"-h"|"help")
        show_help
        ;;
    *)
        main
        ;;
esac
