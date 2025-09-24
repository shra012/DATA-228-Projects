#!/bin/bash
# Bootstrap script for EMR cluster with CloudWatch logging
set -e

echo "Starting bootstrap script..."

sudo yum update -y

# Installing CloudWatch agent
echo "Installing CloudWatch agent..."
wget https://s3.amazonaws.com/amazoncloudwatch-agent/amazon_linux/amd64/latest/amazon-cloudwatch-agent.rpm
sudo rpm -U ./amazon-cloudwatch-agent.rpm

# Creating CloudWatch agent configuration
sudo mkdir -p /opt/aws/amazon-cloudwatch-agent/etc/
cat <<'CWCONFIG' | sudo tee /opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json
{
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "/var/log/spark/spark-*.log",
            "log_group_name": "/aws/emr/airbnb-insights/spark-driver",
            "log_stream_name": "{instance_id}/spark-driver"
          },
          {
            "file_path": "/var/log/hadoop-yarn/yarn-*.log",
            "log_group_name": "/aws/emr/airbnb-insights/hadoop-yarn",
            "log_stream_name": "{instance_id}/yarn"
          },
          {
            "file_path": "/var/log/hadoop-mapreduce/mapred-*.log",
            "log_group_name": "/aws/emr/airbnb-insights/hadoop-mapreduce",
            "log_stream_name": "{instance_id}/mapreduce"
          }
        ]
      }
    }
  }
}
CWCONFIG

# Start CloudWatch agent
echo "Starting CloudWatch agent..."
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
    -a fetch-config -m ec2 -s \
    -c file:/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json

echo "Bootstrap completed successfully"
