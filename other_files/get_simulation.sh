#!/bin/zsh

# Check if the correct number of arguments is provided
if [ "$#" -lt 1 ]; then
  echo "Usage: $0 <simulation_number> [environment]"
  exit 1
fi

# Assign command-line arguments to variables
SIMULATION_NUMBER=$1
ENVIRONMENT=${2:-prod}  # Default to 'prod' if not provided
BUCKET_ENV=${3:-prd}
# Construct the S3 source and destination paths
S3_SOURCE_PATH="s3://optistow-simulation-bucket-${BUCKET_ENV}/simulation_${SIMULATION_NUMBER}_${ENVIRONMENT}"
LOCAL_DESTINATION_PATH="/Users/mdiakhate/Documents/CMA_CGM/Optistow/preprocessing_v2/data/simulations/simulation_${SIMULATION_NUMBER}_${ENVIRONMENT}"
# Execute the AWS S3 copy command
aws s3 cp --recursive --profile $ENVIRONMENT $S3_SOURCE_PATH $LOCAL_DESTINATION_PATH

echo "Files copied from $S3_SOURCE_PATH to $LOCAL_DESTINATION_PATH"