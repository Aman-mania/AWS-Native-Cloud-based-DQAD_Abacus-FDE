#!/bin/bash

# DQAD Deployment Script
# This script automates the deployment of the entire DQAD platform

set -e  # Exit on error

echo "================================================"
echo "DQAD Platform Deployment Script"
echo "================================================"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Functions
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}→ $1${NC}"
}

# Check prerequisites
check_prerequisites() {
    print_info "Checking prerequisites..."
    
    if ! command -v python3 &> /dev/null; then
        print_error "Python 3 is not installed"
        exit 1
    fi
    print_success "Python 3 found"
    
    if ! command -v terraform &> /dev/null; then
        print_error "Terraform is not installed"
        exit 1
    fi
    print_success "Terraform found"
    
    if ! command -v aws &> /dev/null; then
        print_error "AWS CLI is not installed"
        exit 1
    fi
    print_success "AWS CLI found"
    
    echo ""
}

# Generate synthetic data
generate_data() {
    print_info "Generating synthetic payer data..."
    cd data
    pip install -q -r requirements.txt
    python generate_payer_data.py
    cd ..
    print_success "Synthetic data generated"
    echo ""
}

# Package Lambda functions
package_lambdas() {
    print_info "Packaging Lambda functions..."
    
    # Cost Collector
    cd lambda/cost_collector
    pip install -q -r requirements.txt -t .
    zip -q -r deployment.zip . -x "*.pyc" -x "__pycache__/*"
    cd ../..
    print_success "Cost Collector Lambda packaged"
    
    # Orchestrator
    cd lambda/orchestrator
    pip install -q -r requirements.txt -t .
    zip -q -r deployment.zip . -x "*.pyc" -x "__pycache__/*"
    cd ../..
    print_success "Orchestrator Lambda packaged"
    
    echo ""
}

# Deploy infrastructure
deploy_infrastructure() {
    print_info "Deploying AWS infrastructure with Terraform..."
    
    cd infra/terraform
    
    # Check if tfvars exists
    if [ ! -f terraform.tfvars ]; then
        print_error "terraform.tfvars not found. Please create it from terraform.tfvars.example"
        exit 1
    fi
    
    terraform init
    terraform plan -out=tfplan
    
    read -p "Do you want to apply this Terraform plan? (yes/no): " confirm
    if [ "$confirm" == "yes" ]; then
        terraform apply tfplan
        print_success "Infrastructure deployed"
    else
        print_info "Terraform apply cancelled"
        exit 0
    fi
    
    cd ../..
    echo ""
}

# Upload data to S3
upload_data() {
    print_info "Uploading data to S3..."
    
    BUCKET_NAME="dqad-raw-dev"
    
    # Check if bucket exists
    if aws s3 ls "s3://$BUCKET_NAME" 2>&1 | grep -q 'NoSuchBucket'; then
        print_error "S3 bucket $BUCKET_NAME does not exist. Deploy infrastructure first."
        exit 1
    fi
    
    aws s3 sync raw_data/ "s3://$BUCKET_NAME/claims/" --exclude "*.git/*"
    print_success "Data uploaded to S3"
    echo ""
}

# Deploy Databricks notebook
deploy_databricks() {
    print_info "Deploying Databricks notebook..."
    
    # Check if databricks CLI is configured
    if [ ! -f ~/.databrickscfg ]; then
        print_info "Databricks CLI not configured. Skipping..."
        return
    fi
    
    databricks workspace import \
        --language PYTHON \
        --format SOURCE \
        --overwrite \
        databricks/notebook_etl.py \
        /Workspace/DQAD/notebook_etl
    
    print_success "Databricks notebook deployed"
    echo ""
}

# Main deployment flow
main() {
    echo "Starting DQAD deployment..."
    echo ""
    
    check_prerequisites
    
    read -p "Generate synthetic data? (y/n): " gen_data
    if [ "$gen_data" == "y" ]; then
        generate_data
    fi
    
    read -p "Package Lambda functions? (y/n): " pkg_lambda
    if [ "$pkg_lambda" == "y" ]; then
        package_lambdas
    fi
    
    read -p "Deploy AWS infrastructure? (y/n): " deploy_infra
    if [ "$deploy_infra" == "y" ]; then
        deploy_infrastructure
    fi
    
    read -p "Upload data to S3? (y/n): " upload
    if [ "$upload" == "y" ]; then
        upload_data
    fi
    
    read -p "Deploy Databricks notebook? (y/n): " deploy_db
    if [ "$deploy_db" == "y" ]; then
        deploy_databricks
    fi
    
    echo ""
    echo "================================================"
    print_success "DQAD Deployment Complete!"
    echo "================================================"
    echo ""
    echo "Next steps:"
    echo "1. Configure Databricks job (if not already done)"
    echo "2. Run the Databricks ETL job"
    echo "3. Launch the Streamlit dashboard: cd dashboard && streamlit run app.py"
    echo "4. Monitor CloudWatch metrics and alarms"
    echo ""
}

# Run main function
main
