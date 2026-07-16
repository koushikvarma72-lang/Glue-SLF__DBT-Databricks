#!/usr/bin/env bash
# Provision the leanest viable RDS Postgres for the CDL control DB, open its
# firewall, and print the endpoint. Glue jobs reach it directly over the internet
# (same egress path they use for bore.pub today) — so the bore tunnel is retired.
#
# Cost: db.t4g.micro + 20GB gp3 single-AZ ≈ $0.016/hr (~$12/mo). DELETE AFTER THE DEMO:
#   aws rds delete-db-instance --db-instance-identifier cdl-control \
#       --skip-final-snapshot --delete-automated-backups
#
# Usage:
#   export AWS_DEFAULT_REGION=us-west-2
#   ./setup_rds_control.sh 'SomeStrongPass123!'
set -euo pipefail

ID=cdl-control
USER=cdladmin
PASS="${1:?usage: ./setup_rds_control.sh <master-password> [subnet-group]}"
REGION="${AWS_DEFAULT_REGION:-us-west-2}"

# This account's VPC has no default subnets, so RDS needs an explicit DB subnet
# group. Default: reuse the one the existing instances already use.
SUBNET_GROUP="${2:-$(aws rds describe-db-instances --db-instance-identifier sai-ls-poc \
  --region "$REGION" --query 'DBInstances[0].DBSubnetGroup.DBSubnetGroupName' \
  --output text 2>/dev/null)}"
echo "== using DB subnet group: $SUBNET_GROUP =="

echo "== creating $ID (db.t4g.micro, 20GB gp3, single-AZ, public) =="
aws rds create-db-instance \
  --db-instance-identifier "$ID" \
  --db-instance-class db.t4g.micro \
  --engine postgres \
  --allocated-storage 20 --storage-type gp3 \
  --master-username "$USER" --master-user-password "$PASS" \
  --db-subnet-group-name "$SUBNET_GROUP" \
  --publicly-accessible --no-multi-az \
  --backup-retention-period 0 \
  --region "$REGION" >/dev/null
echo "   requested. waiting for it to become available (~3-5 min)…"
aws rds wait db-instance-available --db-instance-identifier "$ID" --region "$REGION"

read -r ENDPOINT SG <<<"$(aws rds describe-db-instances --db-instance-identifier "$ID" \
  --region "$REGION" \
  --query 'DBInstances[0].[Endpoint.Address,VpcSecurityGroups[0].VpcSecurityGroupId]' \
  --output text)"

echo "== opening security group $SG on 5432 (demo: 0.0.0.0/0) =="
aws ec2 authorize-security-group-ingress --group-id "$SG" \
  --protocol tcp --port 5432 --cidr 0.0.0.0/0 --region "$REGION" 2>/dev/null \
  || echo "   (ingress rule already present)"

echo "== creating the 'control' database ('control' is reserved for RDS DBName, not for CREATE DATABASE) =="
PGPASSWORD="$PASS" psql -h "$ENDPOINT" -U "$USER" -d postgres \
  -c 'CREATE DATABASE control;' 2>/dev/null || echo "   (control database already exists)"

echo
echo "==================== RDS CONTROL DB READY ===================="
echo "  endpoint : $ENDPOINT"
echo "  port     : 5432   db: control   user: $USER"
echo
echo "Next — seed the schema (from this folder):"
echo "  PGPASSWORD='$PASS' psql -h $ENDPOINT -U $USER -d control -f seed_control_db.sql"
echo "  PGPASSWORD='$PASS' psql -h $ENDPOINT -U $USER -d control -f seed_pipeline_config.sql"
echo
echo "Then run the pipeline with NO bore:"
echo "  python setup_airflow_pipeline.py --control-host $ENDPOINT --control-port 5432 \\"
echo "      --control-db control --control-user $USER --control-password '$PASS' \\"
echo "      --airflow-password <admin-pw> --trigger --watch"
echo "============================================================="
