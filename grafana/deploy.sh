#!/usr/bin/env bash
set -euo pipefail

STACK_NAME="translink-grafana"
REGION="ap-southeast-2"

# --- Validate ---
if [ -z "${KEY_PAIR:-}" ]; then
  echo "Usage: KEY_PAIR=<your-ec2-keypair-name> ./deploy.sh"
  echo ""
  echo "Available key pairs:"
  aws ec2 describe-key-pairs --region "$REGION" --query 'KeyPairs[].KeyName' --output table
  exit 1
fi

MY_IP=$(curl -s https://checkip.amazonaws.com)/32
echo "Restricting SSH + Grafana access to your IP: $MY_IP"

echo "Deploying stack: $STACK_NAME"
aws cloudformation deploy \
  --template-file "$(dirname "$0")/cloudformation.yml" \
  --stack-name "$STACK_NAME" \
  --parameter-overrides \
    KeyPairName="$KEY_PAIR" \
    AllowedSSHCidr="$MY_IP" \
    AllowedGrafanaCidr="$MY_IP" \
  --region "$REGION" \
  --no-fail-on-empty-changeset

echo ""
echo "--- Outputs ---"
aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --query 'Stacks[0].Outputs' \
  --output table

echo ""
echo "NOTE: Grafana takes ~2-3 minutes to start after stack creation."
echo "      Check setup progress: ssh in and run 'tail -f /var/log/grafana-setup.log'"
