# Execution

```bash
# Refresh service-account's auth-token for this session
gcloud auth application-default login

# Initialize state file (.tfstate)
terraform init

# Check changes to new infrastructure plan
terraform plan -var="project=<your-gcp-project-id>"
```

```bash
# Create new infrastructure
terraform apply -var="project=<your-gcp-project-id>"
```

```bash
# Delete infrastructure after your work, to avoid costs on any running services
terraform destroy
```
