# Guide: Uploading PySpark Image to AWS ECR via Podman

This guide provides step-by-step instructions for preparing and pushing a Docker-compatible image from a local Windows machine to a private Amazon ECR repository.

## 0. Prerequisites
* **AWS CLI**: Installed and configured via `aws configure` (Region: `us-east-1`).
* **Podman**: Path to `podman.exe` added to the system PATH.
* **Podman Machine**: Virtual machine initialized and running.
    ```powershell
    podman machine start
    ```

---

## 1. Navigate to the Working Directory
Open PowerShell and navigate to the folder containing your `Dockerfile` and `job.py` files.
```powershell
cd "C:\path\to\your\project\folder"
```

## 2. Authenticate with Amazon ECR
Retrieve a temporary password from AWS and pass it to Podman to log in to your registry.
```powershell
aws ecr get-login-password --region us-east-1 | podman login --username AWS --password-stdin 380078849537.dkr.ecr.us-east-1.amazonaws.com
```

## 3. Build the Image
Create a local image named `pyspark-batch-repo` based on your Dockerfile.
```powershell
podman build -t pyspark-batch-repo .
```
> [!TIP]
> If you are on an ARM processor (e.g., Apple M-series or Surface Pro 11), add the flag `--platform linux/amd64` to ensure compatibility with Fargate.

## 4. Tag the Image
Assign a full address pointing to your AWS ECR repository to the image.
```powershell
podman tag pyspark-batch-repo:latest 380078849537.dkr.ecr.us-east-1.amazonaws.com/pyspark-batch-repo:latest
```

## 5. Push to the Repository
Upload the finished container to the cloud.
```powershell
podman push 380078849537.dkr.ecr.us-east-1.amazonaws.com/pyspark-batch-repo:latest
```

---

## Result
Once these commands are executed, the image will appear in the AWS Management Console at:
`Amazon ECR > Repositories > pyspark-batch-repo`.