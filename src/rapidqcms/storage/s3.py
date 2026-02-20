from pathlib import Path

import boto3
from botocore.exceptions import ClientError


class S3StorageBackend:
    """AWS S3-backed storage for production.

    Credentials are picked up from the standard boto3 chain:
    IAM instance profile (EC2/ECS), environment variables
    (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY), or ~/.aws/credentials.

    Set via environment:
        RAPIDQCMS_STORAGE_BACKEND=s3
        RAPIDQCMS_S3_BUCKET=my-rapidqcms-bucket
        RAPIDQCMS_S3_PREFIX=rapidqcms          # optional, default "rapidqcms"
    """

    def __init__(self, bucket: str, prefix: str = ""):
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = boto3.client("s3")
        return self._client

    def _key(self, remote_key: str) -> str:
        if self.prefix:
            return f"{self.prefix}/{remote_key}"
        return remote_key

    def _strip_prefix(self, full_key: str) -> str:
        if self.prefix and full_key.startswith(self.prefix + "/"):
            return full_key[len(self.prefix) + 1:]
        return full_key

    def upload(self, local_path: Path, remote_key: str) -> None:
        self.client.upload_file(str(local_path), self.bucket, self._key(remote_key))

    def download(self, remote_key: str, local_path: Path) -> None:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        self.client.download_file(self.bucket, self._key(remote_key), str(local_path))

    def exists(self, remote_key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=self._key(remote_key))
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    def list_keys(self, prefix: str = "") -> list[str]:
        full_prefix = self._key(prefix) if prefix else (self.prefix or "")
        paginator = self.client.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=full_prefix):
            for obj in page.get("Contents", []):
                keys.append(self._strip_prefix(obj["Key"]))
        return keys

    def delete(self, remote_key: str) -> None:
        self.client.delete_object(Bucket=self.bucket, Key=self._key(remote_key))
