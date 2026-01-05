"""Client MinIO pour le stockage d'objets."""

import io
import json
import logging
from datetime import datetime, timedelta, timezone

from config import minio_config
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger("app")


class MinIOStorage:
    def __init__(self):
        self.client = Minio(
            endpoint=minio_config.endpoint,
            access_key=minio_config.access_key,
            secret_key=minio_config.secret_key,
            secure=minio_config.secure,
        )
        self._ensure_buckets()

    def _ensure_buckets(self) -> None:
        """Check if buckets exist; if not, create them."""
        buckets = [
            minio_config.bucket_images,
            minio_config.bucket_exports,
            minio_config.bucket_backups,
        ]

        for bucket in buckets:
            if not self.client.bucket_exists(bucket):
                logger.info(f"Bucket created: {bucket}")
                self.client.make_bucket(bucket)

    def upload_export(
        self, data: bytes, filename: str, content_type: str = "application/octet-stream"
    ) -> str | None:
        """Upload a file to the exports bucket.

        Args:
            data (bytes): File content.
            filename (str): File name.
            content_type (str, optional): MIME type. Defaults to "application/octet-stream".

        Returns:
            str | None: URI MinIO or None.
        """
        try:
            self.client.put_object(
                bucket_name=minio_config.bucket_exports,
                object_name=filename,
                data=io.BytesIO(data),
                length=len(data),
                content_type=content_type,
            )

            uri = f"minio://{minio_config.bucket_exports}/{filename}"
            logger.info(f"Export uploaded: {uri}")
            return uri

        except S3Error as e:
            logger.error(f"Upload failed: {e}")
            return None

    def upload_json(self, data: dict, filename: str) -> str | None:
        """Upload a JSON file to the exports bucket.

        Args:
            data (dict): Dictionary to dump in JSON format.
            filename (str): File name.

        Returns:
            str | None: URI MinIO or None.
        """
        json_bytes = json.dumps(data, indent=4, ensure_ascii=False, default=str).encode(
            "utf-8"
        )
        return self.upload_export(json_bytes, filename, "application/json")

    def upload_csv(self, csv_content: str, filename: str) -> str | None:
        """Upload a JSON file to the exports bucket.

        Args:
            csv_content (str): CSV content.
            filename (str): File name.

        Returns:
            str | None: URI MinIO or None.
        """
        return self.upload_export(csv_content.encode("utf-8"), filename, "text/csv")

    def create_backup(self, data: dict, prefix: str = "backup") -> str | None:
        """Creates a timestamped backup.

        Args:
            data (dict): Data to be backed up.
            prefix (str, optional): File prefix. Defaults to "backup".

        Returns:
            str | None: URI MinIO or None.
        """
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"{prefix}_{timestamp}.json"

        try:
            json_bytes = json.dumps(
                data, indent=4, ensure_ascii=False, default=str
            ).encode("utf-8")

            self.client.put_object(
                bucket_name=minio_config.bucket_backups,
                object_name=filename,
                data=io.BytesIO(json_bytes),
                length=len(json_bytes),
                content_type="application/json",
            )

            uri = f"minio://{minio_config.bucket_backups}/{filename}"
            logger.info(f"Backup created: {uri}")
            return uri

        except S3Error as e:
            logger.error(f"Backup failed: {e}")
            return None

    def upload_image(
        self, image_data: bytes, filename: str, content_type: str = "image/jpeg"
    ) -> str | None:
        """Upload an image to the images bucket.

        Args:
            image_data (bytes): Image to upload.
            filename (str): File name.
            content_type (str, optional): MINE type. Defaults to "image/jpeg".

        Returns:
            str | None: MinIO URI or None.
        """
        try:
            self.client.put_object(
                bucket_name=minio_config.bucket_images,
                object_name=filename,
                data=io.BytesIO(image_data),
                length=len(image_data),
                content_type=content_type,
            )

            uri = f"minio://{minio_config.bucket_images}/{filename}"
            logger.info(f"Image uploaded: {uri}")
            return uri

        except S3Error as e:
            logger.error(f"Image upload failed: {e}")
            return None

    def get_object(self, bucket: str, filename: str) -> bytes | None:
        """Gets an object from a bucket using its file name.

        Args:
            bucket (str): Bucket name.
            filename (str): File name.

        Returns:
            bytes | None: Object get or None.
        """
        try:
            response = self.client.get_object(bucket, filename)
            data = response.read()
            response.close()
            response.release_conn()  # To reuse the connection
            logger.info(f"Object get: minio://{bucket}/{filename}, {len(data)} bytes")
            return data
        except S3Error as e:
            logger.info(f"Get object failed: {e}")
            return None

    def list_objects(self, bucket: str, prefix: str = "") -> list[dict]:
        """List of information about the objects in a bucket based on a prefix.

        Args:
            bucket (str): Bucket name.
            prefix (str, optional): Prefix. Defaults to "".

        Returns:
            list[dict]: List of information about the objects. Keys of dictionaries are "name" (str | None), "size" (int | None) and "modified" (datetime | None).
        """
        objects = self.client.list_objects(bucket, prefix=prefix, recursive=True)
        return [
            {"name": obj.object_name, "size": obj.size, "modified": obj.last_modified}
            for obj in objects
        ]

    def list_images(self) -> list[dict]:
        """List of information about the objects in the images bucket.

        Returns:
            list[dict]: List of information about the objects. Keys of dictionaries are "name" (str | None), "size" (int | None) and "modified" (datetime | None).
        """
        return self.list_objects(minio_config.bucket_images)

    def list_exports(self) -> list[dict]:
        """List of information about the objects in the exports bucket.

        Returns:
            list[dict]: List of information about the objects. Keys of dictionaries are "name" (str | None), "size" (int | None) and "modified" (datetime | None).
        """
        return self.list_objects(minio_config.bucket_exports)

    def list_backups(self) -> list[dict]:
        """List of information about the objects in the backups bucket.

        Returns:
            list[dict]: List of information about the objects. Keys of dictionaries are "name" (str | None), "size" (int | None) and "modified" (datetime | None).
        """
        return self.list_objects(minio_config.bucket_backups)

    def remove_object(self, bucket: str, filename: str) -> bool:
        """Remove an object.

        Args:
            bucket (str): Bucket name.
            filename (str): File name.

        Returns:
            bool: True if the object was successfully deleted, false otherwise.
        """
        try:
            self.client.remove_object(bucket, filename)
            logger.info(f"Object removed: minio://{bucket}/{filename}")
            return True
        except S3Error as e:
            logger.error(f"Object remove error: {e}")
            return False

    def get_presigned_url(
        self, bucket: str, filename: str, expires_hours: int = 24
    ) -> str | None:
        """Generates a pre-signed URL.

        Args:
            bucket (str): Bucket name.
            filename (str): File name.
            expires_hours (int, optional): Number of hours before expiration. Defaults to 24.

        Returns:
            str | None: Pre-signed URL.
        """
        try:
            url = self.client.presigned_get_object(
                bucket_name=bucket,
                object_name=filename,
                expires=timedelta(hours=expires_hours),
            )
            logger.info(f"Get presigned URL from minio://{bucket}/{filename}")
            return url
        except S3Error as e:
            logger.error(f"Get presigned URL failed: {e}")
            return None

    def get_storage_stats(self) -> dict:
        """Storage statistics.

        Returns:
            dict: {"bucket_name": {"nb_objects": (int) number of objects, "total_size": (int) sum of objects size in bytes}, ...}
        """
        stats = {}

        for bucket in [minio_config.bucket_exports, minio_config.bucket_backups]:
            objects = self.list_objects(bucket)
            stats[bucket] = {
                "nb_objects": len(objects),
                "total_size": sum(o["size"] for o in objects),
            }

        return stats
