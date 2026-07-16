"""zip_extracted — reconstructed helper (chunked zip extraction to S3).

Call surface (from landing_to_raw.extractFilesFromS3):
    unzip_and_upload(source_bucket, source_key, target_bucket, target_directory,
                     max_workers=100, chunk_size=512*1024*1024)

The veeva_crm demo flow never reaches it (no zip source systems), but the import
must resolve. Sequential implementation — correct over clever for the reference kit.
"""

import io
import zipfile

import boto3


def unzip_and_upload(source_bucket, source_key, target_bucket, target_directory,
                     max_workers=None, chunk_size=None, **_kwargs):
    s3 = boto3.client("s3")
    body = s3.get_object(Bucket=source_bucket, Key=source_key)["Body"].read()
    zf = zipfile.ZipFile(io.BytesIO(body))
    uploaded = []
    for member in zf.infolist():
        if member.is_dir():
            continue
        name = member.filename.split("/")[-1]
        key = f"{target_directory.rstrip('/')}/{name}"
        s3.upload_fileobj(zf.open(member), Bucket=target_bucket, Key=key)
        uploaded.append(key)
    return uploaded
