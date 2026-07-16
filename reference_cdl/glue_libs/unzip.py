"""unzip — reconstructed helper (deflate64-tolerant zip extraction to S3).

Only used by landing_to_raw for zipped source systems; the veeva_crm demo flow
never calls it, but the import must resolve. Streams a zip from S3, extracts
members, and uploads them under the target prefix.
"""

import io
import zipfile

import boto3


def unzip_and_upload_deflate64_zip(source_bucket, source_key, target_bucket,
                                   target_prefix, **_kwargs):
    s3 = boto3.client("s3")
    body = s3.get_object(Bucket=source_bucket, Key=source_key)["Body"].read()
    try:
        zf = zipfile.ZipFile(io.BytesIO(body))
    except NotImplementedError:  # deflate64 — needs the zipfile-deflate64 package
        import zipfile_deflate64  # noqa: F401  (registers the decompressor)
        zf = zipfile.ZipFile(io.BytesIO(body))
    uploaded = []
    for member in zf.infolist():
        if member.is_dir():
            continue
        name = member.filename.split("/")[-1]
        key = f"{target_prefix.rstrip('/')}/{name}"
        s3.upload_fileobj(zf.open(member), Bucket=target_bucket, Key=key)
        uploaded.append(key)
    return uploaded
