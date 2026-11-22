#!/usr/bin/env python3
"""SeaweedFS (S3) helpers for Part 9."""

import hashlib

import common


def client():
    """One S3 client per thread, on SeaweedFS (path-style addressing)."""
    import boto3
    from botocore.config import Config
    return boto3.client(
        "s3", endpoint_url=common.env("CK_S3_ENDPOINT"),
        aws_access_key_id=common.env("CK_S3_ACCESS_KEY"),
        aws_secret_access_key=common.env("CK_S3_SECRET_KEY"),
        region_name="us-east-1",
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5}))


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def ensure_bucket(s3, bucket):
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception:
        s3.create_bucket(Bucket=bucket)


def list_keys(s3, bucket, prefix):
    """Every key under prefix, sorted."""
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", [])]
    return sorted(keys)
