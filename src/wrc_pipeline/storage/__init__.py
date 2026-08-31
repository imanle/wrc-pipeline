"""Storage clients: MongoDB (metadata) and S3/MinIO (documents).

Deliberately outside the Scrapy package so the transformation stage and the
Dagster assets use the exact same clients as the spider.
"""
