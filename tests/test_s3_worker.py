import os
import pytest
import contextlib
import traceback

from wal_e.worker import s3_worker


def no_real_s3_credentials():
    for e_var in ('AWS_ACCESS_KEY_ID',
                  'AWS_SECRET_ACCESS_KEY',
                  'WALE_S3_INTEGRATION_TESTS'):
        if os.getenv(e_var) is None:
            return True

    return False


def _lackadaisical_delete_bucket(conn, bucket_name):
    """Clean up a created test bucket on a best-effort basis."""

    try:
        conn.delete_bucket(bucket_name)
    except StandardError:
        traceback.print_exc()
    

@contextlib.contextmanager
def BucketCleanup(conn, bucket_name):
    _lackadaisical_delete_bucket(conn, bucket_name)
    yield
    _lackadaisical_delete_bucket(conn, bucket_name)


@pytest.mark.skipif("no_real_s3_credentials()")
def test_s3_endpoint_for_west_uri():
    """Integration test for bucket naming issues

    AWS credentials and WALE_S3_INTEGRATION_TESTS must be set to run
    this test.
    """
    import boto.s3.connection

    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    bucket_name = 'wal-e-test-west' + aws_access_key.lower()
    uri = 's3://{b}'.format(b=bucket_name)

    conn = boto.s3.connection.S3Connection()

    with BucketCleanup(conn, bucket_name):
        conn.create_bucket(bucket_name, location='us-west-1')

        expected = 's3-us-west-1.amazonaws.com'
        result = s3_worker.s3_endpoint_for_uri(uri)

        assert result == expected


@pytest.mark.skipif("no_real_s3_credentials()")
def test_s3_endpoint_for_upcase_bucket(monkeypatch):
    """Integration test for bucket naming issues

    AWS credentials and WALE_S3_INTEGRATION_TESTS must be set to run
    this test.
    """
    import boto.s3.connection

    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    bucket_name = 'wal-e-test-'.upper() + aws_access_key.upper()
    uri = 's3://{b}'.format(b=bucket_name)

    conn = boto.s3.connection.S3Connection(
        calling_format=boto.s3.connection.OrdinaryCallingFormat())

    with BucketCleanup(conn, bucket_name):
        # Reach into boto and hollow out create_bucket's validation,
        # which annoyingly in an error message claims that it is
        # incompatible with Subdomain and Virtualhosted calling
        # formats, which presumably is meant to mean: "may work with
        # OrdinaryCallingFormat".
        #
        # And it would, except for this unconditional check that is
        # being undone here.
        monkeypatch.setattr(boto.s3.connection, 'check_lowercase_bucketname',
                            lambda n: True)
        conn.create_bucket(bucket_name)

        expected = 's3.amazonaws.com'
        result = s3_worker.s3_endpoint_for_uri(uri)

        assert result == expected


@pytest.mark.skipif("no_real_s3_credentials()")
def test_get_bucket_vs_certs():
    """Integration test for bucket naming issues

    AWS credentials and WALE_S3_INTEGRATION_TESTS must be set to run
    this test.
    """
    import boto.s3.connection

    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')

    # Add dots to try to trip up TLS certificate validation.
    bucket_name = 'wal-e.test.eu-west' + aws_access_key.lower()

    conn = boto.s3.connection.S3Connection(
        calling_format=boto.s3.connection.SubdomainCallingFormat())
        #calling_format=boto.s3.connection.OrdinaryCallingFormat())

    with BucketCleanup(conn, bucket_name):
        conn.create_bucket(bucket_name, location='eu-west-1')

        bucket = conn.get_bucket(bucket_name)
        bucket.get_all_keys()
        assert bucket.get_location() == 'eu-west-1'

def test_ordinary_calling_format_upcase():
    """Some bucket names have to switch to an older calling format.

    This case tests upper case names -- which are not allowed -- only.
    """

    uri = 's3://InvalidBucket'
    expected = 's3.amazonaws.com'
    result = s3_worker.s3_endpoint_for_uri(uri)
    assert result == expected
