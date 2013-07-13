import boto
import contextlib
import os
import pytest
import traceback

from wal_e.worker import s3_worker


def no_real_s3_credentials():
    for e_var in ('AWS_ACCESS_KEY_ID',
                  'AWS_SECRET_ACCESS_KEY',
                  'WALE_S3_INTEGRATION_TESTS'):
        if os.getenv(e_var) is None:
            return True

    return False


@contextlib.contextmanager
def FreshBucket(conn, bucket_name, **kwargs):
    # Clean up a dangling bucket from a previous test run, if
    # necessary.
    try:
        conn.delete_bucket(bucket_name)
    except boto.exception.S3ResponseError, e:
        if e.status == 404:
            # If the bucket is already non-existent, then the bucket
            # need not be destroyed from a prior test run.
            pass
        else:
            raise

    # Create the desired bucket.
    while True:
        try:
            bucket = conn.create_bucket(bucket_name, **kwargs)
        except boto.exception.S3CreateError, e:
            if e.status == 409:
                # Conflict; bucket already created -- probably means
                # the prior delete did not process just yet.
                continue

            raise

        break

    yield bucket

    # Delete the bucket again.
    while True:
        try:
            conn.delete_bucket(bucket_name)
        except boto.exception.S3ResponseError, e:
            if e.status == 404:
                # Create not yet visible, but it just happened above:
                # keep trying.  Potential consistency.
                continue
            else:
                raise

        break


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

    conn = boto.s3.connection.S3Connection(
        calling_format=boto.s3.connection.SubdomainCallingFormat())

    with FreshBucket(conn, bucket_name, location='us-west-1'):
        expected = 's3-us-west-1.amazonaws.com'
        result = s3_worker.s3_endpoint_for_uri(uri)

        assert result == expected


@pytest.mark.skipif("no_real_s3_credentials()")
def test_301_redirect():
    """Integration test for bucket naming issues

    AWS credentials and WALE_S3_INTEGRATION_TESTS must be set to run
    this test.
    """
    import boto.s3.connection

    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    bucket_name = 'wal-e-test-west' + aws_access_key.lower()
    uri = 's3://{b}'.format(b=bucket_name)

    conn = boto.s3.connection.S3Connection(
        calling_format=boto.s3.connection.OrdinaryCallingFormat())

    with pytest.raises(boto.exception.S3ResponseError) as e:
         # Just initiating the bucket manipulation API calls is enough
         # to provoke a 301 redirect.
        with FreshBucket(conn, bucket_name, location='us-west-1'):
            pass

    assert e.value.status == 301


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

    # Reach into boto and hollow out create_bucket's validation, which
    # annoyingly in an error message claims that it is incompatible
    # with Subdomain and Virtualhosted calling formats, which
    # presumably might mean: "may work with OrdinaryCallingFormat".
    # Except that's not true, it's just blacklisted altogether, even
    # though boto can interact with buckets that do not follow the
    # naming conventions, once created.
    monkeypatch.setattr(boto.s3.connection, 'check_lowercase_bucketname',
                        lambda n: True)

    with FreshBucket(conn, bucket_name):
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
    bucket_name = 'wal-e.test.dots.' + aws_access_key.lower()

    conn = boto.s3.connection.S3Connection(
        calling_format=boto.s3.connection.SubdomainCallingFormat())

    with pytest.raises(boto.https_connection.InvalidCertificateException):
        with FreshBucket(conn, bucket_name):
            pass


def test_ordinary_calling_format_upcase():
    """Some bucket names have to switch to an older calling format.

    This case tests upper case names -- which are not allowed -- only.
    """

    uri = 's3://InvalidBucket'
    expected = 's3.amazonaws.com'
    result = s3_worker.s3_endpoint_for_uri(uri)
    assert result == expected
