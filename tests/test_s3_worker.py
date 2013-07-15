import boto
import contextlib
import os
import pytest

from boto.s3.connection import (
    OrdinaryCallingFormat,
    SubdomainCallingFormat,
)
from s3_integration_help import (
    no_real_s3_credentials,
    FreshBucket,
)
from wal_e.worker import s3_worker


@pytest.mark.skipif("no_real_s3_credentials()")
def test_s3_endpoint_for_west_uri():
    """Integration test for bucket naming issues

    AWS credentials and WALE_S3_INTEGRATION_TESTS must be set to run
    this test.
    """
    import boto.s3.connection

    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    bucket_name = 'wal-e-test-us-west-1' + aws_access_key.lower()
    uri = 's3://{b}'.format(b=bucket_name)

    with FreshBucket(bucket_name,
                     host='s3-us-west-1.amazonaws.com',
                     calling_format=OrdinaryCallingFormat()) as fb:
        fb.create(location='us-west-1')
        result = s3_worker.s3_endpoint_for_uri(uri)

        expected = 's3-us-west-1.amazonaws.com'

        assert result == expected


@pytest.mark.skipif("no_real_s3_credentials()")
def test_301_redirect():
    """Integration test for bucket naming issues

    AWS credentials and WALE_S3_INTEGRATION_TESTS must be set to run
    this test.
    """
    import boto.s3.connection

    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    bucket_name = 'wal-e-test-301-redirect' + aws_access_key.lower()

    with pytest.raises(boto.exception.S3ResponseError) as e:
         # Just initiating the bucket manipulation API calls is enough
         # to provoke a 301 redirect.
        with FreshBucket(bucket_name,
                         calling_format=OrdinaryCallingFormat()) as fb:
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

    # Reach into boto and hollow out create_bucket's validation, which
    # annoyingly in an error message claims that it is incompatible
    # with Subdomain calling format, which presumably might mean: "may
    # work with OrdinaryCallingFormat".  Except that's not true, it's
    # just blacklisted altogether, even though boto can interact with
    # buckets that do not follow the naming conventions, once created.
    monkeypatch.setattr(boto.s3.connection, 'check_lowercase_bucketname',
                        lambda n: True)

    with FreshBucket(bucket_name, calling_format=OrdinaryCallingFormat()):
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

    with pytest.raises(boto.https_connection.InvalidCertificateException):
        with FreshBucket(bucket_name, calling_format=SubdomainCallingFormat()):
            pass


def test_ordinary_calling_format_upcase():
    """Some bucket names have to switch to an older calling format.

    This case tests upper case names -- which are not allowed -- only.
    """

    uri = 's3://InvalidBucket'
    expected = 's3.amazonaws.com'
    result = s3_worker.s3_endpoint_for_uri(uri)
    assert result == expected
