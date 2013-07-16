import boto
import os
import pytest

from boto.s3 import connection
from s3_integration_help import (
    FreshBucket,
    no_real_s3_credentials,
)
from wal_e.s3 import calling_format
from wal_e.s3.calling_format import (
    _is_mostly_subdomain_compatible,
    _is_ipv4_like,
)

SUBDOMAIN_BOGUS = [
    '1.2.3.4',
    'myawsbucket.',
    'myawsbucket-.',
    'my.-awsbucket',
    '.myawsbucket',
    'myawsbucket-',
    '-myawsbucket',
    'my_awsbucket',
    'my..examplebucket',

    # Too short.
    'sh',

    # Too long.
    'long' * 30,
]

SUBDOMAIN_OK = [
    'myawsbucket',
    'my-aws-bucket',
    'myawsbucket.1',
    'my.aws.bucket'
]

# Contrivance to quiet down pyflakes, since pytest does some
# string-evaluation magic in test collection.
no_real_s3_credentials = no_real_s3_credentials


def test_subdomain_detect():
    """Exercie subdomain compatible/incompatible bucket names."""
    for bn in SUBDOMAIN_OK:
        assert _is_mostly_subdomain_compatible(bn) is True

    for bn in SUBDOMAIN_BOGUS:
        assert _is_mostly_subdomain_compatible(bn) is False


def test_us_standard_default_for_bogus():
    """Test degradation to us-standard for all weird bucket names.

    Such bucket names are not supported outside of us-standard by
    WAL-E.
    """
    for bn in SUBDOMAIN_BOGUS:
        cinfo = calling_format.from_bucket_name(bn)
        assert cinfo.region == 'us-standard'


def test_cert_validation_sensitivity():
    """Test degradation of dotted bucket names to OrdinaryCallingFormat

    Although legal bucket names with SubdomainCallingFormat, these
    kinds of bucket names run afoul certification validation, and so
    they are forced to fall back to OrdinaryCallingFormat.
    """
    for bn in SUBDOMAIN_OK:
        if '.' not in bn:
            cinfo = calling_format.from_bucket_name(bn)
            assert (cinfo.calling_format ==
                    boto.s3.connection.SubdomainCallingFormat)
        else:
            assert '.' in bn

            cinfo = calling_format.from_bucket_name(bn)
            assert (cinfo.calling_format == connection.OrdinaryCallingFormat)
            assert cinfo.region is None
            assert cinfo.ordinary_endpoint is None


@pytest.mark.skipif("no_real_s3_credentials()")
def test_real_get_location():
    """Exercise a case where a get location call is needed.

    In cases where a bucket has offensive characters, like dots, that
    would otherwise break TLS, test the sniffing the right endpoint
    that can be used to address the bucket.
    """
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    bucket_name = 'wal-e-test-us-west-1.get.location.' + aws_access_key.lower()

    cinfo = calling_format.from_bucket_name(bucket_name)

    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

    with FreshBucket(bucket_name,
                     host='s3-us-west-1.amazonaws.com',
                     calling_format=connection.OrdinaryCallingFormat()) as fb:
        fb.create(location='us-west-1')
        conn = cinfo.connect(aws_access_key_id, aws_secret_access_key)

        assert cinfo.region == 'us-west-1'
        assert cinfo.calling_format is connection.OrdinaryCallingFormat
        assert conn.host == 's3-us-west-1.amazonaws.com'


@pytest.mark.skipif("no_real_s3_credentials()")
def test_subdomain_compatible():
    """Exercise a case where connecting is region-oblivious."""
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    bucket_name = 'wal-e-test-us-west-1-no-dots' + aws_access_key.lower()

    cinfo = calling_format.from_bucket_name(bucket_name)

    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

    with FreshBucket(bucket_name,
                     host='s3-us-west-1.amazonaws.com',
                     calling_format=connection.OrdinaryCallingFormat()) as fb:
        fb.create(location='us-west-1')
        conn = cinfo.connect(aws_access_key_id, aws_secret_access_key)

        assert cinfo.region is None
        assert cinfo.calling_format is connection.SubdomainCallingFormat
        assert isinstance(conn.calling_format,
                          connection.SubdomainCallingFormat)


def test_ipv4_detect():
    """IPv4 lookalikes are not valid SubdomainCallingFormat names

    Even though they otherwise follow the bucket naming rules,
    IPv4-alike names are called out as specifically banned.
    """
    assert _is_ipv4_like('1.1.1.1') is True

    assert _is_ipv4_like('1.1.1.256') is False
    assert _is_ipv4_like('1.1.1.hello') is False
    assert _is_ipv4_like('hello') is False
    assert _is_ipv4_like('-1.1.1.1') is False
    assert _is_ipv4_like('-1.1.1') is False
    assert _is_ipv4_like('-1.1.1.') is False
