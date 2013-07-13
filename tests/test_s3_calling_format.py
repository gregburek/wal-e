import boto

from wal_e.s3.calling_format import (_is_mostly_subdomain_compatible,
                                     _is_ipv4_like)

from wal_e.s3 import calling_format


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


def test_subdomain_detect():
    for bn in SUBDOMAIN_OK:
        assert _is_mostly_subdomain_compatible(bn) is True

    for bn in SUBDOMAIN_BOGUS:
        assert _is_mostly_subdomain_compatible(bn) is False


def test_us_standard_default_for_bogus():
    for bn in SUBDOMAIN_BOGUS:
        rinfo = calling_format.from_bucket_name(bn)
        assert rinfo.region == 'us-standard'


def test_ipv4_detect():
    assert _is_ipv4_like('1.1.1.1') is True

    assert _is_ipv4_like('1.1.1.256') is False
    assert _is_ipv4_like('1.1.1.hello') is False
    assert _is_ipv4_like('hello') is False
    assert _is_ipv4_like('-1.1.1.1') is False
    assert _is_ipv4_like('-1.1.1') is False
    assert _is_ipv4_like('-1.1.1.') is False


def test_ambivalence_about_cert_failing_domains():
    for bn in SUBDOMAIN_OK:
        if '.' not in bn:
            rinfo = calling_format.from_bucket_name(bn)
            assert (rinfo.calling_format ==
                    boto.s3.connection.SubdomainCallingFormat)
        else:
            rinfo = calling_format.from_bucket_name(bn)
            assert (rinfo.calling_format ==
                    boto.s3.connection.OrdinaryCallingFormat)
            assert rinfo.region is None
            assert rinfo.ordinary_endpoint is None
