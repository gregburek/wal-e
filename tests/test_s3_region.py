from wal_e.s3 import region


def test_ipv4_detect():
    assert region._is_ipv4_like('1.1.1.1') is True

    assert region._is_ipv4_like('1.1.1.256') is False
    assert region._is_ipv4_like('1.1.1.hello') is False
    assert region._is_ipv4_like('hello') is False
    assert region._is_ipv4_like('-1.1.1.1') is False
    assert region._is_ipv4_like('-1.1.1') is False
    assert region._is_ipv4_like('-1.1.1.') is False


def test_subdomain_detect():
    assert region._is_subdomain_convention_ok('myawsbucket') is True
    assert region._is_subdomain_convention_ok('my.aws.bucket') is True
    assert region._is_subdomain_convention_ok('myawsbucket.1') is True

    assert region._is_subdomain_convention_ok('1.2.3.4') is False
    assert region._is_subdomain_convention_ok('myawsbucket.') is False
    assert region._is_subdomain_convention_ok('my..examplebucket') is False
