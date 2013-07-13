import boto
import collections


_S3_REGIONS = {
    # A map like this is actually defined in boto.s3 in newer versions of boto
    # but we reproduce it here for the folks (notably, Ubuntu 12.04) on older
    # versions.
    'ap-northeast-1': 's3-ap-northeast-1.amazonaws.com',
    'ap-southeast-1': 's3-ap-southeast-1.amazonaws.com',
    'ap-southeast-2': 's3-ap-southeast-2.amazonaws.com',
    'eu-west-1': 's3-eu-west-1.amazonaws.com',
    'us-standard': 's3.amazonaws.com',
    'us-west-1': 's3-us-west-1.amazonaws.com',
    'us-west-2': 's3-us-west-2.amazonaws.com',
}

try:
    # Override the hard-coded region map with boto's mappings if
    # available.
    from boto.s3 import regions
    _S3_REGIONS.update(dict((r.name, r.endpoint) for r in regions()))
except ImportError:
    pass


def _is_ipv4_like(s):
    parts = s.split('.')

    if len(parts) != 4:
        return False

    for part in parts:
        try:
            number = int(part)
        except ValueError:
            return False

        if number < 0 or number > 255:
            return False

    return True


def _is_mostly_subdomain_compatible(bucket_name):
    """Returns True if SubdomainCallingFormat can be used...mostly

    This checks to make sure that putting aside certificate validation
    issues that a bucket_name is able to use the
    SubdomainCallingFormat.
    """
    return (bucket_name.lower() == bucket_name and
            len(bucket_name) >= 3 and
            len(bucket_name) <= 63 and
            '_' not in bucket_name and
            '..' not in bucket_name and
            '-.' not in bucket_name and
            '.-' not in bucket_name and
            not bucket_name.startswith('-') and
            not bucket_name.endswith('-') and
            not bucket_name.startswith('.') and
            not bucket_name.endswith('.') and
            not _is_ipv4_like(bucket_name))


CallingInfo = collections.namedtuple('CallingInfo',
                                    ['calling_format',

                                     # Only necessarily defined if the
                                     # calling_convention is
                                     # OrdinaryCallingFormat.
                                     'region',
                                     'ordinary_endpoint'])


def from_bucket_name(bucket_name):
    mostly_ok = _is_mostly_subdomain_compatible(bucket_name)

    if not mostly_ok:
        return CallingInfo(
            region='us-standard',
            calling_format=boto.s3.connection.OrdinaryCallingFormat,
            ordinary_endpoint=_S3_REGIONS['us-standard'])
    else:
        if '.' in bucket_name:
            # The bucket_name might have been DNS compatible, but once
            # dots are involved TLS certificate validations will
            # certainly fail even if that's the case.
            #
            # Leave it to the caller to perform the API call, as to
            # avoid teaching this part of the code about credentials.
            return CallingInfo(
                calling_format=boto.s3.connection.OrdinaryCallingFormat,
                region=None,
                ordinary_endpoint=None)
        else:
            # SubdomainCallingFormat can be used, with TLS,
            # world-wide, and WAL-E can be region-oblivious.
            #
            # This is because there are no dots in the bucket name,
            # and no other bucket naming abnormalities either.
            return CallingInfo(
                calling_format=boto.s3.connection.SubdomainCallingFormat,
                region=None,
                ordinary_endpoint=None)

    assert False
