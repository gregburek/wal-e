import boto
import collections

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


def _is_subdomain_convention_ok(bucket_name):
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
            not bucket_name.endswith('.')
            and not _is_ipv4_like(bucket_name))

RegionInfo = collections.namedtuple('RegionInfo',
                                    ['calling_format',

                                     # Only necessarily defined if the
                                     # calling_convention is
                                     # OrdinaryCallingFormat.
                                     'region',
                                     'ordinary_endpoint'])


def calling_format_from_bucket_name(bucket_name):
    if _is_subdomain_convention_ok(bucket_name):
        return RegionInfo(
            calling_format=boto.s3.connection.SubdomainCallingFormat())
    else:
        return RegionInfo(
            region='us-standard',
            calling_format=boto.s3.connection.OrdinaryCallingFormat(),
            ordinary_endpoint='s3.amazonaws.com')
