import boto
import collections


_S3_REGIONS = {
    # A map like this is actually defined in boto.s3 in newer versions of boto
    # but we reproduce it here for the folks (notably, Ubuntu 12.04) on older
    # versions.
    'us-west-1': 's3-us-west-1.amazonaws.com',
    'us-west-2': 's3-us-west-2.amazonaws.com',
    'ap-northeast-1': 's3-ap-northeast-1.amazonaws.com',
    'ap-southeast-1': 's3-ap-southeast-1.amazonaws.com',
    'ap-southeast-2': 's3-ap-southeast-2.amazonaws.com',
    'eu-west-1': 's3-eu-west-1.amazonaws.com',
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
                                    ['region',
                                     'calling_convention',
                                     'ordinary_endpoint'])


class RegionGuesswork(object):
    def __init__(self):
        self.cache = {}

    def guess_for(self, bucket_name):
        if bucket_name in self.cache:
            return self.cache[bucket_name]

        if not _is_subdomain_convention_ok():
            # Handle case of legacy S3 API calling formats.
            #
            # WAL-E only attempts to support this in the 'us-standard'
            # region, as to not regress previous support for that, but
            # makes no attempt to support these kinds of bucket names
            # in new regions.
            #
            # The most common reason that people do this is because
            # AWS unceremoniously allows creation of upper-case bucket
            # names in S3 classic and in some older regions.  There
            # are other restrictions (because the new format has
            # absorbed restrictions seen in the DNS standards), but in
            # practice people only seem to report this issue, so for
            # now just handle that case.
            info = RegionInfo(
                region='us-standard',
                calling_convention=boto.s3.connection.OrdinaryCallingFormat,
                ordinary_endpoint='s3.amazonaws.com'
            )

            self.cache[bucket_name] = info

            logger.warning(msg='upper case buckets will be deprecated',
                           detail=('The offending bucket name is {0}.'
                                   .format(bucket_name)),
                           hint=('Upper case bucket names do not work in '
                                 'newer regions and cannot use the newer '
                                 'preferred S3 calling conventions.'))
        else:
            # Attempting to use .get_bucket() with OrdinaryCallingFormat raises
            # a S3ResponseError (status 301).  See boto/443 referenced above.
            c_kwargs['calling_format'] = SubdomainCallingFormat()
            conn = connection or S3Connection(*c_args, **c_kwargs)
            bucket = conn.get_bucket(bucket_name)

            try:
                location = bucket.get_location()
            except boto.exception.S3ResponseError, e:
                if e.status == 403:
                    # A 403 can be caused by IAM keys that do not
                    # permit GetBucketLocation.  To not change
                    # behavior for environments that do not have
                    # GetBucketLocation allowed, fall back to the
                    # default endpoint, preserving behavior for those
                    # using S3-Classic.
                    s3_endpoint_for_uri.cache[bucket_name] = default
                else:
                    raise
            else:
                s3_endpoint_for_uri.cache[bucket_name] = \
                    _S3_REGIONS.get(location, default)

        # Cache with connection info must be filled by this point
        return self.cache[bucket_name]
