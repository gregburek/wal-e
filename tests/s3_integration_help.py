import boto
import contextlib
import os


def no_real_s3_credentials():
    for e_var in ('AWS_ACCESS_KEY_ID',
                  'AWS_SECRET_ACCESS_KEY',
                  'WALE_S3_INTEGRATION_TESTS'):
        if os.getenv(e_var) is None:
            return True

    return False


def apathetic_bucket_delete(bucket_name, *args, **kwargs):
    conn = boto.s3.connection.S3Connection(*args, **kwargs)

    try:
        conn.delete_bucket(bucket_name)
    except boto.exception.S3ResponseError, e:
        if e.status == 404:
            # If the bucket is already non-existent, then the bucket
            # need not be destroyed from a prior test run.
            pass
        else:
            raise

    return conn


def insistent_bucket_delete(bucket_name, *args, **kwargs):
    conn = boto.s3.connection.S3Connection(*args, **kwargs)

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
    

def insistent_bucket_create(conn, bucket_name, *args, **kwargs):
    while True:
        try:
            bucket = conn.create_bucket(bucket_name, *args, **kwargs)
        except boto.exception.S3CreateError, e:
            if e.status == 409:
                # Conflict; bucket already created -- probably means
                # the prior delete did not process just yet.
                continue

            raise

        break


class FreshBucket(object):
    def __init__(self, bucket_name, *args, **kwargs):
        self.bucket_name = bucket_name
        self.conn_args = args
        self.conn_kwargs = kwargs

    def __enter__(self):
        # Clean up a dangling bucket from a previous test run, if
        # necessary.
        self.conn = apathetic_bucket_delete(self.bucket_name,
                                            self.conn_args,
                                            self.conn_kwargs)

        return self

    def create(*args, **kwargs):
        insistent_bucket_create(self._conn, self.bucket_name, *args, **kwargs)

    def 

        
@contextlib.contextmanager
def FreshBucket(bucket_name, *args, **kwargs):
    if 'location' in kwargs:
        kwargs_sans_location = kwargs.copy()
        del kwargs_sans_location['location']
    else:
        
        
    # Clean up a dangling bucket from a previous test run, if
    # necessary.
    apathetic_bucket_delete(bucket_name, *args, **kwargs)

    # Create the desired bucket.
    insistent_bucket_create(bucket_name, *args, **kwargs)

    # Begin test execution.
    yield bucket

    # Delete the bucket again to clean up.
    insistent_bucket_delete(bucket_name, *args, **kwargs)
