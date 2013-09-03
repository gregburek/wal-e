import pytest

from wal_e import s3
from wal_e.s3 import credentials


@pytest.fixture(autouse=True)
def never_use_aws_env_vars(monkeypatch):
    """Protect tests from acquiring credentials from the environment."""

    env_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY',
                'AWS_SECURITY_TOKEN']
    for name in env_vars:
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def instance_metadata_iam(monkeypatch):
    """Return generic instance metadata with IAM keys."""
    from boto import utils

    # Fixture for get_instance_metadata, copied from the boto test
    # suite.
    INSTANCE_CONFIG = {
        'allowall': {
            u'AccessKeyId': u'iam_access_key',
            u'Code': u'Success',
            u'Expiration': u'2012-09-01T03:57:34Z',
            u'LastUpdated': u'2012-08-31T21:43:40Z',
            u'SecretAccessKey': u'iam_secret_key',
            u'Token': u'iam_token',
            u'Type': u'AWS-HMAC'
        }
    }

    monkeypatch.setattr(utils, 'get_instance_metadata',
                        lambda: INSTANCE_CONFIG)


@pytest.fixture
def instance_metadata_no_iam(monkeypatch):
    """Return generic instance metadata without IAM keys."""
    from boto import utils

    INSTANCE_CONFIG = {
        'allowall': {
            u'Code': u'Success',
            u'Expiration': u'2012-09-01T03:57:34Z',
            u'LastUpdated': u'2012-08-31T21:43:40Z',
            u'Token': u'iam_token',
            u'Type': u'AWS-HMAC'
        }
    }

    monkeypatch.setattr(utils, 'get_instance_metadata',
                        lambda: INSTANCE_CONFIG)


def test_empty(monkeypatch):
    """Tests creds not specified at all."""
    cred = credentials.from_environment()

    assert cred.key.value is None
    assert cred.key.providence is credentials.Environ
    assert cred.secret.value is None
    assert cred.secret.providence is credentials.Environ
    assert cred.token.value is None
    assert cred.token.providence is credentials.Environ


def test_env_no_private(monkeypatch):
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'PUBLIC-BOGUS')

    cred = credentials.from_environment()

    assert cred.key.providence is credentials.Environ
    assert cred.key.value == 'PUBLIC-BOGUS'
    assert cred.secret.value is None
    assert cred.token.value is None


def test_env_no_public(monkeypatch):
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'PRIVATE-BOGUS')

    cred = credentials.from_environment()

    assert cred.key.value is None
    assert cred.secret.value == 'PRIVATE-BOGUS'
    assert cred.secret.providence is credentials.Environ
    assert cred.token.value is None


def test_argv_public_only():
    """Tests argv key public input with missing private creds."""
    cred = credentials.from_argv('PUBLIC-BOGUS', None)

    assert cred.key.value == 'PUBLIC-BOGUS'
    assert cred.key.providence is credentials.Argv
    assert cred.secret.value is None


def test_argv_instance_profile():
    cred = credentials.from_argv('instance-profile', None)

    assert cred.key.value == 'instance-profile'
    assert cred.key.providence is credentials.Argv
    assert cred.secret.value is None
    assert cred.token.value is None


def test_search_simple_no_argv():
    """Tests credential search that doesn't rely on argv."""
    cred = credentials.search_credentials(None, None)

    print cred.key.providence
    print cred.secret.providence

    assert cred.key.providence is credentials.Environ
    assert cred.secret.providence is credentials.Environ


def test_search_nothing_specified():
    cred = s3.search_credentials(None, None)

    assert cred.key.value is None
    assert cred.key.providence is credentials.Environ
    assert cred.secret.value is None
    assert cred.secret.providence is credentials.Environ
    assert cred.token.value is None
    assert cred.token.providence is credentials.Environ


def test_search_full_env(monkeypatch):
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'PUBLIC-BOGUS')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'PRIVATE-BOGUS')

    cred = s3.search_credentials(None, None)

    assert cred.key.value == 'PUBLIC-BOGUS'
    assert cred.key.providence is credentials.Environ
    assert cred.secret.value == 'PRIVATE-BOGUS'
    assert cred.secret.providence is credentials.Environ
    assert cred.token.value is None
    assert cred.token.providence is credentials.Environ


def test_search_env_argv_mix(monkeypatch):
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'SECRET-BOGUS')
    monkeypatch.setenv('AWS_SECURITY_TOKEN', 'TOKEN-BOGUS')

    cred = s3.search_credentials('KEY-BOGUS', None)

    assert cred.key.value == 'KEY-BOGUS'
    assert cred.key.providence is credentials.Argv
    assert cred.secret.value == 'SECRET-BOGUS'
    assert cred.secret.providence is credentials.Environ
    assert cred.token.value == 'TOKEN-BOGUS'
    assert cred.token.providence is credentials.Environ


def test_kv_repr():
    expected = "KV('hello', 'world', <class 'wal_e.s3.credentials.Argv'>)"

    assert repr(credentials.KV('hello', 'world', credentials.Argv)) == expected


def test_credentials_stringifcation():
    cred = credentials.Credentials(
        credentials.KV('1', 'a', credentials.Argv),
        credentials.KV('2', 'b', credentials.Environ),
        credentials.KV('3', 'c', credentials.Argv))

    expected = ("Credentials(KV('1', 'a', "
                "<class 'wal_e.s3.credentials.Argv'>), "
                "KV('2', 'b', <class 'wal_e.s3.credentials.Environ'>), "
                "KV('3', 'c', <class 'wal_e.s3.credentials.Argv'>))")

    assert repr(cred) == expected

    expected = ("Credentials(KV('1', 'a', "
                "<class 'wal_e.s3.credentials.Argv'>), '[redacted]', "
                "KV('3', 'c', <class 'wal_e.s3.credentials.Argv'>))")

    assert str(cred) == expected
