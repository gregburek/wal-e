import pytest

from wal_e import exception
from wal_e import s3
from wal_e.s3 import credentials


@pytest.fixture(autouse=True)
def never_use_aws_env_vars(monkeypatch):
    """Protect tests from acquiring credentials from the environment."""

    for name in ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']:
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


def test_env_complete(monkeypatch):
    """Tests creds fully specified via environment variable."""
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'PUBLIC-BOGUS')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'PRIVATE-BOGUS')

    cred = credentials.from_environment()
    assert cred.public_source is credentials.Environ
    assert cred.private_source is credentials.Environ
    assert cred.is_complete
    cred.raise_on_incomplete()


def test_env_empty(monkeypatch):
    """Tests creds not specified at all."""
    cred = credentials.from_environment()
    assert not cred.is_complete
    assert cred.public_source is credentials.Environ
    assert cred.private_source is credentials.Environ

    with pytest.raises(credentials.IncompleteCredentials):
        cred.raise_on_incomplete()


def test_env_no_private(monkeypatch):
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'PUBLIC-BOGUS')

    cred = credentials.from_environment()

    assert cred.public_source is credentials.Environ
    assert cred.private_source is credentials.Environ

    assert cred.private is None
    assert cred.public == 'PUBLIC-BOGUS'

    assert not cred.is_complete

    with pytest.raises(credentials.IncompleteCredentials):
        cred.raise_on_incomplete()


def test_env_no_public(monkeypatch):
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'PRIVATE-BOGUS')

    cred = credentials.from_environment()

    assert cred.public_source is credentials.Environ
    assert cred.private_source is credentials.Environ

    assert cred.private == 'PRIVATE-BOGUS'
    assert cred.public is None

    assert not cred.is_complete

    with pytest.raises(credentials.IncompleteCredentials):
        cred.raise_on_incomplete()


def test_env_instance_profile(monkeypatch):
    """Tests expected unexpanded instance-profile crash."""
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'instance-profile')

    cred = credentials.from_environment()

    assert not cred.is_complete

    with pytest.raises(exception.UserCritical):
        cred.raise_on_incomplete()


def test_argv_public_only():
    """Tests argv key public input with missing private creds."""
    cred = credentials.from_argv(public='PUBLIC-BOGUS')

    assert cred.public_source is credentials.Argv
    assert cred.private_source is credentials.Argv

    assert cred.private is None
    assert cred.public == 'PUBLIC-BOGUS'

    assert not cred.is_complete

    with pytest.raises(credentials.IncompleteCredentials):
        cred.raise_on_incomplete()


def test_argv_instance_profile():
    """Tests expected unexpanded instance-profile crash."""
    cred = credentials.from_argv(public='instance-profile')

    assert not cred.is_complete

    with pytest.raises(exception.UserCritical):
        cred.raise_on_incomplete()


def test_search_simple_no_argv():
    """Tests credential search that doesn't rely on argv."""
    cred = credentials.search_credentials(None)

    assert not cred.is_complete
    assert cred.public_source is credentials.Environ
    assert cred.private_source is credentials.Environ

    with pytest.raises(credentials.IncompleteCredentials):
        cred.raise_on_incomplete()


def test_search_nothing_specified():
    cred = s3.search_credentials(None)

    assert not cred.is_complete

    with pytest.raises(credentials.IncompleteCredentials):
        cred.raise_on_incomplete()


def test_search_full_env(monkeypatch):
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'PUBLIC-BOGUS')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'PRIVATE-BOGUS')

    cred = s3.search_credentials(None)

    assert cred.is_complete
    cred.raise_on_incomplete()


def test_search_okay_blend(monkeypatch):
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'PRIVATE-BOGUS')

    cred = s3.search_credentials('PUBLIC-BOGUS')

    assert cred.is_complete
    cred.raise_on_incomplete()


def test_search_busted(monkeypatch):
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'PUBLIC-BOGUS')

    cred = s3.search_credentials('PUBLIC-BOGUS')

    assert not cred.is_complete

    with pytest.raises(credentials.IncompleteCredentials):
        cred.raise_on_incomplete()


def test_search_env_triggered_expansion(monkeypatch, instance_metadata_iam):
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'instance-profile')

    cred = s3.search_credentials(None)

    print cred.public
    print cred.private
    assert cred.is_complete
    cred.raise_on_incomplete()
