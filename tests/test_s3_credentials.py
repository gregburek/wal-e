import pytest

from wal_e.s3 import credentials
from wal_e import exception


@pytest.fixture(autouse=True)
def never_use_aws_env_vars(monkeypatch):
    """Protect tests from acquiring credentials from the environment."""

    for name in ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']:
        monkeypatch.delenv(name, raising=False)


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


def test_argv_complete():
    """Tests creds fully specified via argv."""
    cred = credentials.from_argv(public='PUBLIC-BOGUS',
                                 private='PRIVATE-BOGUS')
    assert cred.public_source is credentials.Argv
    assert cred.private_source is credentials.Argv
    assert cred.is_complete
    assert cred.public == 'PUBLIC-BOGUS'
    assert cred.private == 'PRIVATE-BOGUS'
    cred.raise_on_incomplete()


def test_argv_empty():
    """Tests creds not specified at all."""
    cred = credentials.from_argv()
    assert not cred.is_complete
    assert cred.public_source is credentials.Argv
    assert cred.private_source is credentials.Argv

    with pytest.raises(credentials.IncompleteCredentials):
        cred.raise_on_incomplete()


def test_argv_no_private():
    """Tests missing private creds."""
    cred = credentials.from_argv(public='PUBLIC-BOGUS')

    assert cred.public_source is credentials.Argv
    assert cred.private_source is credentials.Argv

    assert cred.private is None
    assert cred.public == 'PUBLIC-BOGUS'

    assert not cred.is_complete

    with pytest.raises(credentials.IncompleteCredentials):
        cred.raise_on_incomplete()


def test_argv_no_public():
    """Tests missing public creds."""
    cred = credentials.from_argv(private='PRIVATE-BOGUS')

    assert cred.public_source is credentials.Argv
    assert cred.private_source is credentials.Argv

    assert cred.private == 'PRIVATE-BOGUS'
    assert cred.public is None

    assert not cred.is_complete

    with pytest.raises(credentials.IncompleteCredentials):
        cred.raise_on_incomplete()


def test_argv_instance_profile():
    """Tests expected unexpanded instance-profile crash."""
    cred = credentials.from_argv(public='instance-profile')

    assert not cred.is_complete

    with pytest.raises(exception.UserCritical):
        cred.raise_on_incomplete()
