import pytest

from wal_e.s3 import credentials


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
    assert not cred.is_complete
    with pytest.raises(credentials.IncompleteCredentials):
        cred.raise_on_incomplete()
