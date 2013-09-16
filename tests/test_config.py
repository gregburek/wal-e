import pytest

from wal_e import config

@pytest.fixture(autouse=True)
def never_use_aws_env_vars(monkeypatch):
    """Protect tests from acquiring config from the environment."""

    env_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY',
                'AWS_SECURITY_TOKEN']
    for name in env_vars:
        monkeypatch.delenv(name, raising=False)


def test_empty(monkeypatch):
    """Tests creds not specified at all."""
    cred = config.from_environment()

    assert cred.key.value is None
    assert cred.key.providence is config.Environ
    assert cred.secret.value is None
    assert cred.secret.providence is config.Environ
    assert cred.token.value is None
    assert cred.token.providence is config.Environ


def test_env_no_private(monkeypatch):
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'PUBLIC-BOGUS')

    cred = config.from_environment()

    assert cred.key.providence is config.Environ
    assert cred.key.value == 'PUBLIC-BOGUS'
    assert cred.secret.value is None
    assert cred.token.value is None


def test_env_no_public(monkeypatch):
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'PRIVATE-BOGUS')

    cred = config.from_environment()

    assert cred.key.value is None
    assert cred.secret.value == 'PRIVATE-BOGUS'
    assert cred.secret.providence is config.Environ
    assert cred.token.value is None


def test_argv_public_only():
    """Tests argv key public input with missing private creds."""
    cred = config.from_argv('PUBLIC-BOGUS', None)

    assert cred.key.value == 'PUBLIC-BOGUS'
    assert cred.key.providence is config.Argv
    assert cred.secret.value is None


def test_argv_instance_profile():
    cred = config.from_argv('instance-profile', None)

    assert cred.key.value == 'instance-profile'
    assert cred.key.providence is config.Argv
    assert cred.secret.value is None
    assert cred.token.value is None


def test_search_simple_no_argv():
    """Tests config search that doesn't rely on argv."""
    cred = config.search(None, None)

    print cred.key.providence
    print cred.secret.providence

    assert cred.key.providence is config.Environ
    assert cred.secret.providence is config.Environ


def test_search_nothing_specified():
    cred = config.search(None, None)

    assert cred.key.value is None
    assert cred.key.providence is config.Environ
    assert cred.secret.value is None
    assert cred.secret.providence is config.Environ
    assert cred.token.value is None
    assert cred.token.providence is config.Environ


def test_search_full_env(monkeypatch):
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'PUBLIC-BOGUS')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'PRIVATE-BOGUS')

    cred = config.search(None, None)

    assert cred.key.value == 'PUBLIC-BOGUS'
    assert cred.key.providence is config.Environ
    assert cred.secret.value == 'PRIVATE-BOGUS'
    assert cred.secret.providence is config.Environ
    assert cred.token.value is None
    assert cred.token.providence is config.Environ


def test_search_env_argv_mix(monkeypatch):
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'SECRET-BOGUS')
    monkeypatch.setenv('AWS_SECURITY_TOKEN', 'TOKEN-BOGUS')

    cred = config.search('KEY-BOGUS', None)

    assert cred.key.value == 'KEY-BOGUS'
    assert cred.key.providence is config.Argv
    assert cred.secret.value == 'SECRET-BOGUS'
    assert cred.secret.providence is config.Environ
    assert cred.token.value == 'TOKEN-BOGUS'
    assert cred.token.providence is config.Environ


def test_kv_repr():
    expected = "KV('hello', 'world', <class 'wal_e.s3.config.Argv'>)"

    assert repr(config.KV('hello', 'world', config.Argv)) == expected


def test_config_stringifcation():
    cred = config.Config(
        config.KV('1', 'a', config.Argv),
        config.KV('2', 'b', config.Environ),
        config.KV('3', 'c', config.Argv))

    expected = ("Config(KV('1', 'a', "
                "<class 'wal_e.s3.config.Argv'>), "
                "KV('2', 'b', <class 'wal_e.s3.config.Environ'>), "
                "KV('3', 'c', <class 'wal_e.s3.config.Argv'>))")

    assert repr(cred) == expected

    expected = ("Config(KV('1', 'a', "
                "<class 'wal_e.s3.config.Argv'>), '[redacted]', "
                "KV('3', 'c', <class 'wal_e.s3.config.Argv'>))")

    assert str(cred) == expected
