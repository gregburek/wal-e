import os

from boto import utils

from wal_e import exception

INSTANCE_PROFILE_USER_INPUT = 'instance-profile'


class Source(object):
    # Marker type for sources of config
    pass


class Environ(Source):
    """Config from environment variable."""


class Argv(Source):
    """Config from argument vector."""


class KV(object):
    def __init__(self, name, value, providence):
        self.name = name
        self.value = value
        self.providence = providence

    def __repr__(self):
        return 'KV({0!r}, {1!r}, {2!r})'.format(self.name, self.value,
                                                self.providence)


class Config(object):
    def __init__(self, key, secret, token, prefix):
        self.key = key
        self.secret = secret
        self.token = token
        self.prefix = prefix

    def __str__(self):
        return 'Config({0!r}, {1!r}, {2!r}, {3!r})'.format(
            self.key, '[redacted]', self.token, self.prefix)

    def __repr__(self):
        return 'Config({0!r}, {1!r}, {2!r}, {3!r})'.format(
            self.key, self.secret, self.token, self.prefix)


def from_environment():
    def make(name):
        val = os.getenv(name)
        if val is None:
            return KV(name, None, Environ)
        else:
            return KV(name, val, Environ)

    key = make('AWS_ACCESS_KEY_ID')
    secret = make('AWS_SECRET_ACCESS_KEY')
    token = make('AWS_SECURITY_TOKEN')
    prefix = make('WALE_S3_PREFIX')

    return Config(key, secret,
                       token)


def from_argv(key, token, prefix):
    return Config(KV('AWS_ACCESS_KEY_ID', key, Argv),
                  KV('AWS_SECRET_ACCESS_KEY', None, Argv),
                  KV('AWS_SECURITY_TOKEN', token, Argv),
                  KV('WALE_S3_PREFIX', prefix, Argv))


def mask(lhs, rhs):
    """Overlay right-hand-side onto left-hand-side config

    If the rhs has an undefined config part (i.e. "rhs.value is None"), then
    fall back to the lhs's value.
    """

    def mask_one(attrname):
        lhs_atom = getattr(lhs, attrname)
        rhs_atom = getattr(rhs, attrname)

        if rhs_atom.value is None:
            return lhs_atom
        else:
            return rhs_atom

        assert False

    attrnames = ['key', 'secret', 'token', 'prefix']

    kwargs = {}
    for attrname in attrnames:
        kwargs[attrname] = mask_one(attrname)

    return Config(**kwargs)


def search_config(args_key, args_token, args_prefix):
    env_cfg = from_environment()
    argv_cfg = from_argv(args_key, args_token, args_prefix)
    env_and_argv = mask(env_cfg, argv_cfg)

    return env_and_argv
