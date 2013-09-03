import os

from boto import utils

from wal_e import exception

INSTANCE_PROFILE_USER_INPUT = 'instance-profile'


class Source(object):
    # Marker type for sources of credentials
    pass


class Environ(Source):
    """Credential from environment variable."""


class Argv(Source):
    """Credential from argument vector."""


class InstanceProfileEnv(Source):
    """Instance profile expansion.

    Triggered by environment variable.
    """
    pass


class InstanceProfileArgv(Source):
    """Instance profile expansion.

    Triggered by argument vector.
    """
    pass


class KV(object):
    def __init__(self, name, value, providence):
        self.name = name
        self.value = value
        self.providence = providence

    def __repr__(self):
        return 'KV({0!r}, {1!r}, {2!r})'.format(self.name, self.value,
                                                self.providence)


class Credentials(object):
    """

    e.g. AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SECURITY_TOKEN.
    """
    def __init__(self,
                 key, secret, token):
        self.key = key
        self.secret = secret
        self.token = token

    def __str__(self):
        return 'Credentials({0!r}, {1!r}, {2!r})'.format(
            self.key, '[redacted]', self.token)

    def __repr__(self):
        return 'Credentials({0!r}, {1!r}, {2!r})'.format(
            self.key, self.secret, self.token)


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

    return Credentials(key, secret,
                       token)


def from_argv(key, token):
    return Credentials(KV('AWS_ACCESS_KEY_ID', key, Argv),
                       KV('AWS_SECRET_ACCESS_KEY', None, Argv),
                       KV('AWS_SECURITY_TOKEN', token, Argv))


def from_instance_profile(key):
    if key.providence is Environ:
        providence = InstanceProfileEnv
    elif key.providence is Argv:
        providence = InstanceProfileArgv
    else:
        assert False

    md = utils.get_instance_metadata()

    key = KV('AWS_ACCESS_KEY_ID', md.get('AccessKeyId'), providence)
    secret = KV('AWS_SECRET_ACCESS_KEY', md.get('SecretAccessKey'), providence)
    token = KV('AWS_SECURITY_TOKEN', md.get('Token'), providence)

    return Credentials(key, secret, token)


def mask(lhs, rhs):
    """Overlay rhs onto lhs credentials

    If the rhs has an undefined credential part (i.e. public/private =
    'None'), then fall back to the lhs's value.
    """

    def mask_one(attrname):
        lhs_atom = getattr(lhs, attrname)
        rhs_atom = getattr(rhs, attrname)

        if rhs_atom.value is None:
            return lhs_atom
        else:
            return rhs_atom

        assert False

    attrnames = ['key', 'secret', 'token']

    kwargs = {}
    for attrname in attrnames:
        kwargs[attrname] = mask_one(attrname)

    return Credentials(**kwargs)


def search_credentials(args_key, args_token):
    env_cred = from_environment()
    argv_cred = from_argv(args_key, args_token)
    env_and_argv = mask(env_cred, argv_cred)

    if env_and_argv.key.value == INSTANCE_PROFILE_USER_INPUT:
        resolved = from_instance_profile(env_and_argv.key)

        if (resolved.key.value is None or
            resolved.secret.value is None or
            resolved.secret.value is None):
            raise exception.UserException(
                msg='could not retrieve instance profile credentials')

        return mask(env_and_argv, resolved)
    else:
        return env_and_argv
