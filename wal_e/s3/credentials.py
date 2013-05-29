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


class IncompleteCredentials(exception.UserException):
    """Exception raised when crendentials are verified.

    Rraised when either public/private portions of the credential are
    not available.
    """
    pass


class Credentials(object):
    """Represents a public/private key pair

    e.g. AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.  Also includes
    the source of these credentials.
    """
    def __init__(self, public, private,
                 public_source, private_source):
        assert issubclass(public_source, Source)
        assert issubclass(private_source, Source)

        self.public = public
        self.private = private
        self.public_source = public_source
        self.private_source = private_source

    @property
    def is_complete(self):
        """Return true if these credentials are not underspecified

        e.g. there is a public and secret key.  This is to say nothing
        as to whether that key combination is going to be accepted or
        not by AWS.
        """
        return ((self.public is not None
                 # instance-profile must be expanded to a real
                 # credential for a credential to be considered
                 # complete.
                 and self.public.lower() != INSTANCE_PROFILE_USER_INPUT) and
                self.private is not None)

    def raise_on_incomplete(self):
        """Given an incomplete credential, raise a UserException

        This UserException should include some text to help the user
        figure out why the credentials are underspecified.
        """
        if self.is_complete:
            return None

        if self.public == INSTANCE_PROFILE_USER_INPUT:
            raise exception.UserCritical(
                msg='bug: unexpanded credentials passed to maybe_complain',
                hint='report a bug to the wal-e project')

        # Go about setting up the detail message.
        def humanize_source(source):
            """Provide human readable name of 'source'"""
            if source is Environ:
                return 'enviroment variable'
            elif source is Argv:
                return 'command line'
            elif source is InstanceProfileEnv:
                return 'instance profile, specified via environment variable'
            elif source is InstanceProfileArgv:
                return 'instance profile, specified via command line'
            else:
                raise exception.UserCritical(
                    msg='bug: unexpected source of credential configuration',
                    detail='source is: ' + repr(source),
                    hint='report a bug to the wal-e project')

        def emit_status(what, value, source):
            """Provide status of passed credential component"""
            if value is None:
                return '{what} is not set'.format(what=what)
            else:
                # It is a deliberate design decision to not emit the
                # contents of the public *or* private keys, because
                # getting them swapped is all too easy, and that would
                # compromise one's secrets in logs in that case.
                return '{what} set by {source}'.format(
                    what=what, source=humanize_source(source))

        detail_lines = [
            'The credentials passed to wal-e have this configuration:']
        detail_lines.append(emit_status('AWS_ACCESS_KEY_ID',
                                        self.public, self.public_source))
        detail_lines.append(emit_status('AWS_SECRET_ACCESS_KEY',
                                        self.private, self.private_source))
        detail = '\n'.join(detail_lines)

        # Finally, raise the formatted error.
        raise IncompleteCredentials(
            msg='wal-e cannot start: incomplete credentials passed',
            detail=detail,
            hint=('check your environment and command line options '
                  'carefully to make sure keys are being passed properly'))


def from_environment():
    return Credentials(public=os.getenv('AWS_ACCESS_KEY_ID'),
                       public_source=Environ,
                       private=os.getenv('AWS_SECRET_ACCESS_KEY'),
                       private_source=Environ)


def from_argv(public):
    return Credentials(public=public,
                       public_source=Argv,
                       private=None,
                       private_source=Argv)


def from_instance_profile(public_source):
    if public_source is Environ:
        source = InstanceProfileEnv
    elif public_source is Argv:
        source = InstanceProfileArgv
    else:
        assert False

    md = utils.get_instance_metadata()

    public = md.get('AccessKeyId')
    private = md.get('SecretAccessKey')

    cred = Credentials(public=public,
                       private=private,
                       public_source=source,
                       private_source=source)

    return cred


def mask(lhs, rhs):
    """Overlay rhs onto lhs credentials

    If the rhs has an undefined credential part (i.e. public/private =
    'None'), then fall back to the lhs's value.
    """
    private = rhs.private
    private_source = rhs.private_source

    if private is None:
        private = lhs.private
        private_source = lhs.private_source

    public = rhs.public
    public_source = rhs.public_source

    if public is None:
        public = lhs.public
        public_source = lhs.public_source

    return Credentials(public=public, private=private,
                       public_source=public_source,
                       private_source=private_source)


def search_credentials(public_from_args):
    env_cred = from_environment()
    argv_cred = from_argv(public_from_args)
    env_argv = mask(env_cred, argv_cred)

    if env_argv.public == INSTANCE_PROFILE_USER_INPUT:
        resolved = from_instance_profile(env_argv.public_source)
        return mask(env_argv, resolved)
    else:
        return env_argv
