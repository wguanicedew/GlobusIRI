"""Exception types, each mapped to a CLI exit code (see docs/design.md, errors section)."""

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_CONFIG = 2
EXIT_LOGIN_REQUIRED = 3
EXIT_PANDA = 4
EXIT_REMOTE = 5
EXIT_JOB_FAILED = 6
EXIT_VALIDATION = 7


class GlobusIriError(Exception):
    exit_code = EXIT_ERROR

    def __init__(self, message, *, fix=None):
        super().__init__(message)
        self.fix = fix

    def __str__(self):
        msg = super().__str__()
        return f"{msg}; run: {self.fix}" if self.fix else msg


class ConfigError(GlobusIriError):
    exit_code = EXIT_CONFIG


class LoginRequired(GlobusIriError):
    """No usable refresh/access token, or a consent is missing."""

    exit_code = EXIT_LOGIN_REQUIRED


class PandaError(GlobusIriError):
    exit_code = EXIT_PANDA


class RemoteError(GlobusIriError):
    """IRI or Globus API returned an error after retries."""

    exit_code = EXIT_REMOTE

    def __init__(self, message, *, status=None, fix=None):
        super().__init__(message, fix=fix)
        self.status = status


class JobFailed(GlobusIriError):
    exit_code = EXIT_JOB_FAILED


class ValidationFailed(GlobusIriError):
    exit_code = EXIT_VALIDATION


class DatabaseError(GlobusIriError):
    """The run index database (SQLite or MySQL/MariaDB) failed or cannot be reached."""

    exit_code = EXIT_ERROR
