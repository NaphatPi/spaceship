"""Module containing exceptions"""


class DatasetAlreadyExists(Exception):
    """Dataset or directory with the same name already exists"""


class DatasetNotFound(Exception):
    """Dataset not found"""


class DatasetNameNotAllowed(Exception):
    """Table name given is invalid. Only alphanumeric with - or _ are allowed"""


class AmbiguousSourceTable(Exception):
    """Source table pattern in the SQL statement is ambiguous"""


class RestorationError(Exception):
    """Failed to restore deltatable to a specific version"""
