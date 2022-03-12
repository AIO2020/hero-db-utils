"""
ED Data imports exception types
"""

class FieldsValidationError(Exception):
    """
    Exception type for validation errors related to
    DataModel instances.
    """
    pass

class UnexpectedQueryResult(Exception):
    """
    Exception for unexpected database results.
    """
    pass

class NoResultsError(UnexpectedQueryResult):
    """
    Exception when results from a query were expected
    but none were found.
    """
