"""
Custom exception hierarchy for the photo organizer application.

This module defines specific exception types to improve error handling
and debugging throughout the application.
"""


class PhotoOrganizerError(Exception):
    """Base exception for all photo organizer errors."""
    pass


class FileHashError(PhotoOrganizerError):
    """Raised when file hashing fails."""
    pass


class MetadataExtractionError(PhotoOrganizerError):
    """Raised when metadata cannot be extracted from a file."""
    pass


class DatabaseError(PhotoOrganizerError):
    """Raised when database operations fail."""
    pass


class FileOperationError(PhotoOrganizerError):
    """Raised when file copy/move operations fail."""
    pass


class LinkingError(PhotoOrganizerError):
    """Raised when file relationship linking fails."""
    pass
