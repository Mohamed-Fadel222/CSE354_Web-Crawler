"""
Message tag constants for communication between components.
Import this file in all components to ensure consistent tag usage.
"""

# Communication Tags
MSG_TAG_URL_SUBMISSION = 0      # URL to be crawled
MSG_TAG_CONTENT_SUBMISSION = 1  # Content to be indexed
MSG_TAG_SEARCH_REQUEST = 2      # Search request
MSG_TAG_ERROR = 99              # Error/exception
MSG_TAG_HEARTBEAT = 999         # System status/heartbeat

# Tag descriptions for documentation
TAG_DESCRIPTIONS = {
    MSG_TAG_URL_SUBMISSION: "URL submission (from webapp to crawler)",
    MSG_TAG_CONTENT_SUBMISSION: "Content submission (from crawler to indexer)",
    MSG_TAG_SEARCH_REQUEST: "Search request",
    MSG_TAG_ERROR: "Error/exception",
    MSG_TAG_HEARTBEAT: "System status/heartbeat"
}

def get_tag_description(tag):
    """Get a human-readable description of a tag"""
    return TAG_DESCRIPTIONS.get(tag, "Unknown tag") 