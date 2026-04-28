"""
Whitelist of public API names that should appear in docs/api/ but are not in
hetu.__init__.__all__. The keys of EXTRAS are dotted import paths; the values
are the topic-group page they belong on (matching docs/api/<group>.md).
"""

EXTRAS: dict[str, str] = {
    "hetu.data.backend.base.RaceCondition": "exceptions",
    "hetu.data.backend.base.UniqueViolation": "exceptions",
    "hetu.data.backend.repo.SessionRepository": "system",
    "hetu.system.definer.SystemClusters": "system",
    "hetu.system.future.create_future_call": "system",
}

# Topic mapping for items already in hetu.__all__. If a name from __all__ is
# not listed here, the script logs a warning and skips it.
TOPIC_MAP: dict[str, str] = {
    # decorators
    "define_component": "decorators",
    "define_system": "decorators",
    "define_endpoint": "decorators",
    "property_field": "decorators",
    # components
    "BaseComponent": "components",
    "Permission": "components",
    # system
    "SystemContext": "system",
    # endpoint
    "EndpointContext": "endpoint",
    "ResponseToClient": "endpoint",
    "elevate": "endpoint",
}

# Names from __all__ that the script intentionally skips (modules, not symbols).
SKIP: set[str] = {"data", "system", "common", "endpoint"}
