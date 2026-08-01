"""
Test settings: skip the migration history (broken on sqlite) and build the
schema directly from the current models.

Usage: python manage.py test --settings=core.test_settings
"""

from .settings import *  # noqa: F401,F403


class DisableMigrations(dict):
    def __contains__(self, item):
        return True

    def __getitem__(self, item):
        return None


MIGRATION_MODULES = DisableMigrations()
