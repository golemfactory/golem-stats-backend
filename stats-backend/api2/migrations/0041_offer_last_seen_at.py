from django.db import migrations, models


class Migration(migrations.Migration):
    """Add the freshness marker used to tell current offers from history.

    Deliberately backfilled as NULL: no existing row can claim a scan saw it,
    so every offer reports as unknown until the scraper observes it again
    (~60s for anything still on the market). The stored properties and prices
    are left untouched.
    """

    dependencies = [
        ("api2", "0040_transactionscraperindex_currently_indexing"),
    ]

    operations = [
        migrations.AddField(
            model_name="offer",
            name="last_seen_at",
            field=models.DateTimeField(blank=True, db_index=True, null=True),
        ),
    ]
