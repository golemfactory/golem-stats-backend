from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("api2", "0041_offer_last_seen_at"),
    ]

    operations = [
        migrations.CreateModel(
            name="OfferVariantSighting",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("provider_node_id", models.CharField(db_index=True, max_length=42)),
                ("runtime", models.CharField(max_length=42)),
                ("content_hash", models.CharField(max_length=32)),
                ("first_seen_at", models.DateTimeField(db_index=True)),
                ("last_seen_at", models.DateTimeField(db_index=True)),
            ],
            options={
                "unique_together": {("provider_node_id", "runtime", "content_hash")},
            },
        ),
    ]
