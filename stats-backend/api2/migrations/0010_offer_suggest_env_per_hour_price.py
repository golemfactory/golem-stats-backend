# Generated by Django 4.1.7 on 2024-01-16 21:38

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api2', '0009_offer_is_overpriced_offer_overpriced_compared_to'),
    ]

    operations = [
        migrations.AddField(
            model_name='offer',
            name='suggest_env_per_hour_price',
            field=models.FloatField(null=True),
        ),
    ]
