# Generated by Django 4.1.7 on 2024-03-28 15:01

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api2', '0027_remove_node_earnings_total_alter_node_computing_now_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='node',
            name='earnings_total',
            field=models.FloatField(blank=True, null=True),
        ),
    ]