# Generated by Django 4.1.7 on 2024-10-03 13:45

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api2', '0036_nodestatushistory_remove_duplicate'),
    ]

    operations = [
        migrations.AlterField(
            model_name='node',
            name='version',
            field=models.CharField(blank=True, db_index=True, max_length=7, null=True),
        ),
    ]