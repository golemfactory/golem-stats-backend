# Generated by Django 4.1.7 on 2024-09-24 11:14

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api2', '0033_alter_nodestatushistory_is_online_and_more'),
    ]

    operations = [
        migrations.AddIndex(
            model_name='nodestatushistory',
            index=models.Index(fields=['node_id', 'timestamp'], name='api2_nodest_node_id_220bdf_idx'),
        ),
    ]
