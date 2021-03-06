# Generated by Django 3.1.7 on 2021-03-29 19:42

import django.contrib.postgres.fields.jsonb
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Node',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('node_id', models.CharField(max_length=42)),
                ('data', django.contrib.postgres.fields.jsonb.JSONField()),
                ('online', models.BooleanField(default=False)),
            ],
        ),
    ]
