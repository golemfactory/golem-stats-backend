# Generated by Django 3.2 on 2021-04-23 15:39

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('collector', '0010_networkstatsmax'),
    ]

    operations = [
        migrations.AlterField(
            model_name='networkstatsmax',
            name='date',
            field=models.DateTimeField(),
        ),
    ]