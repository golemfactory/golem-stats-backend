# Generated by Django 3.2.2 on 2021-05-19 08:44

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('collector', '0015_networkaveragepricing_networkmedianpricing'),
    ]

    operations = [
        migrations.CreateModel(
            name='NetworkAveragePricingMax',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('start', models.FloatField()),
                ('cpuh', models.FloatField()),
                ('perh', models.FloatField()),
                ('date', models.DateTimeField()),
            ],
        ),
        migrations.CreateModel(
            name='NetworkMedianPricingMax',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('start', models.FloatField()),
                ('cpuh', models.FloatField()),
                ('perh', models.FloatField()),
                ('date', models.DateTimeField()),
            ],
        ),
        migrations.CreateModel(
            name='ProvidersComputingMax',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('total', models.IntegerField()),
                ('date', models.DateTimeField()),
            ],
        ),
        migrations.AddField(
            model_name='networkaveragepricing',
            name='date',
            field=models.DateTimeField(auto_now=True),
        ),
        migrations.AddField(
            model_name='networkmedianpricing',
            name='date',
            field=models.DateTimeField(auto_now=True),
        ),
        migrations.AddField(
            model_name='providerscomputing',
            name='date',
            field=models.DateTimeField(auto_now=True),
        ),
    ]
