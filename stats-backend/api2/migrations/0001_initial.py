# Generated by Django 3.2.12 on 2022-04-14 09:37

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Node',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('node_id', models.CharField(max_length=42, unique=True)),
                ('wallet', models.CharField(blank=True, max_length=42, null=True)),
                ('earnings_total', models.FloatField(blank=True, null=True)),
                ('online', models.BooleanField(default=False)),
                ('computing_now', models.BooleanField(default=False)),
                ('version', models.CharField(max_length=7)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
            ],
        ),
        migrations.CreateModel(
            name='Offer',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('offer', models.JSONField(null=True)),
                ('runtime', models.CharField(max_length=42, unique=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('provider', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='api2.node')),
            ],
        ),
    ]
