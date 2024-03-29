# Generated by Django 4.1.7 on 2024-01-15 18:35

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('metamask', '0001_initial'),
        ('api2', '0004_offer_monthly_price_glm'),
    ]

    operations = [
        migrations.CreateModel(
            name='HealtcheckTask',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('status', models.TextField()),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('provider', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='api2.node')),
                ('user', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='metamask.userprofile')),
            ],
        ),
    ]
