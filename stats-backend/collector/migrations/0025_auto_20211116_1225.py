# Generated by Django 3.2.9 on 2021-11-16 11:25

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('collector', '0024_node_benchmarked_at'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='node',
            name='benchmark_score',
        ),
        migrations.RemoveField(
            model_name='node',
            name='benchmarked_at',
        ),
        migrations.CreateModel(
            name='Benchmarks',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('benchmark_score', models.IntegerField(default=0)),
                ('benchmarked_at', models.DateTimeField(blank=True, null=True)),
                ('provider', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='collector.node')),
            ],
        ),
    ]
