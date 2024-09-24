from django.db import migrations, models

def update_nodestatushistory(apps, schema_editor):
    NodeStatusHistory = apps.get_model('api2', 'NodeStatusHistory')
    Node = apps.get_model('api2', 'Node')

    for history in NodeStatusHistory.objects.all():
        if history.provider:
            history.node_id = history.provider.node_id
            history.save()

class Migration(migrations.Migration):

    dependencies = [
        ('api2', '0030_relaynodes_ip_address_relaynodes_port'),
    ]

    operations = [
        migrations.AddField(
            model_name='nodestatushistory',
            name='node_id',
            field=models.CharField(max_length=42, null=True),
        ),
        migrations.RunPython(update_nodestatushistory),
        migrations.RemoveField(
            model_name='nodestatushistory',
            name='provider',
        ),
        migrations.AlterField(
            model_name='nodestatushistory',
            name='node_id',
            field=models.CharField(max_length=42),
        ),

    ]