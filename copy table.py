import boto3
import sys
from time import sleep
from botocore.exceptions import ClientError

print("Enter the profile_name for the table where you want to copy from.")
from_profile_name = input("Profile Name [default]: ").strip()

print()

print("Enter the profile name for the table where you want to copy to.")
to_profile_name = input("Profile Name [default]: ").strip()
print()
source = boto3.Session(profile_name='default' if from_profile_name ==
                       '' else from_profile_name).client('dynamodb', use_ssl=False)

dest = boto3.Session(profile_name='default' if to_profile_name ==
                     '' else to_profile_name).client('dynamodb', use_ssl=False)

tables = source.list_tables(Limit=10 * 4)['TableNames']
tables.sort(key=lambda x: x.lower())
for id, x in enumerate(tables):
    print(str(id) + '. ' + x)
tables_num = list(map(int, input("Choose Table: ").strip().split()))

print("\nChoose Action:")

print("1. Create Table")
print("2. Copy Table")
print("3. Create and Copy Table")
action = int(input().strip())
if not (0 < action < 4):
    print("Invalid Action")
    print("Quiting")
    exit(1)

for i in tables_num:
    table = tables[i]
    # Check if table exists in the source
    try:
        src_schema = source.describe_table(TableName=table)
    except Exception as e:
        print("Table:%s doesn't exist." % table)
        continue
    src_schema = src_schema['Table']
    num_items = src_schema['ItemCount']

    try:
        dest.describe_table(TableName=table)
        if action & 1 == 1:
            print("Table %s already exists.\nSkipping." % table)
    except ClientError as e:
        if action & 1 == 1:
            print("Creating table %s in destination database." % table, end=' ')
            sys.stdout.flush()
            src_schema.pop('CreationDateTime')
            src_schema.pop('TableId')
            src_schema.pop('ItemCount')
            src_schema.pop('TableArn')
            src_schema.pop('TableSizeBytes')
            src_schema.pop('TableStatus')
            src_schema.pop('LatestStreamArn', None)
            src_schema.pop('LatestStreamLabel', None)
            src_schema['ProvisionedThroughput'].pop(
                'LastDecreaseDateTime', None)
            src_schema['ProvisionedThroughput'].pop(
                'LastIncreaseDateTime', None)
            src_schema['ProvisionedThroughput'].pop('NumberOfDecreasesToday')
            a = dest.create_table(**src_schema)['TableDescription']
            print("Creating Table", end='')
            sys.stdout.flush()
            while a['TableStatus'] == 'CREATING':
                sleep(0.5)
                print('.', end='')
                sys.stdout.flush()
                a = dest.describe_table(TableName=table)['Table']
            print("\nTable Created.")
            sys.stdout.flush()
        else:
            print("Table %s doesn't exist in the destination." % table)
            print("Skipping.")
            sys.stdout.flush()
            continue

    if action & 2 == 2:
        print("Copying table %s." % table)
        source_items = source.scan(TableName=table)
        print()
        last_eval_key = source_items.get('LastEvaluatedKey',False)
        source_items = source_items['Items']
        print("Total Items to copy:", str(num_items))
        l = 0
        while True:
            for ind, item in enumerate(source_items):
                sleep(2)
                dest.put_item(TableName=table, Item=item)
                sys.stdout.write("\033[K")
                print("(%s/%s) Write Progress: %s %%" %
                      (str(l + 1), str(num_items), str((l + 1) * 100 // num_items)), end='\r')
                l+=1
            if not last_eval_key:
                break
            source_items = source.scan(TableName=table,ExclusiveStartKey=last_eval_key)
            last_eval_key = source_items.get('LastEvaluatedKey',False)
            source_items = source_items['Items']
        print()
