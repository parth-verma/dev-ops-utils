import boto3
import requests
import sys
from time import sleep
from botocore.exceptions import ClientError


def download_function(name, url):
    r = requests.get(url, stream=True)
    stream = b''
    for chunk in r.iter_content(chunk_size=1024):
        if chunk:
            stream += chunk
    return stream
print("Enter the profile_name for the table where you want to copy from.")
from_profile_name = input("Profile Name [default]: ").strip()

print()

print("Enter the profile name for the table where you want to copy to.")
to_profile_name = input("Profile Name [default]: ").strip()
print()
source = boto3.Session(profile_name='default' if from_profile_name ==
                       '' else from_profile_name).client('lambda')

dest = boto3.Session(profile_name='default' if to_profile_name ==
                     '' else to_profile_name).client('lambda')
dest_iam = boto3.Session(profile_name='default' if to_profile_name ==
                         '' else to_profile_name).resource('iam').Role('APIGatewayLambdaExecRole')

functions = [i['FunctionName'] for i in source.list_functions()['Functions']]

for ind, ele in enumerate(functions):
    print("%s. %s" % (str(ind), str(ele)))

functions_num = list(map(int, input("Choose Functions: ").strip().split()))


for i in functions_num:
    function = functions[i]
    src_function_code = source.get_function(FunctionName=function)[
        'Code']['Location']
    code = download_function(None, src_function_code)
    src_function_config = source.get_function_configuration(
        FunctionName=function)
    src_function_config['Code'] = {'ZipFile': code}
    src_function_config['Role'] = dest_iam.arn
    src_function_config.pop('FunctionArn', None)
    src_function_config.pop('CodeSize', None)
    src_function_config.pop('LastModified', None)
    src_function_config.pop('CodeSha256', None)
    src_function_config.pop('Version', None)
    src_function_config.pop('VpcConfig', None)
    src_function_config.pop('DeadLetterConfig', None)
    src_function_config.pop('KMSKeyArn', None)
    src_function_config.pop('MasterArn', None)
    src_function_config.pop('ResponseMetadata', None)
    env_variables = {}
    print("Enter Environment Variables")
    for j in src_function_config.get('Environment', {}).get('Variables', {}):
        env_variables[j] = input(j + ': ').strip()
        src_function_config['Environment']['Variables'] = env_variables
    dest.create_function(**src_function_config)
