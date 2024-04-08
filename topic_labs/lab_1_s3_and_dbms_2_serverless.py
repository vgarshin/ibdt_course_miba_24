import os
import sys
import json
import boto3
import names
import datetime
import clickhouse_connect
import pandas as pd

DATA_BUCKET = os.environ['PRJ_BUCKET_NAME']
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

def access_data(file_path):
    with open(file_path) as file:
        access_data = json.load(file)
    return access_data

def row_info(rin):
    """
    Extracts names of:
      - docker image
      - id of the Jupyter application
      - name of the host, where Jupyter runs
    
    """
    img = rin[rin.find('container_image='):].split('\'')[1]
    hub = rin[rin.find('pod_name='):].split('\'')[1]
    host = rin[rin.find('host='):].split('\'')[1]
    return img, hub, host

def sq_brackets(sin):
    """
    Split log string amd extracts:
      - timestamp of the event
      - name of application
      - type of logs
      - code of event
      - description
    
    """
    try:
        s = sin.split('[', 1)[1].split(']')[0]
        msg = sin[len(s) + 2 :].strip()
        s = s.split()
        head = s[0]
        ts = ' '.join(s[1:3])
        svc = s[3]
        typ = s[4].split(':')[0]
        code = s[4].split(':')[1]
    except:
        head, ts, svc, typ, code = '', '', '', '', ''
        msg = sin
    return head, ts, svc, typ, code, msg

def parce_users_activities(row):
    """
    Ugly function.
    
    You may use dictionary to make it
    more pythonic or something else.
    
    """
    code = row['event_code']
    msg = row['message']
    if code in ['43', '44']:
        user = msg.split()[-1]
        log = 'logged out'
    elif code in ['61', '85', '111']:
        user = msg.split()[3]
        log = 'spawning sever with advanced configuration option'
    elif code == '148':
        user = msg.split()[-1]
        log = 'user is running'
    elif code == '167':
        user = msg.split()[1]
        log = 'server is already active'
    if code == '238':
        user = msg.split()[-1]
        log = 'adding role for user'
    elif code in ['257', '333']:
        user = msg.split()[2]
        log = 'adding user to proxy'
    elif code in ['281', '359']:
        user = msg.split()[2]
        log = 'removing user from proxy'
    elif code == '361':
        user = msg.split()[1]
        log = 'user requested new auth token'
    elif code == '380':
        user = msg.split()[3]
        log = 'previous spawn failed'
    elif code in ['402', '394']:
        user = msg.split()[0]
        log = 'pending spawn'
    elif code == '567':
        user = msg.split('/')[4]
        log = 'stream closed while handling '
    elif code == '626':
        user = msg.split()[1]
        log = 'server is already started'
    elif code == '651':
        user = msg.split('-')[2]
        log = 'creating oauth client for user'
    elif code in ['664', '749']:
        user = msg.split()[1]
        log = 'server is ready'
    elif code == '681':
        user = msg.split()[0].replace('\'s', '')
        log = 'server failed to start'
    elif code == '689':
        user = msg.split()[3].replace('\'s', '')
        log = 'unhandled error starting with timeout'
    elif code == '738':
        user = msg.split()[0].replace(',', '').replace('\'s', '')
        log = 'server never showed up and giving up'
    elif code in ['757', '810']:
        user = msg.split()[-1]
        log = 'logged in'
    elif code in ['904', '963']:
        user = msg.split()[1]
        log = 'server took time to start'
    elif code in ['1067', '2022']:
        user = msg.split()[1]
        log = 'user server stopped with exit code 1'
    elif code in ['1110', '1170']:
        user = msg.split()[1]
        log = 'server took time to stop'
    elif code in ['1143', '1203']:
        user = msg.split()[1].replace(':', '')
        log = 'server is slow to stop'
    elif code in ['1344', '1409']:
        user = msg.split('/')[3]
        log = 'failing suspected api request to not-running server'
    elif code in ['1143', '1203']:
        user = msg.split()[1].replace('/', '')
        log = 'server is slow to stop'
    elif code == '1415':
        user = msg.split()[-1]
        log = 'admin requesting spawn on behalf'
    elif code == '1437':
        user = msg.split()[1]
        log = 'user requested server which user do not own'
    elif code == '1840':
        user = msg.split()[4].replace('jupyter-', '').replace(',', '')
        log = 'attempting to create pod with timeout'
    elif code == '1857':
        user = msg.split()[3].replace('jupyter-', '').replace(',', '')
        log = 'found existing pod and attempting to kill'
    elif code == '1861':
        user = msg.split()[2].replace('jupyter-', '').replace(',', '')
        log = 'killed pod and will try starting singleuser pod again'
    elif code in ['1875', '2469', '2509']:
        user = msg.split()[4].replace('claim-', '').replace('jupyter-', '').replace(',', '')
        log = 'attempt to create pvc with timeout'
    elif code in ['1887', '2525']:
        user = msg.split()[1].replace('claim-', '')
        log = 'pvc already exists'
    elif code in ['1961', '2044']:
        user = msg.split()[1].replace('jupyter-', '')
        log = 'restarting pod reflector'
    elif code in ['1997', '2780']:
        user = msg.split('-')[-1]
        log = 'deleting pod'
    elif code == '2069':
        user = msg.split()[0].replace(',', '')
        log = 'user does not appear to be running and shutting it down'  
    elif code in ['2077', '2504']:
        user = msg.split()[0]
        log = 'still running'
    elif code == '2085':
        user = msg.split()[0]
        log = 'server appears to have stopped while the hub was down'
    elif code == '2170':
        user = msg.split('-')[-1]
        log = 'deleting oauth client'
    else:
        user, log = '', ''
    return user, log

def handler(event, context):

    # get object storage data

    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        endpoint_url='https://storage.yandexcloud.net'
    )
    s3.download_file(DATA_BUCKET, 'YandexInternalRootCA.crt', '/tmp/YandexInternalRootCA.crt')
    s3.download_file(DATA_BUCKET, 'access_ch.json', '/tmp/access_ch.json')
    access_ch = access_data('/tmp/access_ch.json')
    get_object_response = s3.get_object(
        Bucket=DATA_BUCKET, 
        Key='jhub_logs_prj.csv'
    )

    # preprocessing

    df = pd.read_csv(get_object_response['Body'], sep=';')
    df['img'], df['hub'], df['host'] = zip(*df['kubernetes'].map(row_info))
    df['head'], df['timestamp'], df['service'], df['event_type'], df['event_code'], df['message'] = zip(*df['log'].map(sq_brackets))
    df['user'], df['log'] = zip(*df.apply(parce_users_activities, axis=1))

    # users

    df = df.loc[df.user != '', [
        'timestamp',
        'hub',
        'img',
        'host',
        'event_code',
        'event_type',
        'log',
        'user'
    ]].reset_index(drop=True)
    logins = df.user.unique()
    users = []
    for login in logins:
        user = {}
        user['login'] = login
        user['name'] = names.get_full_name()
        user['email'] = login + '@gsom.spbu.ru'
        users.append(user)
    df_users = pd.DataFrame(users)

    client = clickhouse_connect.get_client(
        host=access_ch['host'], 
        username=access_ch['user'], 
        password=access_ch['password'],
        port=access_ch['port'],
        verify='/tmp/YandexInternalRootCA.crt'
    )
    result = client.query('DROP TABLE IF EXISTS db1.users')
    query = '''
    CREATE TABLE IF NOT EXISTS db1.users
    (
        email String,
        login String,
        name String
    ) ENGINE = MergeTree
    ORDER BY email;
    '''
    result = client.query(query)
    client.insert_df(
        'db1.users',
        df_users
    )

    # instances

    df_instances = df[[
        'hub',
        'img',
        'host'
    ]].reset_index(drop=True)
    df_instances.drop_duplicates(inplace=True)
    df_instances.reset_index(drop=True, inplace=True)
    result = client.query('DROP TABLE IF EXISTS db1.instances')
    query = '''
    CREATE TABLE IF NOT EXISTS db1.instances
    (
        hub String,
        img String,
        host String
    ) ENGINE = MergeTree
    ORDER BY hub;
    '''
    result = client.query(query)
    client.insert_df(
        'db1.instances',
        df_instances
    )

    # logs

    df_logs = df[[
        'timestamp',
        'hub',
        'event_code',
        'event_type',
        'log',
        'user'
    ]].reset_index(drop=True)
    df_logs.rename({'user': 'login'}, axis='columns', inplace=True)
    result = client.query('DROP TABLE IF EXISTS db1.logs')
    query = '''
    CREATE TABLE IF NOT EXISTS db1.logs
    (
        timestamp String,
        hub String,
        event_code String,
        event_type String,
        log String,
        login String
    ) ENGINE = MergeTree
    ORDER BY hub;
    '''
    result = client.query(query)
    client.insert_df(
        'db1.logs',
        df_logs
    )
