import argparse
import json
import os
import subprocess

# Read the JSON config file into memory.

with open(r'dg_delete_config.json') as f:
    config = json.load(f)

# Validate command line arguments.

parser = argparse.ArgumentParser(description=config['argparse']['description'])
for arg in config['argparse']['args']:
    parser.add_argument(arg['name'], help=arg['help'])
args = parser.parse_args()

config['age_in_days'] = abs(int(args.age_in_days))

# Establish a bi-directional connection with the Informatica repository via sqlplus and select old DGs.

sqlplus = []
for arg in config['sqlplus']['popen']:
    sqlplus.append(arg.format(e=os.environ))

infa_repo = subprocess.Popen(sqlplus, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
sql = '\n'.join(config['sqlplus']['input']['set']) + '\n' + '\n'.join(config['sqlplus']['input']['listobjects']).format(c=config) + '\n'
result = infa_repo.communicate(input=sql)

# Build a Python list of DGs by parsing out the result string.

dgs = []
for line in result[0].split('\n'):
    # One line per DG.
    dg = {}
    if len(line) > 0:
        # Split the name and date columns apart.
        dg['name'], dg['last_saved_date'] = line.split('|')
        dgs.append(dg)

if len(dgs) == 0:
    # No DGs to delete.
    print config['prompts']['no_dgs'].format(config['age_in_days'])
else:
    # Current working directory has to be pmrep's location in order to work.
    os.chdir(config['pmrep']['path'].format(e=os.environ))

    # Loop through the DGs and delete them one at a time.
    for dg in dgs:
        print config['prompts']['deleting'].format(dg=dg)
        pmrep = []
        for arg in config['pmrep']['popen']:
            pmrep.append(arg.format(dg=dg))
        infa_repo = subprocess.Popen(pmrep, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        result = infa_repo.communicate()[0]
        print result

#EOF
