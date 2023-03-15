
#!/usr/bin/python

import sys
import yaml
import os

print()
print("MINTS")
print()

yamlFile =  str(sys.argv[1])
print("YAML File: " + yamlFile)
print()

with open(yamlFile) as file:
    # The FullLoader parameter handles the conversion from YAML
    # scalar values to Python the dictionary format
    mintsDefinitions = yaml.load(file, Loader=yaml.FullLoader)

airMarID = mintsDefinitions['airmarID']

dataFolder = mintsDefinitions['dataFolder']+ "/ref/"

sysStr = 'rsync -avzrtu -e "ssh" ' +  "--include='*.csv' --include='*/' --exclude='*' /home/mints/Downloads/reference/" + airMarID + " " + dataFolder
print(sysStr)
os.system(sysStr)
