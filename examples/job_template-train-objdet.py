# simple script to convert the MMDetection Python template file into a json structure
# to allow copy/pasting into the actual json template
# json doesn't support multi-line strings unfortunately

import json

with open("./job_template-train-objdet.template", "r") as tf:
    lines = tf.readlines()
data = dict()
data['body'] = "".join(lines)
print(json.dumps(data, indent=2))
