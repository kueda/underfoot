"""
Converts the metadata in USGS metadata text files back to the XML from whence
it came

This is a very flawed parser that assumes a tex tfile with key: value pairs
where indentation determines depth. USGS metadata text files are pretty
messy, often with inconsistent indentation. Sometimes they contain lines
that will just screw this up, like text blocks that contain a leading
`word:otherword` combo.
"""

import re
from sys import argv, exit as sys_exit
import xml.etree.ElementTree as ET

DEBUG = False

try:
    argv[1]
except IndexError:
    print("You must specify an input path")
    sys_exit()
try:
    argv[2]
except IndexError:
    print("You must specify an output path")
    sys_exit()

PREV_INDENT = 0

# Open the file, check for latin encoding. Probably a better way to do this
# without loading the whole file into memory, but whatever, these are pretty
# small.
with open(argv[1], encoding="utf-8") as utf_file:
    stack = []
    try:
        lines = utf_file.readlines()
    except UnicodeDecodeError:
        utf_file.close()
        with open(argv[1], encoding="latin-1") as latin_file:
            lines = latin_file.readlines()
    utf_file.close()

for line in lines:
    # skip blank line
    if not line or len(line.strip()) == 0:
        continue
    indent = len(line) - len(line.lstrip())
    if DEBUG:
        print(
            "### {} {} {}".format(
                str(PREV_INDENT).rjust(5),
                str(indent).rjust(5),
                line.replace('\n', "")
            )
        )
    matches = re.match(r'^\s*(([A-z_\-]+):)?\s*?(.+)?', line)
    # skip lines that don't match the key/value pattern
    if not matches:
        continue
    keycolon, key, val = matches.groups()
    # if there's a key and the indentation went down, that means we're closing
    # tags, so pop out based on the level of indentation
    if key and indent < PREV_INDENT:
        while len(stack) > 1:
            prev = stack.pop()
        if DEBUG:
            print(f"popped {prev['node'].tag}")
        if DEBUG:
            print(f"adding {prev['node'].tag} to {stack[-1]['node'].tag}")
        stack[-1]['node'].append(prev['node'])
        if indent >= prev['indent']:
            if DEBUG:
                print("stop the pop")
            break
    # key/value pairs can be added as a complete child of the top of the stack
    if key and val:
        node = ET.SubElement(stack[-1]['node'], key)
        node.text = val.strip()
        if DEBUG:
            print(f"added {node.tag} as a child of {stack[-1]['node'].tag}")
        prev_node = node
    # bare keys mean we need push a new node to the stack, b/c the next lines
    # will be children
    elif key:
        node = ET.Element(key)
        if val:
            node.text = val
        stack.append({'node': node, 'indent': indent})
        if DEBUG:
            print(f"pushed {node.tag}")
        prev_node = node
    # bare values should be added as text for the node at the top of the stack
    else:
        if prev_node.text:
            prev_node.text = f"{prev_node.text}\n{val}"
        else:
            prev_node.text = val
        if DEBUG:
            print(f"added text to {stack[-1]['node'].tag}")
    if key:
        PREV_INDENT = indent
# close any remaining elements on the stack
while len(stack) > 1:
    prev = stack.pop()
    stack[-1]['node'].append(prev['node'])
ET.ElementTree(stack[-1]['node']).write(argv[2], encoding="utf-8")
