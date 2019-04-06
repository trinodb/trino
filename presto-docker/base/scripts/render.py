#!/usr/bin/env python

from jinja2 import Environment, FileSystemLoader

import argparse

def render(vars, files):
  '''
  Render Jinja2 template to replace version tag
  :param args: variables to be embedded, files: file to be rendered
  :return:
  '''
  for path in files:
    env = Environment(loader=FileSystemLoader('/usr/local/presto/', encoding='utf8'))
    tpl = env.get_template(path)
    rendered = tpl.render(vars)

    # Remove *.template
    with open(path[:-9], 'w') as f:
      f.write(rendered)
      print("Rendered {}".format(path[:-9]))  

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description="Process build args from Docker")
  parser.add_argument('files', help='File to be rendered', nargs='*')
  parser.add_argument('--node-id', help='Node ID', dest='node_id')
  args = parser.parse_args()

  vars = {
    'node_id': args.node_id,
  }
  render(vars, args.files)