#!/usr/bin/env python3
#
# A script to generate CMakeLists.txt from bazel C++ target.
# With CMakeLists.txt file, it can be opened directly from CLion.
#
# Usage:
# $ bazel-cmakelists
#
# CMake also support generating other IDE project files such as Eclipse and Xcode.
# Run following command additionally to generate Xcode project:
#
# $ cmake -G Xcode
#
# DON'T USE CMAKE TO BUILD THE PROJECT. It is not tested and likely to fail.

import gflags as flags
from os import path
import os
import re
import xml.etree.ElementTree as ET
import subprocess
import sys

FLAGS = flags.FLAGS

flags.DEFINE_string('output', 'CMakeLists.txt',
                    ('output file name'))

flags.DEFINE_string('project',
                    subprocess.check_output("basename $(bazel info workspace)", shell=True).strip(),
                    ('project name'))

flags.DEFINE_string('target', '//dataflow',
                    ('bazel target'))

flags.DEFINE_boolean('open', False, ('Open CLion after generation, only supports Mac'))

flags.DEFINE_boolean('mac_debug', False, ('Generate project to use with debugger on Mac'))

EXTERNAL_PATTERN = re.compile("^@(.*)\\/\\/")
SOURCE_EXTENSION = set([".h", ".hpp", ".c", ".cc", ".cpp", ".cxx"])

def GetBasePath(fn):
  fn = str(fn)
  fn = fn.replace('/:', '/')
  fn = fn.replace(':', '/')
  if EXTERNAL_PATTERN.match(fn):
    fn = EXTERNAL_PATTERN.sub("external/\\1/", fn)
  return str(fn.lstrip('/'))

def ConvertGeneratedPath(fn):
  bazel_root = 'bazel-' + str(FLAGS.project)
  if str(FLAGS.mac_debug):
    genfiles_root = path.join('bazel-out', 'local-dbg', 'genfiles')
  if not str(FLAGS.mac_debug):
    genfiles_root = 'bazel-genfiles'
  return path.join(genfiles_root, GetBasePath(fn))

def ConvertExternalPath(fn):
  bazel_root = 'bazel-' + str(FLAGS.project)
  if not str(FLAGS.mac_debug) and EXTERNAL_PATTERN.match(fn):
    return path.join(str(bazel_root), str(GetBasePath(fn)))
  return GetBasePath(fn)

def ExtractSources():
  query = 'kind("source file", deps(%s))' % str(FLAGS.target)
  sources = []
  for fn in subprocess.check_output(['bazel', 'query', query]).splitlines():
    fn = ConvertExternalPath(fn)
    if path.splitext(fn)[1] in SOURCE_EXTENSION:
      sources.append(fn)
  return sources

def ExtractGenerated():
  query = 'kind("generated file", deps(%s))' % FLAGS.target
  sources = []
  for fn in subprocess.check_output(['bazel', 'query', query]).splitlines():
    fn = ConvertGeneratedPath(fn)
    if path.splitext(fn)[1] in SOURCE_EXTENSION:
      sources.append(fn)
  return sources

def ExtractIncludes():
  query = 'kind(cc_library, deps(%s))' % str(FLAGS.target)
  includes = []
  xml = subprocess.check_output(['bazel', 'query', query, '--output', 'xml'])
  tree = ET.fromstring(xml)
  for e in tree.findall(".//list[@name='includes']/.."):
    prefix = e.attrib['name'].split(':')[0]
    for val in [i.attrib['value'] for i in e.findall("list[@name='includes']/string")] + ["."]:
      geninc = path.normpath(path.join(ConvertGeneratedPath(prefix), val))
      if geninc not in includes:
        includes.append(geninc)
      inc = path.normpath(path.join(ConvertExternalPath(prefix), val))
      if inc not in includes:
        includes.append(inc)

  for e in tree.findall(".//string[@name='include_prefix']/.."):
    prefix = e.attrib['name'].split(':')[0]
    include_prefix = e.find("string[@name='include_prefix']").attrib['value']
    geninc = path.normpath(path.join(ConvertGeneratedPath(prefix), val))
    if geninc.endswith(include_prefix):
      geninc = geninc[:-len(include_prefix)]
      if geninc not in includes:
        includes.append(geninc)
    inc = path.normpath(path.join(ConvertExternalPath(prefix), val))
    if inc.endswith(include_prefix):
      inc = inc[:-len(include_prefix)]
      if inc not in includes:
        includes.append(inc)

  return includes

def ExtractDefines():
  query = 'attr("defines", "", deps(%s))' % str(FLAGS.target)
  xml = subprocess.check_output(['bazel', 'query', query, '--output', 'xml'])
  tree = ET.fromstring(xml)
  defines = []
  for e in tree.findall(".//list[@name='defines']/string"):
    defines.append(e.attrib['value'])
  return defines

def ExtractCopts():
  query = 'attr("copts", "", deps(%s))' % str(FLAGS.target)
  xml = subprocess.check_output(['bazel', 'query', query, '--output', 'xml'])
  tree = ET.fromstring(xml)
  copts = []
  for e in tree.findall(".//list[@name='copts']/string"):
    copts.append(e.attrib['value'])
  return copts

def GenerateCMakeList():
  bazel_args = ['build']
  bazel_args.extend(['-c', 'dbg'])
  bazel_args.append(str(FLAGS.target))
  subprocess.check_output(['bazel'] + bazel_args)
  sources = ExtractSources()
  generated = ExtractGenerated()
  includes = ExtractIncludes()
  defines = ExtractDefines()
  copts = ExtractCopts()
  for opt in copts:
    if opt.startswith("-I-"):
      includes.append(opt[3:])
    elif opt.startswith("-I"):
      includes.append(opt[2:])
    elif opt.startswith("-D"):
      defines.append(opt[2:])

  output = str(FLAGS.output)
  if str(FLAGS.mac_debug):
    file_root = subprocess.check_output(['bazel', 'info', 'execution_root']).strip()
  else:
    file_root = subprocess.check_output(['bazel', 'info', 'workspace']).strip()
  output = str(FLAGS.output)#path.join(str(file_root), str(FLAGS.output))
  try:
    os.remove(output)
  except:
    pass
  with open(output, 'w') as cmakelist:
    cmakelist.write("cmake_minimum_required(VERSION 3.3)\n")
    cmakelist.write("project(%s)\n\n" % str(FLAGS.project))
    cmakelist.write("set(CMAKE_CXX_FLAGS \"${CMAKE_CXX_FLAGS} -std=c++11\")\n\n")
    cmakelist.write("set(SOURCE_FILES\n    ")
    cmakelist.write("\n    ".join(sources))
    cmakelist.write(")\n\n")
    cmakelist.write("set(GENERATED_SOURCES\n    ")
    cmakelist.write("\n    ".join(generated))
    cmakelist.write(")\n\n")
    cmakelist.write("set(INCLUDE_DIRECTORIES\n    ")
    cmakelist.write("\n    ".join(includes))
    if FLAGS.mac_debug:
      cmakelist.write("\n    bazel-out/local-dbg/genfiles")
    else:
      cmakelist.write("\n    bazel-genfiles")
    cmakelist.write("\n    .)\n\n")
    if len(defines) > 0:
      cmakelist.write("add_definitions(\n    -D")
      cmakelist.write("\n    -D".join(defines))
      cmakelist.write(")\n\n")

    cmakelist.write("include_directories(${INCLUDE_DIRECTORIES})\n")
    cmakelist.write("add_executable({} ${{GENERATED_SOURCES}} ${{SOURCE_FILES}})\n".format(FLAGS.project.decode()))

  if str(FLAGS.open):
    if sys.platform == 'darwin':
      subprocess.call(['open', output, '-a', 'CLion'])
    else:
      sys.stderr.write("Open flag is only supported in Mac.\n")
  # sys.stderr.write("CMakeLists.txt generated in following directory:\n")
  # sys.stderr.write(str(file_root) + "\n")

if __name__ == "__main__":
    try:
        argv = FLAGS(sys.argv)  # parse flags
    except flags.FlagsError as e:
        sys.exit('%s\nUsage: %s ARGS\n%s' % (e, sys.argv[0], FLAGS))

    GenerateCMakeList()
