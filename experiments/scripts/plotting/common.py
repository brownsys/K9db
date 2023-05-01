import gflags
import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Consistent colors for the different baseline / systems.
SYSTEM_COLORS = {
  'MariaDB': 'C0',
  'MariaDB+Memcached': 'C6',
  'K9db (unencrypted)': 'C2',
  'K9db': 'C1',
  'Physical separation (1k users)': 'C3',
  'All owners': 'C4',
  'No views': 'C7',
}

# Command line flags.
FLAGS = gflags.FLAGS
gflags.DEFINE_bool('paper', False, 'Adjusts the size of the plots for paper.')

def ParseFlags(argv):
  try:
    argv = FLAGS(argv)
  except gflags.FlagsError as e:
    print(e)
    print('Usage: %s ARGS\\n%s' % (argv[0], FLAGS))
    sys.exit(1)


# Initialize plots.
def InitializeMatplotLib():
  if FLAGS.paper:
    matplotlib.rc('font', family='serif', size=9)
    matplotlib.rc('text.latex', preamble='\\usepackage{times,mathptmx}')
    matplotlib.rc('text', usetex=True)
    matplotlib.rc('legend', fontsize=8)
    matplotlib.rc('figure', figsize=(3.63,1.5))
    matplotlib.rc('axes', linewidth=0.5)
    matplotlib.rc('lines', linewidth=0.5)

  else:
    font = {
      'family' : 'sans-serif',
      'sans-serif' : ['Arial'],
      'size'   : '16'
    }

    matplotlib.rc('font', **font)
    matplotlib.rc('legend', fontsize=14)
    matplotlib.rc('axes', linewidth=1.0)
    matplotlib.rc('lines', linewidth=2.0, markersize=10.0)
    matplotlib.rcParams['xtick.major.width'] = 1.5
    matplotlib.rcParams['xtick.major.size'] = 10
    matplotlib.rcParams['ytick.major.width'] = 1.5
    matplotlib.rcParams['ytick.major.size'] = 10
    matplotlib.rcParams['ytick.minor.width'] = 1
    matplotlib.rcParams['ytick.minor.size'] = 5
    plt.figure(figsize=(8, 3))



# Parse an input file.
def ParseLobstersFile(f):
  data = dict()
  with open(f) as f:
    past_comments = True
    for l in f.readlines():
      # Read until after the header
      if l[0] == "#":
        past_comments = True
        continue
      if not past_comments:
        continue

      # Ignore not sojourn
      if "sojourn" not in l:
        continue

      # Not sojourn, parse
      tokens = l.split()
      if len(tokens) != 4:
        continue

      endpoint = tokens[0]
      percentile = tokens[2]
      time = float(tokens[3]) / 1000
      d = data.get(endpoint, dict())
      d[percentile] = time
      data[endpoint] = d

  return data

def ParseVotesFile(f):
  write = dict()
  read = dict()
  with open(f) as f:
    past_comments = False
    for l in f.readlines():
      # Read until after the header
      if l[0] == "#":
        past_comments = True
        continue
      if not past_comments:
        continue

      tokens = l.split()
      operation = tokens[0]
      percentile = tokens[1]
      time = tokens[2]
      if operation == "write":
        write[percentile] = float(time) / 1000
      elif operation == "read":
        read[percentile] = float(time) / 1000
  return (write, read)

def ParseOwncloudFile(f):
  data = dict()
  with open(f) as f:
    past_comments = False
    for l in f.readlines():
      # Read until after the header
      if l[0:3] == "-->":
        past_comments = True
        continue
      if not past_comments:
        continue

      # ignore count of operations
      if not '[' in l:
        continue

      tokens = l.split(':')
      operation = tokens[0].strip()
      percentile = operation[-3:-1]
      operation = operation[:-5]
      time = tokens[1].strip()

      d = data.get(operation, dict())
      d[percentile] = float(time) / 1000
      data[operation] = d
  return data
import matplotlib
pth = matplotlib.path.Path
w = .03
cut_marker = matplotlib.path.Path(
        [(-1, 0 + w), (-0.5, 1 + w), (.5, -1 + w), (1, 0 + w),
        (1, 0-w), (.5, -1-w), (-0.5, 1-w), (-1,0 -w), (0,0) ],
        [1] + [pth.CURVE4] * 3 + [pth.LINETO] + [pth.CURVE4] * 3 + [pth.CLOSEPOLY])
marker_kwargs = dict(marker=cut_marker, markersize=7,
              linestyle="none", color='k', mec='k', mew=1, clip_on=False)


def plot_low_cut_border(ax):
  ax.plot([0,1], [0,0], transform=ax.transAxes, **marker_kwargs)
def plot_high_cut_border(ax):
  ax.plot([0, 1], [1, 1], transform=ax.transAxes, **marker_kwargs)

def get_middle(low, high):
  return round(float(low) + float(high) / 2, 1)

