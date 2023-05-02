import sys

from common import gflags, np, plt, SYSTEM_COLORS, FLAGS, ParseFlags, InitializeMatplotLib, ParseLobstersFile
from os import listdir
from os.path import join, isfile

# Parameters configuring parsing of inputs and legend of plot.
ENDPOINTS = ["Story", "Frontpage", "User", "Comments", "Recent", "CommentVote", "StoryVote", "Comment", "Submit"]
PERCENTILE = "95"
DATA_SCALE_1 = 5800

# Define additional flags.
gflags.DEFINE_string('dir', '../logs/lobsters-scale/', 'Path to directory with scale numbers')
gflags.DEFINE_string('outdir', '../outputs/lobsters-scale', 'Output directory')

def GetFiles():
  result, scales = [], []
  for f in listdir(FLAGS.dir):
    p = join(FLAGS.dir, f)
    if not isfile(p):
      continue
    if not f.endswith(".out"):
      continue
    if not f.startswith("scale"):
      continue
    result.append(p)
    scales.append(float(f[len("scale"):-4]))
  return result, scales

# Plot a separate figure for each percentile
def PlotLobstersWhiskers(data, scales):
  fig, ax = plt.subplots()

  pairs = []
  for endpoints, scale in zip(data, scales):
    x = round(scale * DATA_SCALE_1, -3)
    print(scale, x)
    latencies = [ endpoints[e][PERCENTILE] for e in ENDPOINTS ]
    pairs.append((x, latencies))

  # Sort by scale.
  pairs.sort(key=lambda x: x[0])

  # Create data vectors for plotting.
  labels = ["{}k".format(int(round(x, 3) / 1000)) for (x, _) in pairs]
  x = np.arange(len(pairs))
  series = [latencies for (_, latencies) in pairs]

  # Plot and fix color.
  bp = ax.boxplot(series, positions=x, widths=0.5, patch_artist=True,
                  boxprops = { 'lw': 0.5 }, medianprops = { 'c': 'k' },
                  whiskerprops = { 'lw': 0.5 }, whis=(0, 100))
  for patch in bp['boxes']:
    patch.set_facecolor(SYSTEM_COLORS['K9db'])

  # Setup axis.
  plt.ylabel("Latency [ms]")
  plt.ylim(ymax = 25)
  plt.xlabel("Number of users")
  plt.xticks(x, labels)

  # Setup legend.
  ax.legend([bp["boxes"][0]], ['K9db'], loc='upper left', frameon=False,
            facecolor=None)
            
  plt.savefig(FLAGS.outdir + "/lobsters-scale.pdf", format="pdf",
              bbox_inches="tight", pad_inches=0.01)

# Main.
if __name__ == "__main__":
  ParseFlags(sys.argv)
  InitializeMatplotLib()

  # Parse input data.
  data = []
  files, scales = GetFiles()
  for f in files:
    data.append(ParseLobstersFile(f))

  # Plot output.
  PlotLobstersWhiskers(data, scales)
  
