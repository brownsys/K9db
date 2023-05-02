import sys

from common import gflags, np, plt, SYSTEM_COLORS, FLAGS, ParseFlags, InitializeMatplotLib, ParseOwncloudFile, get_middle, plot_high_cut_border, plot_low_cut_border

# Define additional flags.
gflags.DEFINE_string('k9db', '../logs/owncloud/k9db.out', 'Input file with K9db numbers')
gflags.DEFINE_string('baseline', '../logs/owncloud/mariadb.out', 'Input file with mariadb numbers')
gflags.DEFINE_string('hybrid', '../logs/owncloud/hybrid.out', 'Input file with mariadb+memcached numbers')
gflags.DEFINE_string('cuts', 'no', 'How many cuts to do on the y-axis ' + \
                                   '(\eg "(x1, y1), (x2, y2)" will a bottom ' + \
                                   'portion from x1 -> y1 and a top portion from x2 -> y2.)')
gflags.DEFINE_string('baseline_memory', '../logs/owncloud/mariadb-memory.out', 'Input file with baseline memory numbers')
gflags.DEFINE_string('memcached_memory', '../logs/owncloud/memcached-memory.out', 'Input file with memcached memory numbers')
gflags.DEFINE_string('k9db_memory', '../logs/owncloud/k9db-memory.out', 'Input file with k9db memory numbers')
gflags.DEFINE_string('outdir', '../outputs/owncloud', 'Output directory')


ENDPOINTS = ["Read", "Direct", "Group", "Read File PK", "Update File PK"]
LABELS = ["View files", "Share w/ user", "Share w/ group", "Get", "Update"]

W = 0.3
X = np.arange(len(ENDPOINTS))
X_BASE = X - W
X_HYBRID = X
X_K9DB = X + W

CUTS = [(0, 1), (1, 7), (7, 39)]

def ParseCuts(cuts):
  if cuts == 'no':
    return [], False
  if cuts == 'yes':
    return CUTS, True

  cuts ="".join(cuts.split())
  cuts = cuts.split('),(')
  cuts[0] = cuts[0][1:]
  cuts[-1] = cuts[-1][:-1]
  cuts = [tuple(float(y) for y in c.split(',')) for c in cuts]
  return cuts, True

def PlotCuts(baseline, hybrid, k9db, cuts):
  # Broken plot: we have three plots: the bottom one has small scale for READ by PK.
  # The top one is for the share_view.
  fig, cuts_ax = plt.subplots(len(cuts), 1, sharex=True,
                              gridspec_kw={'height_ratios': [1 for c in cuts],
                                           'hspace': 0.05})
  # Make it bot to top
  cuts_ax = list(reversed(cuts_ax))

  # Build data bars.
  base50 = [baseline[e]["50"] for e in ENDPOINTS]
  base95 = [baseline[e]["95"] for e in ENDPOINTS]
  hybrid50 = [hybrid[e]["50"] for e in ENDPOINTS]
  hybrid95 = [hybrid[e]["95"] for e in ENDPOINTS]
  k9db50 = [k9db[e]["50"] for e in ENDPOINTS]
  k9db95 = [k9db[e]["95"] for e in ENDPOINTS]

  # Plot bars.
  for ax in cuts_ax:
    ax.bar(X_BASE, base95, color=SYSTEM_COLORS['MariaDB'], width=W, alpha=0.3)
    ax.bar(X_BASE, base50, color=SYSTEM_COLORS['MariaDB'], width=W, label='MariaDB')
    ax.bar(X_HYBRID, hybrid95, color=SYSTEM_COLORS['MariaDB+Memcached'], width=W, alpha=0.3)
    ax.bar(X_HYBRID, hybrid50, color=SYSTEM_COLORS['MariaDB+Memcached'], width=W, label='MariaDB+Memcached')
    ax.bar(X_K9DB, k9db95, color=SYSTEM_COLORS['K9db'], width=W, alpha=0.3)
    ax.bar(X_K9DB, k9db50, color=SYSTEM_COLORS['K9db'], width=W, label='K9db')

  # Setup axis: each section only covers the cut.
  for ax, (cut0, cut1) in zip(cuts_ax[1:], cuts[1:]):

    ax.set_ylim(ymin=float(cut0), ymax=float(cut1))
    ax.set_xlabel(None)
    ax.tick_params('x', labelrotation=0, labelbottom=False)
    ax.set_yticks([get_middle(cut0, cut1), cut1])

  # The bot part has x-axis labels.
  cut0, cut1 = cuts[0]
  bot_ax, top_ax = cuts_ax[0], cuts_ax[-1]
  bot_ax.set_ylim(ymin=cut0, ymax=cut1)
  bot_ax.set_xlabel(None)
  bot_ax.set_xticks(X, labels=LABELS, rotation=10)#, ha='right')
  bot_ax.set_yticks([cut0, get_middle(cut0, cut1), cut1])

  # Get the broken feel: hide all indications that top and bottom are different subplots.
  bot_ax.spines.top.set_linestyle(":")
  top_ax.spines.bottom.set_linestyle(":")
  top_ax.axes.xaxis.set_ticks_position('none')
  for ax in cuts_ax[1:-1]:
    ax.spines.bottom.set_linestyle(":")
    ax.tick_params('x', bottom=False, labelbottom=False, which='both')
    ax.axes.xaxis.set_ticks_position('none')
    ax.spines.top.set_linestyle(':')
    plot_high_cut_border(ax)
    plot_low_cut_border(ax)

  plot_high_cut_border(cuts_ax[0])
  plot_low_cut_border(cuts_ax[-1])

  # Setup legend.
  (h, l) = bot_ax.get_legend_handles_labels()
  fig.legend(h[:3], l[:3], loc=(0.565, 0.61), frameon=False, ncol=1, facecolor=None,
             columnspacing=0.6, handlelength=1, handletextpad=0.5)

  fig.text(0.02, 0.5, 'Latency [ms]', va='center', rotation='vertical')

  # Setup figure
  fig.savefig(FLAGS.outdir + "/owncloud-comparison.pdf", bbox_inches='tight', pad_inches=0.01)

def PlotNoCuts(baseline, hybrid, k9db):
  # Broken plot: we have three plots: the bottom one has small scale for READ by PK.
  # The top one is for the share_view.
  fig, ax = plt.subplots(1, 1)

  # Build data bars.
  base50 = [baseline[e]["50"] for e in ENDPOINTS]
  base95 = [baseline[e]["95"] for e in ENDPOINTS]
  hybrid50 = [hybrid[e]["50"] for e in ENDPOINTS]
  hybrid95 = [hybrid[e]["95"] for e in ENDPOINTS]
  k9db50 = [k9db[e]["50"] for e in ENDPOINTS]
  k9db95 = [k9db[e]["95"] for e in ENDPOINTS]

  # Plot bars.
  ax.bar(X_BASE, base95, color=SYSTEM_COLORS['MariaDB'], width=W, alpha=0.3)
  ax.bar(X_BASE, base50, color=SYSTEM_COLORS['MariaDB'], width=W, label='MariaDB')
  ax.bar(X_HYBRID, hybrid95, color=SYSTEM_COLORS['MariaDB+Memcached'], width=W, alpha=0.3)
  ax.bar(X_HYBRID, hybrid50, color=SYSTEM_COLORS['MariaDB+Memcached'], width=W, label='MariaDB+Memcached')
  ax.bar(X_K9DB, k9db95, color=SYSTEM_COLORS['K9db'], width=W, alpha=0.3)
  ax.bar(X_K9DB, k9db50, color=SYSTEM_COLORS['K9db'], width=W, label='K9db')

  # Setup axis.
  ax.set_xlabel(None)
  ax.set_ylabel('Latency [ms]')
  ax.set_xticks(X, labels=LABELS, rotation=10)

  # Setup legend.
  (h, l) = ax.get_legend_handles_labels()
  ax.legend(h[:3], l[:3], loc='upper right', frameon=False, ncol=1, facecolor=None,
            columnspacing=0.6, handlelength=1, handletextpad=0.5)

  # Setup figure
  fig.savefig(FLAGS.outdir + "/owncloud-comparison.pdf", bbox_inches='tight', pad_inches=0.01)

# Memory.
def ComputeMemoryRatio(baseline, k9db, memcached):
  baseline_disk_usage = None
  k9db_memory_usage = None
  memcached_memory_usage = None

  # Parse files.
  with open(baseline) as baseline:
    for l in baseline.readlines():
      l = l.split()
      if l[0] == "owncloud":
        baseline_disk_usage = float(l[1])

  with open(k9db) as k9db:
    ls = list(k9db.readlines())
    l = ls[-1]
    l = l.split()
    k9db_memory_usage = int(l[-1]) / 1024.0

  with open(memcached) as memcached:
    for l in memcached.readlines():
      l = l.split()
      if l[0] == 'STAT' and l[1] == 'bytes':
        memcached_memory_usage = int(l[-1]) / 1048576.0

  with open(FLAGS.outdir + "/memory.txt", "w") as f:
    ov1 = memcached_memory_usage / baseline_disk_usage
    ov2 = k9db_memory_usage / memcached_memory_usage
    ov3 = k9db_memory_usage / baseline_disk_usage
    f.write('Baseline disk usage: {}\n'.format(baseline_disk_usage))
    f.write('Memcached size: {}, overhead: {}\n'.format(memcached_memory_usage, ov1))
    f.write('K9db size: {}, overhead wrt memcached: {}, wrt baseline: {}\n'.format(k9db_memory_usage, ov2, ov3))

# Main.
if __name__ == "__main__":
  ParseFlags(sys.argv)
  InitializeMatplotLib()

  # Parse input data.
  baseline = ParseOwncloudFile(FLAGS.baseline)
  hybrid = ParseOwncloudFile(FLAGS.hybrid)
  k9db = ParseOwncloudFile(FLAGS.k9db)

  # Plot output.
  cuts, flag = ParseCuts(FLAGS.cuts)
  if flag:
    PlotCuts(baseline, hybrid, k9db, cuts)
  else:
    PlotNoCuts(baseline, hybrid, k9db)

  # Print memory numbers.
  ComputeMemoryRatio(FLAGS.baseline_memory, FLAGS.k9db_memory, FLAGS.memcached_memory)
