import sys

from common import gflags, np, plt, SYSTEM_COLORS, FLAGS, ParseFlags, InitializeMatplotLib, ParseOwncloudFile, plot_high_cut_border, plot_low_cut_border, get_middle

# Define additional flags.
gflags.DEFINE_string('k9db', '../logs/owncloud/k9db.out', 'Input file with K9db numbers')
gflags.DEFINE_string('views', '../logs/drilldown/noviews.out', 'Input file with views disabled')
gflags.DEFINE_string('accessors', '../logs/drilldown/noaccessors.out', 'Input file with accessors disabled')
gflags.DEFINE_string('physical', '../logs/drilldown/physical.out', 'Input file with physical separation')
gflags.DEFINE_string('outdir', '../outputs/drilldown', 'Output directory')

gflags.DEFINE_string('cuts', 'no', 'How many cuts to do on the y-axis ' + \
                                 '(\eg "(x1, y1), (x2, y2)" will a bottom ' + \
                                 'portion from x1 -> y1 and a top portion from x2 -> y2.)')

ENDPOINTS = ["Read", "Direct", "Group", "Read File PK", "Update File PK"]
LABELS = ["View files", "Share w/ user", "Share w/ group", "Get", "Update"]
#LEGEND_LABELS = ['Physical separation', 'Logical separation', 'w/ Accessors', 'w/ In-mem indexes', 'K9db']
LEGEND_LABELS = ['Physical separation (1k users)', 'All owners', 'No views', 'K9db']
LEGEND_LABELS = list(reversed(LEGEND_LABELS))
ACTUAL_LABELS = list(reversed(['Physical separation (1k users)', '+ Logical $\mu$DBs', '+ Accessors', '+ Views']))

W = 0.18
X = np.arange(len(ENDPOINTS))
Xs = [X - (W * (i - 2)) for i in range(len(LEGEND_LABELS))]

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

def PlotCuts(data, cuts):
  # Broken plot: we have three plots: the bottom one has small scale for READ by PK.
  # The top one is for the share_view.
  fig, cuts_ax = plt.subplots(len(cuts), 1, sharex=True,
                              gridspec_kw={'height_ratios': [1 for c in cuts],
                                           'hspace': 0.05})

  # Make it bot to top
  cuts_ax = list(reversed(cuts_ax))
  data = list(reversed(data))

  # Plot bars.
  for (i, ax) in enumerate(cuts_ax):
    first = i ==0
    last = i == (len(cuts_ax) - 1)

    if not first:
      ax.spines.bottom.set_linestyle(":")
      ax.tick_params('x', bottom=False, labelbottom=False, which='both')
      ax.axes.xaxis.set_ticks_position('none')
      plot_low_cut_border(ax)
    if not last:
      ax.spines.top.set_linestyle(':')
      plot_high_cut_border(ax)
    # Build data bars.
    for x, y, label, alabel in zip(Xs, data, LEGEND_LABELS, ACTUAL_LABELS):
      y50 = [y[e]["50"] for e in ENDPOINTS]
      y95 = [y[e]["95"] for e in ENDPOINTS]

      # Plot bars.
      ax.bar(x, y95, color=SYSTEM_COLORS[label], width=W, alpha=0.3)
      ax.bar(x, y50, color=SYSTEM_COLORS[label], width=W, label=alabel)

  # Setup axis: each section only covers the cut.
  for ax, (cut0, cut1) in zip(cuts_ax[1:], cuts[1:]):
    ax.set_ylim(ymin=cut0, ymax=cut1)
    ax.set_xlabel(None)
    #ax.tick_params('x', labelrotation=0, labelbottom=False)
    middle = get_middle(cut0, cut1)
    ax.set_yticks([middle, cut1])

  # The bot part has x-axis labels.
  cut0, cut1 = cuts[0]
  bot_ax, top_ax = cuts_ax[0], cuts_ax[-1]
  bot_ax.set_ylim(ymin=cut0, ymax=cut1)
  bot_ax.set_xlabel(None)
  bot_ax.set_xticks(X, labels=LABELS, rotation=10)#, ha='right')
  bot_ax.set_yticks([cut0, get_middle(cut0, cut1), cut1])

  # Get the broken feel: hide all indications that top and bottom are different subplots.


  # Plot separator lines.

  # Setup legend.
  (h, l) = bot_ax.get_legend_handles_labels()
  h = h[:len(LEGEND_LABELS)]
  l = l[:len(LEGEND_LABELS)]
  h = h[::-1]
  l = l[::-1]
  fig.legend(h, l, loc=(0.26, 0.71),
             frameon=True, ncol=2, facecolor=None,
             columnspacing=0.6, handlelength=1, handletextpad=0.5).get_frame().set_linewidth(0.0)

  fig.text(0.02, 0.5, 'Latency [ms]', va='center', rotation='vertical')

  # Setup figure
  fig.savefig(FLAGS.outdir + "/drilldown.pdf", bbox_inches='tight', pad_inches=0.01)

def PlotNoCuts(data):
  data = list(reversed(data))

  # Broken plot: we have three plots: the bottom one has small scale for READ by PK.
  # The top one is for the share_view.
  fig, ax = plt.subplots(1, 1)

  # Build data bars.
  for x, y, label in zip(Xs, data, LEGEND_LABELS):
    y50 = [y[e]["50"] for e in ENDPOINTS]
    y95 = [y[e]["95"] for e in ENDPOINTS]

    # Plot bars.
    ax.bar(x, y95, color=SYSTEM_COLORS[label], width=W, alpha=0.3)
    ax.bar(x, y50, color=SYSTEM_COLORS[label], width=W, label=label)

  # Setup axis.
  ax.set_xlabel(None)
  ax.set_ylabel('Latency [ms]')
  ax.set_xticks(X, labels=LABELS, rotation=10)

  # Setup legend.
  (h, l) = ax.get_legend_handles_labels()
  h = h[:len(LEGEND_LABELS)]
  l = l[:len(LEGEND_LABELS)]
  h = h[::-1]
  l = l[::-1]
  ax.legend(h, l, loc='upper right',
            frameon=False, ncol=2, facecolor=None,
            columnspacing=0.6, handlelength=1, handletextpad=0.5)

  # Setup figure
  fig.savefig(FLAGS.outdir + "/drilldown.pdf", bbox_inches='tight', pad_inches=0.01)

# Main.
if __name__ == "__main__":
  ParseFlags(sys.argv)
  InitializeMatplotLib()

  # Parse files.
  inputs = [FLAGS.physical, FLAGS.accessors, FLAGS.views, FLAGS.k9db]
  data = [ParseOwncloudFile(input) for input in inputs]

  # Plot output.
  cuts, flag = ParseCuts(FLAGS.cuts)
  if flag:
    PlotCuts(data, cuts)
  else:
    PlotNoCuts(data)
  
