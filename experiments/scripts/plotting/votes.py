import sys

from common import gflags, np, plt, SYSTEM_COLORS, FLAGS, ParseFlags, InitializeMatplotLib, ParseVotesFile

# Define additional flags.
gflags.DEFINE_string('k9db', '../logs/votes/k9db.out', 'Input file with k9db numbers')
gflags.DEFINE_string('baseline', '../logs/votes/baseline.out', 'Input file with mariadb numbers')
gflags.DEFINE_string('hybrid', '../logs/votes/hybrid.out', 'Input file with mariadb+memcached numbers')
gflags.DEFINE_string('outdir', '../outputs/votes/', 'Output directory')

READ_SIZE = 1.86
WRITE_SIZE = 3.63 - READ_SIZE

# Plot a separate figure for each percentile
def PlotVotesRead(baseline, hybrid, k9db):
  fig, ax = plt.subplots(1,1)

  # Plot bars.
  ax.bar(-0.5, baseline["95"], color=SYSTEM_COLORS['MariaDB'], width=0.5, alpha=0.3)
  ax.bar(-0.5, baseline["50"], color=SYSTEM_COLORS['MariaDB'], width=0.5, label='MariaDB')

  ax.bar(0, hybrid["95"], color=SYSTEM_COLORS['MariaDB+Memcached'], width=0.5, alpha=0.3)
  ax.bar(0, hybrid["50"], color=SYSTEM_COLORS['MariaDB+Memcached'], width=0.5, label='MariaDB+Memcached')

  ax.bar(0.5, k9db["95"], color=SYSTEM_COLORS['K9db'], width=0.5, alpha=0.3)
  ax.bar(0.5, k9db["50"], color=SYSTEM_COLORS['K9db'], width=0.5, label='K9db')

  # Setup axis
  ax.set_ylabel('Latency [ms]')
  ax.set_ylim(ymin=0, ymax=1)
  ax.set_xlim(xmin=-1.1, xmax=1.1)
  ax.set_xlabel(None)
  ax.tick_params('x', labelrotation=0, labelbottom=False)
  ax.set_xticks([])
  #ax.set_yticks([0.5, 1, 1.5])

  # Setup legend.
  (h, l) = ax.get_legend_handles_labels()
  ax.legend(h[:3], l[:3], loc='upper left', frameon=False, ncol=1, facecolor=None,
            columnspacing=0.6, handlelength=1, handletextpad=0.5)

  # Setup figure
  fig.set_figwidth(READ_SIZE)
  fig.savefig(FLAGS.outdir + "/votes-read.pdf", bbox_inches='tight', pad_inches=0.01)

def PlotVotesWrite(baseline, hybrid, k9db):
  fig, ax = plt.subplots(1,1)

  # Plot bars.
  ax.bar(-0.5, baseline["95"], color=SYSTEM_COLORS['MariaDB'], width=0.5, alpha=0.3)
  ax.bar(-0.5, baseline["50"], color=SYSTEM_COLORS['MariaDB'], width=0.5, label='MariaDB')

  ax.bar(0, hybrid["95"], color=SYSTEM_COLORS['MariaDB+Memcached'], width=0.5, alpha=0.3)
  ax.bar(0, hybrid["50"], color=SYSTEM_COLORS['MariaDB+Memcached'], width=0.5, label='MariaDB+Memcached')

  ax.bar(0.5, k9db["95"], color=SYSTEM_COLORS['K9db'], width=0.5, alpha=0.3)
  ax.bar(0.5, k9db["50"], color=SYSTEM_COLORS['K9db'], width=0.5, label='K9db')

  # Setup axis
  #ax.set_ylim(ymin=0, ymax=1.9)
  ax.set_xlim(xmin=-1.1, xmax=1.1)
  ax.set_xlabel(None)
  ax.tick_params('x', labelrotation=0, labelbottom=False)
  ax.set_xticks([])
  #ax.set_yticks([0.5, 1, 1.5])

  # Setup figure
  fig.set_figwidth(WRITE_SIZE)
  fig.savefig(FLAGS.outdir + "/votes-write.pdf", bbox_inches='tight', pad_inches=0.01)

# Main.
if __name__ == "__main__":
  ParseFlags(sys.argv)
  InitializeMatplotLib()

  # Parse input data.
  baseline = ParseVotesFile(FLAGS.baseline)
  hybrid = ParseVotesFile(FLAGS.hybrid)
  k9db = ParseVotesFile(FLAGS.k9db)

  # Plot output.
  PlotVotesRead(baseline[1], hybrid[1], k9db[1])
  PlotVotesWrite(baseline[0], hybrid[0], k9db[0])
  
