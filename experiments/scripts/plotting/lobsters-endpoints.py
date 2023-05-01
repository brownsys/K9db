import sys

from common import gflags, np, plt, SYSTEM_COLORS, FLAGS, ParseFlags, InitializeMatplotLib, ParseLobstersFile

# Parameters configuring parsing of inputs and legend of plot.
PLOT_LABELS = {
  "Story": "Read story",
  "Frontpage": "Frontpage",
  "User": "User profile",
  "Comments": "Comments",
  "Recent": "Recent stories",
  "CommentVote": "Vote on comment",
  "StoryVote": "Vote on story",
  "Comment": "Post comment",
  "Login": "Login",
  "Submit": "Post story",
  "Logout": "Logout"
}
ENDPOINTS = ["Story", "Frontpage", "User", "Comments", "Recent", "CommentVote", "StoryVote", "Comment", "Submit"]
PERCENTILES = ["50", "95", "99"]

X = np.arange(len(ENDPOINTS))
W = 0.3

# Define additional flags.
gflags.DEFINE_string('unencrypted', '../logs/lobsters/unencrypted.out', 'Input file with unencrypted k9db numbers')
gflags.DEFINE_string('k9db', '../logs/lobsters/scale2.59.out', 'Input file with K9db numbers')
gflags.DEFINE_string('baseline', '../logs/lobsters/baseline.out', 'Input file with baseline numbers')
gflags.DEFINE_string('baseline_memory', '../logs/lobsters/baseline-memory.out', 'Input file with baseline memory numbers')
gflags.DEFINE_string('memcached_memory', '../logs/lobsters/memcached-memory.out', 'Input file with memcached memory numbers')
gflags.DEFINE_string('k9db_memory', '../logs/lobsters/k9db-memory.out', 'Input file with k9db memory numbers')
gflags.DEFINE_string('outdir', '../outputs/lobsters-endpoints/', 'Output directory')

# Formatting for the markdown output.
def PrintMarkdown(baseline, k9db, unencrypted):
  MD_LABELS = ["  Endpoint  ", "Percentile", "Baseline", "K9db", "No Enc.", "Ratio(P/B)", "Ratio(E/P)"]

  def _pad(s, n, char=" "):
    s = str(s)
    if len(s) < n:
      p = ""
      for i in range(n - len(s)):
        p += char
      return s + p
    return s

  def _format(*args):
    strs = (_pad(args[i], len(MD_LABELS[i])) for i in range(len(MD_LABELS)))
    return (("| {} " * len(MD_LABELS)) + "|").format(*strs)

  with open(FLAGS.outdir + '/numbers.md', 'w') as md_file:
    # Print header
    md_file.write(_format(*MD_LABELS))
    md_file.write('\n')
    md_file.write(_format(*(_pad('', len(l), char="-") for l in MD_LABELS)))
    md_file.write('\n')

    # Print data
    for e in ENDPOINTS:
      for p in PERCENTILES:
        bs = baseline[e][p]
        pl = k9db[e][p]
        cr = unencrypted[e][p]  # change this for unencrypted k9db numbers.
        r1 = "{:.3f}".format(pl / bs)
        r2 = "{:.3f}".format(cr / pl)
        md_file.write(_format(e, p, bs, pl, cr, r1, r2))
        md_file.write('\n')


# Plot a separate figure for each percentile
def PlotLobstersPercentiles(baseline, k9db, unencrypted):
  for percentile in PERCENTILES:
    m = [baseline[endpoint][percentile] for endpoint in ENDPOINTS]
    p = [k9db[endpoint][percentile] for endpoint in ENDPOINTS]
    e = [unencrypted[endpoint][percentile] for endpoint in ENDPOINTS]

    plt.clf()
    plt.bar(X - W, m, W, label="MariaDB", color=SYSTEM_COLORS['MariaDB'])
    plt.bar(X, p, W, label="K9db", color=SYSTEM_COLORS['K9db'])
    plt.bar(X + W, e, W, label="K9db (unencrypted)", color=SYSTEM_COLORS['K9db (unencrypted)'])

    plt.ylabel("Latency [ms]")
    plt.xticks(X, [PLOT_LABELS[e] for e in ENDPOINTS], rotation=25, ha='right')
    plt.xlabel("Lobsters endpoint")
    plt.ylim(ymax=30)
    plt.legend(frameon=False)
    plt.savefig((FLAGS.outdir + "/lobsters-{}th.pdf").format(percentile),
                format="pdf", bbox_inches="tight", pad_inches=0.01)

# Plot 50th and 95th percentile on one figure
def PlotLobstersMergedPercentiles(baseline, k9db, unencrypted):
  for percentile in ["50", "95"]:
    m = [baseline[endpoint][percentile] for endpoint in ENDPOINTS]
    p = [k9db[endpoint][percentile] for endpoint in ENDPOINTS]
    e = [unencrypted[endpoint][percentile] for endpoint in ENDPOINTS]

    alpha = 1 if percentile == "50" else 0.3
    label_mariadb = "MariaDB" if percentile == "50" else None
    label_k9db = "K9db" if percentile == "50" else None
    label_k9db_unencrypted = "K9db (unencrypted)" if percentile == "50" else None

    plt.bar(X - W, m, W, label=label_mariadb, color=SYSTEM_COLORS['MariaDB'], alpha=alpha)
    plt.bar(X, p, W, label=label_k9db, color=SYSTEM_COLORS['K9db'], alpha=alpha)
    plt.bar(X + W, e, W, label=label_k9db_unencrypted, color=SYSTEM_COLORS['K9db (unencrypted)'], alpha=alpha)

  plt.ylabel("Latency [ms]")
  plt.xticks(X, [PLOT_LABELS[e] for e in ENDPOINTS], rotation=25, ha='right')
  plt.xlabel("Lobsters endpoint")
  plt.ylim(ymax=30)
  plt.legend(frameon=False)
  plt.savefig(FLAGS.outdir + "/figure8.pdf", format="pdf", bbox_inches="tight", pad_inches=0.01)

# Print out the ratio between the memory usage of various setups.
def ComputeMemoryRatio(baseline, k9db, memcached):
  baseline_disk_usage = None
  k9db_memory_usage = None
  k9db_no_notifications = None
  k9db_index = None
  memcached_memory_usage = None

  # Parse files.
  with open(baseline) as baseline:
    for l in baseline.readlines():
      l = l.split()
      if l[0] == "lobsters":
        baseline_disk_usage = float(l[1])

  with open(k9db) as k9db:
    ls = list(k9db.readlines())
    l = ls[-1]
    l = l.split()
    k9db_memory_usage = int(l[-1]) / 1024.0
    for l in ls:
      l = l.split()
      if l[0] == "_index_0" and l[1] == "TOTAL":
        k9db_index = int(l[-1]) / 1024.0
      elif l[0] == "_14" and l[1] == "TOTAL":
        k9db_no_notifications = k9db_memory_usage - (int(l[-1]) / 1024.0)

  with open(memcached) as memcached:
    l = list(memcached.readlines())[-1]
    l = l.split()
    memcached_memory_usage = float(l[-1])

  with open(FLAGS.outdir + "/memory.txt", "w") as f:
    ov1 = memcached_memory_usage / baseline_disk_usage
    ov2 = k9db_memory_usage / memcached_memory_usage
    ov3 = k9db_memory_usage / baseline_disk_usage
    ov4 = k9db_no_notifications / memcached_memory_usage
    ov5 = k9db_no_notifications / baseline_disk_usage
    f.write('Baseline disk usage: {}\n'.format(baseline_disk_usage))
    f.write('Memcached size: {}, overhead: {}\n'.format(memcached_memory_usage, ov1))
    f.write('K9db size: {}, overhead wrt memcached: {}, wrt baseline: {}\n'.format(k9db_memory_usage, ov2, ov3))
    f.write('K9db (no expensive query) size: {}, overhead wrt memcached: {}, wrt baseline: {}\n'.format(k9db_no_notifications, ov4, ov5))
    f.write('K9db index size: {}\n'.format(k9db_index))

# Main.
if __name__ == "__main__":
  ParseFlags(sys.argv)
  InitializeMatplotLib()

  # Parse input data.
  baseline = ParseLobstersFile(FLAGS.baseline)
  k9db = ParseLobstersFile(FLAGS.k9db)
  unencrypted = ParseLobstersFile(FLAGS.unencrypted)

  # Plot output.
  # PrintMarkdown(baseline, k9db, unencrypted)
  PlotLobstersMergedPercentiles(baseline, k9db, unencrypted)

  # Perform memory comparison.
  ComputeMemoryRatio(FLAGS.baseline_memory, FLAGS.k9db_memory, FLAGS.memcached_memory)
  
