extern crate owncloud_scenarios;
use owncloud_scenarios::{Backend};

fn main() {
    let num_users = 3;
    let files_per_user = 3;
    let direct_shares_per_file = 1;
    let group_shares_per_file = 2;
    let users_per_group = 3;
    let backends = [Backend::Pelton, Backend::MySQL];
    let num_samples = 10;
    let runs = 1;
    let debug = false;

    use std::process::{Command, Child, Stdio};
    use std::io::Write;
    use std::time::{Duration, Instant};


    let mut bazel_args = vec!["mysql_proxy/src:mysql_proxy"];
    let mut pelton_args = vec![];
    if debug {
        pelton_args.push("-alsologtostderr");
    } else {
        bazel_args.push("--config");
        bazel_args.push("opt");
    }

    let run_bazel_command = |build: &str, run_args: &[&str] | {
        let mut args = vec![build];
        args.extend(bazel_args.iter());
        if !run_args.is_empty() {
            args.push("--");
            args.extend(run_args.iter());
        }
        Command::new("bazel")
            .args(args)
            .current_dir("../..")
            .spawn()
            .unwrap()
    };
    let mut build = run_bazel_command("build", &vec![]);

    if !build.wait().unwrap().success() {
        eprintln!("bazel build failed");
        return;
    }

    let mut f = std::fs::File::create("results.csv").unwrap();

    writeln!(f, "Run,Backend,Time").unwrap();
    for run in 0..runs {
        for backend in backends.iter() {
            let mut pelton_handle = if backend.is_pelton() {
                Some(
                    run_bazel_command("run", &pelton_args)
                )
            } else {
                None
            };
        
            std::thread::sleep(Duration::from_secs(3));
            let mut result_proc = 
                Command::new("cargo")
                    .args(
                        ["run", "--bin", "benchmark-harness", "--",
                         "--num-users", &num_users.to_string(),
                         "--files-per-user", &files_per_user.to_string(),
                         "--direct-shares-per-file", &direct_shares_per_file.to_string(),
                         "--group-shares-per-file", &group_shares_per_file.to_string(),
                         "--users-per-group", &users_per_group.to_string(),
                         "--backend", &backend.to_string(),
                         "--samples", &num_samples.to_string(),
                        ])
                    .stdout(Stdio::piped())
                    .spawn()
                    .unwrap();
            let result_status = result_proc.wait().unwrap();
            let h_res = pelton_handle.map(|mut h| h.kill());
            if !result_status.success() {
                return;
            }
            let mut result_str = String::from_utf8(result_proc.wait_with_output().unwrap().stdout).unwrap();
            let mut result_ls = result_str.lines();
            let result = result_ls.next().expect("No lines were emitted by child");
            writeln!(f, "{},{},{}", run, backend, result).unwrap();
        }
    }
}