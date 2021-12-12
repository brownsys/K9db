# Directions for setting up pelton dependencies on cloud instances.
1. Create the instance, we are using `e2-standard-8` with 8 VCPUs and 32GB RAM.
   We also have 30 GB SSD, and no hard drive.
2. Clone pelton into your machine, a good location is under /home/pelton.
3. Go to the root of pelton `cd /home/pelton` and run:
```bash
sudo ./osdi/setup-root.sh
./osdi/setup-user.sh
bazel build ...
```

