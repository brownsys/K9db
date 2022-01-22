# Directions for setting up pelton dependencies on cloud instances.
1. Create the instance, we are using `e2-standard-8` with 8 VCPUs and 32GB RAM.
   We also have 30 GB SSD, and no hard drive.
2. Clone pelton into your machine so that the root location is `/home/pelton/pelton`
3. Go to the root of pelton `cd /home/pelton/pelton` and run:
```bash
./osdi/setup-user.sh
bazel build ...
```
4. If the directory is not the same as above, you will need to modify
   the library linking directory inside pelton/proxy/BUILD.bazel
