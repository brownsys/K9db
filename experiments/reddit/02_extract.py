#!/usr/bin/env python3

import glob
import tarfile
import argparse
import os
import gzip
import lzma
import shutil

parser = argparse.ArgumentParser(description='Reddit data prep')
parser.add_argument('-i', '--input-path', type=str, dest='data_path', default='./data',
                    help='path where to read data from')
parser.add_argument('-o', '--output-path', type=str, dest='dest_path', default='./data',
                    help='path where to save data to')


if __name__ == '__main__':
    args = parser.parse_args()

    paths = glob.glob(os.path.join(args.data_path, '*'))
    print('>> found {} files'.format(len(paths)))

    for path in paths:

        ext = os.path.splitext(path)[1][1:]
        dest_path = os.path.join(args.dest_path, os.path.basename(path).replace('.' + ext, '.json'))
        print(dest_path)
        try:
            if ext == 'xz':
                print('-- extracting {}'.format(path))
                with lzma.open(path, 'rb') as f_in:
                    data = f_in.read()
                    with open(dest_path, 'wb') as f_out:
                        f_out.write(data)
                print('-- extracted to {}'.format(dest_path))
            elif ext == 'gz':
                print('-- extracting {}'.format(path))
                with gzip.open(path, 'rb') as f_in:
                    with open(dest_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                print('-- extracted to {}'.format(dest_path))
            else:
                print('-- E: Unsupported file extension {} of file {} encountered'.format(ext, path))
        except:
            print('--E: extraction failed for file {}'.format(path))

    print('>> done!')