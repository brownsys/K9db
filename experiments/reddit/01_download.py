#!/usr/bin/env python3

import os
import sys
import tempfile
import time
import argparse
import json
import os
import glob
import sys
import urllib.request
from bs4 import BeautifulSoup
# use tqdm module https://pypi.python.org/pypi/tqdm
# pip install tqdm
import urllib
from tqdm import tqdm

parser = argparse.ArgumentParser(description='Reddit data prep')
parser.add_argument('--path', type=str, dest='dest_path', default='./data',
                    help='path where to save data to')

base_url = 'https://files.pushshift.io/reddit/daily/'

def get_urls():
    response = urllib.request.urlopen(base_url).read()
    soup = BeautifulSoup(response, "html.parser")

    links = map(lambda link: link['href'], soup.find_all('a', href=True))
    links = list(filter(lambda link: link.startswith('./'), links))
    print('>>> found {} files'.format(len(links)))
    urls = list(map(lambda link: base_url + link[2:], links))
    return urls

# downloads file from url and diplays nice progress bar
def download_file(url, location, message='', showprogress=True):
    def my_hook(t):
        """
        Wraps tqdm instance. Don't forget to close() or __exit__()
        the tqdm instance once you're done with it (easiest using `with` syntax).

        Example
        -------

        >>> with tqdm(...) as t:
        ...     reporthook = my_hook(t)
        ...     urllib.urlretrieve(..., reporthook=reporthook)

        """
        last_b = [0]

        def inner(b=1, bsize=1, tsize=None):
            """
            b  : int, optional
                Number of blocks just transferred [default: 1].
            bsize  : int, optional
                Size of each block (in tqdm units) [default: 1].
            tsize  : int, optional
                Total size (in tqdm units). If [default: None] remains unchanged.
            """
            if tsize is not None:
                t.total = tsize
            t.update((b - last_b[0]) * bsize)
            last_b[0] = b

        return inner

    if showprogress:
        with tqdm(unit='B', unit_scale=True, miniters=1,
                  desc=message + url.split('/')[-1]) as t:  # all optional kwargs
            urllib.request.urlretrieve(url, filename=location,
                                       reporthook=my_hook(t), data=None)
    else:
        urllib.request.urlretrieve(url, filename=location, data=None)

if __name__ == '__main__':
    args = parser.parse_args()

    if not os.path.exists(args.dest_path):
        os.mkdir(args.dest_path)

    urls = get_urls()

    for url in urls:
        dest_path = os.path.join(args.dest_path, os.path.basename(url))
        if not os.path.exists(dest_path):
            print('-- downloading {} to {}'.format(url, dest_path))
            download_file(url, dest_path, showprogress=True)
    print('>> done!')
