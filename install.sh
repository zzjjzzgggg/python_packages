#! /bin/bash
#
# install.sh
# Copyright (C) 2016 jzzhao <jzzhao@zhlap>
#
# Distributed under terms of the MIT license.
#


pyinfo=$(python3 -c "import sys; print('python{0[0]}.{0[1]}'.format(sys.version_info[:]))")

targetdir=~/.local/lib/$pyinfo/site-packages
mkdir -p $targetdir
pwd >> $targetdir/mytools.pth
