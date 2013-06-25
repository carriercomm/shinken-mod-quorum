#!/usr/bin/python

# -*- coding: utf-8 -*-

# Copyright (C) 2013:
#    Gabes Jean, naparuba@gmail.com
#
# This file is part of Shinken Enterprise Edition, all rights reserved

import os
import mmap
import time
import json

from shinken.basemodule import BaseModule
from shinken.log import logger

properties = {
    'daemons': ['arbiter'],
    'type': 'quorum',
    'external': False,
}


# called by the plugin manager to get a broker
def get_instance(plugin):
    logger.debug("[Quorum]: Get Quorum arbiter module for plugin %s" % plugin.get_name())

    # Beware : we must have RAW string here, not unicode!
    path          = plugin.path
    max_instances = int(getattr(plugin, 'max_instances', '32'))
    
    instance = Quorum_arbiter(plugin, path, max_instances)
    return instance


# Will be asked from the arbiter when he need to go from slave to master
class Quorum_arbiter(BaseModule):
    def __init__(self, mod_conf, path, max_instances):
        BaseModule.__init__(self, mod_conf)
        self.path          = path
        self.max_instances = max_instances
        self.page_size     = mmap.PAGESIZE
        self.total_size    = self.max_instances*self.page_size


    # Called by Arbiter to say 'let's prepare yourself guy'
    def init(self):
        if not os.path.exists(self.path):
            logger.info("[Quorum] Trying to create a quorum file %s of %s size" % (self.path, self.total_size))
            # Opening the file in create mode, and fill with fuull size of 0
            fd = os.open(FILE, os.O_CREAT|os.O_WRONLY)
            os.write(fd, '\0'*self.total_size)
            os.close(fd)
        else:
            # Assume that the file is quite as big as it should
            f = os.open(FILE, os.O_RDWR)
            end = os.lseek(f, 0, os.SEEK_END)
            # if the end offset is too smal, try to increase it
            if end < self.total_size:
                logger.info("[Quorum] Trying to increase the quorum file %s of %s size" % (self.path, self.total_size))
                for p in range(end, self.total_size):
                    os.write(f, '\0')
            os.close(f)

        # Ok here we got a valid file
        logger.info("[Quorum] Quorum file checked")
