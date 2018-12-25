#!/bin/bash
wget -O /dev/null http://127.0.0.1:14159/health && exit 0 || exit 1