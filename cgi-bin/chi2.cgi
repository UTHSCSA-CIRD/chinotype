#!/bin/sh
# ack:
# http://wiki.pylonshq.com/display/pylonscookbook/Installing+and+running+Pylons+as+plain+CGI+with+a+no-frills+hosting+service

cd /usr/local/chi2
# edit the below with the correct path!
export LD_LIBRARY_PATH=/usr/lib/oracle/12.1/client64/lib
python -m param_check http://localhost/webclient/index.php http://localhost:9090/i2b2/services/PMService/ /var/log/chi2
