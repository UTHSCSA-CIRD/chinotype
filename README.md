# chinotype
General purpose script for looking for over and under represented EHR facts in an i2b2 patient-set. Can be used for finding adverse drug reactions, off-label uses, healthcare outcomes disparities, comorbidities, and who knows what else. The name is a portmanteau of Chi-squared and phenotype.

Also includes a web plugin to call the script from i2b2.

Note: you may need to edit cgi-bin/chi2.cgi to have the correct LD_LIBRARY_PATH for your system.
Note: this assumes you have already deployed apache, java, wildfly, and i2b2

## Packages needed:
### CentOS
#### Oracle Instant Client (download current latest version from oracle.com)
* `rpm -ivh ~/oracle-instantclient12.2-basic-12.2.0.1.0-1.i386.rpm `
* `rpm -ivh ~/oracle-instantclient12.2-devel-12.2.0.1.0-1.i386.rpm `

  `echo 'export ORACLE_VERSION="12.2"' >> $HOME/.bashrc `
  `echo 'export ORACLE_HOME="/usr/lib/oracle/$ORACLE_VERSION/client"'>> $HOME/.bashrc `
  `echo 'export PATH=$PATH:"$ORACLE_HOME/bin"' >> $HOME/.bashrc`
  `echo 'export LD_LIBRARY_PATH="$ORACLE_HOME/lib"' >> $HOME/.bashrc`

#### Development Environment
* `yum install epel-release`
* `yum install python-pip`
* `yum install gcc`
* `yum install python-devel`
#### Additional Python Modules
I don't know why yet, but you need to do these before you do `pip install -r requirements.txt`
* `pip install argh`
* `pip install argparse`
* `pip install cx_Oracle`
#### Create Directories (assuming that your cgi scripts run with apache's permissions)
`mkdir /var/log/chi2; chgrp apache /var/log/chi2; chmod g+w /var/log/chi2`
`cp -r chi2 /usr/local/chi2; chgrp -r apache /usr/local/chi2; chmod -R g+w /usr/local/chi2`
`cp cgi-bin/chi2.cgi /var/www/cgi-bin`
`mkdir /var/www/html/webclient/js-i2b2/cells/plugins/uthscsa/`
`cp -r webclient/js-i2b2/cells/plugins/uthscsa/chi2 /var/www/html/webclient/js-i2b2/cells/plugins/uthscsa`

