# -*- mode: ruby -*-
# vi: set ft=ruby :
Vagrant.configure("2") do |config|
  config.vm.box = "bento/ubuntu-20.04"

  config.vm.provider "virtualbox" do |v|
    v.memory = 2048
    v.cpus = 2
  end

  # Forward postgres to 5433 on the host so you can access it with QGIS and the
  # like at ubuntu@localhost:5433
  config.vm.network "forwarded_port", guest: 5432, host: 5433

  # Modifications to postgres security to allow port forwarding to the host.
  # Obviously this is insecure and only meant to be used in a local setting
  postgresql_conf_access_for_host = "listen_addresses='*'"
  pgsql_mods = "grep -q -F \"#{postgresql_conf_access_for_host}\" /etc/postgresql/12/main/postgresql.conf  || echo \"#{postgresql_conf_access_for_host}\" >> /etc/postgresql/12/main/postgresql.conf\n"
  %w(vagrant ubuntu underfoot).each do |user|
    pgsql_mods << "sudo -u postgres psql -c \"DROP ROLE IF EXISTS #{user}; CREATE ROLE #{user} INHERIT LOGIN SUPERUSER PASSWORD '#{user}'\"\n"
    pg_hba_access_for_local_ipv4 =   "host all #{user} 127.0.0.1/32 trust # Trust all local connections"
    pg_hba_access_for_host =         "host all #{user} 10.0.2.2/32 trust # Support connections from the vagrant host"
    [pg_hba_access_for_local_ipv4, pg_hba_access_for_host].each do |pattern|
      pgsql_mods << "grep -q -F '#{pattern}' /etc/postgresql/12/main/pg_hba.conf || echo '#{pattern}' >> /etc/postgresql/12/main/pg_hba.conf\n"
    end
  end

  # Provision the vm with the relevant dependencies, modify postgres settings
  # and create an ubuntu superuser
  config.vm.provision "shell", inline: <<-SHELL
    add-apt-repository ppa:ubuntugis/ppa
    add-apt-repository ppa:deadsnakes/ppa
    apt-get update
    apt-get install -y build-essential git virtualenv python3.8 python3.8-dev python3-gdal gdal-bin libgdal-dev postgis unzip osmosis sqlite3
    #{pgsql_mods}
    service postgresql restart
  SHELL

end
