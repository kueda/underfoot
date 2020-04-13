# -*- mode: ruby -*-
# vi: set ft=ruby :
Vagrant.configure("2") do |config|
  config.vm.box = "bento/ubuntu-18.04"

  config.vm.provider "virtualbox" do |v|
    v.memory = 2048
    v.cpus = 2
  end

  # Forward postgres to 5433 on the host so you can access it with QGIS and the
  # like at ubuntu@localhost:5433
  config.vm.network "forwarded_port", guest: 5432, host: 5433

  # # Modifications to postgres security to allow port forwarding to the host.
  # # Obviously this is insecure and only meant to be used in a local setting
  # postgresql_conf_access_for_host = "listen_addresses='*'"
  # pgsql_mods = "grep -q -F \"#{postgresql_conf_access_for_host}\" /etc/postgresql/10/main/postgresql.conf  || echo \"#{postgresql_conf_access_for_host}\" >> /etc/postgresql/10/main/postgresql.conf\n"
  # %w(vagrant ubuntu underfoot).each do |user|
  #   pgsql_mods << "sudo -u postgres psql -c \"DROP ROLE IF EXISTS #{user}; CREATE ROLE #{user} INHERIT LOGIN SUPERUSER PASSWORD '#{user}'\"\n"
  #   pg_hba_access_for_local_ipv4 =   "host all #{user} 127.0.0.1/32 trust # Trust all local connections"
  #   pg_hba_access_for_host =         "host all #{user} 10.0.2.2/32 trust # Support connections from the vagrant host"
  #   [pg_hba_access_for_local_ipv4, pg_hba_access_for_host].each do |pattern|
  #     pgsql_mods << "grep -q -F '#{pattern}' /etc/postgresql/10/main/pg_hba.conf || echo '#{pattern}' >> /etc/postgresql/10/main/pg_hba.conf\n"
  #   end
  # end

  # # Provision the vm with the relevant dependencies, modify postgres settings
  # # and create an ubuntu superuser
  # config.vm.provision "shell", inline: <<-SHELL
  #   add-apt-repository ppa:ubuntugis/ppa
  #   apt-get update
  #   apt-get install -y git
  #   apt-get install -y virtualenv
  #   apt-get install -y npm
  #   apt-get install -y python3
  #   apt-get install -y python-gdal
  #   apt-get install -y gdal-bin
  #   apt-get install -y libgdal-dev
  #   apt-get install -y postgis
  #   apt-get install -y unzip
  #   apt-get install -y osmosis
  #   #{pgsql_mods}
  #   service postgresql restart
  # SHELL

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
  # FYI, setting up the custom Postgres repo is based on https://wiki.postgresql.org/wiki/Apt
  config.vm.provision "shell", inline: <<-SHELL
    add-apt-repository ppa:ubuntugis/ppa
    apt-get update
    apt-get install -y git
    apt-get install -y virtualenv
    apt-get install -y python3
    apt-get install -y python3-dev
    apt-get install -y python-gdal
    apt-get install -y gdal-bin
    apt-get install -y libgdal-dev
    apt-get install -y unzip
    apt-get install -y osmosis
    apt-get install curl ca-certificates gnupg
    curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
    sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
    apt-get update
    apt-get install -y postgresql-12-postgis-3
    #{pgsql_mods}
    service postgresql restart
  SHELL

  # TODO move node-related setup to an nvm-based post-provisioning step, b/c for
  # some reason it just doesn't work anymore

end
