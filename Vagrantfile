# -*- mode: ruby -*-
# vi: set ft=ruby :
Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu/xenial64"

  config.vm.provider "virtualbox" do |v|
    v.memory = 2048
    v.cpus = 2
  end

  # Forward postgres to 5433 on the host so you can access it with QGIS and the
  # like at ubuntu@localhost:5433
  config.vm.network "forwarded_port", guest: 5432, host: 5433

  # Modifications to postgres security to allow port forwarding to the host.
  # Obviously this is insecure and only meant to be used in a local setting
  pg_hba_access_for_host = "host all all 10.0.2.2/32 trust # Support connections from the vagrant host"
  postgresql_conf_access_for_host = "listen_addresses='*'"

  # Provision the vm with the relevant dependencies, modify postgres settings
  # and create an ubuntu superuser
  config.vm.provision "shell", inline: <<-SHELL
    add-apt-repository ppa:ubuntugis/ppa
    apt-get update
    apt-get install -y git virtualenv nodejs python3 postgis gdal-bin libgdal1-dev unzip
    sudo -u postgres psql -c "DROP ROLE IF EXISTS ubuntu; CREATE ROLE ubuntu LOGIN SUPERUSER"
    grep -q -F '#{pg_hba_access_for_host}' /etc/postgresql/9.5/main/pg_hba.conf  || echo '#{pg_hba_access_for_host}' >> /etc/postgresql/9.5/main/pg_hba.conf
    grep -q -F "#{postgresql_conf_access_for_host}" /etc/postgresql/9.5/main/postgresql.conf  || echo "#{postgresql_conf_access_for_host}" >> /etc/postgresql/9.5/main/postgresql.conf
    service postgresql restart
  SHELL
end
