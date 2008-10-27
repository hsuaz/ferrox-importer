mysql -u ferrox <<END
DROP DATABASE furaffinity;
CREATE DATABASE furaffinity;
END

paster setup-app ~/dev/ferrox/development.ini
