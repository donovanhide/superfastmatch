curl http://downloads.mongodb.org/osx/mongodb-osx-x86_64-2.2.1.tgz > mongo.tgz
mkdir mongo
tar -zxvf mongo.tgz --strip-components=1 -C mongo
sudo mkdir -p data/db
