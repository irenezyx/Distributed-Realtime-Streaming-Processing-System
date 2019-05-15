yum install wget -y
wget http://downloads.typesafe.com/scala/2.11.7/scala-2.11.7.tgz
tar xvf scala-2.11.7.tgz
sudo mv scala-2.11.7 /usr/lib
sudo ln -s /usr/lib/scala-2.11.7 /usr/lib/scala
export PATH=$PATH:/usr/lib/scala/bin
scala -version
