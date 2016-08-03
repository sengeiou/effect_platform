#!/bin/bash
##for check
LANG=en_US.utf-8
#/home/ads/tools/apache-ant-1.7.1/bin/ant -f ../hadoop/build.xml
mvn -f ../hadoop/pom.xml clean
mvn -f ../hadoop/pom.xml install -Dmaven.test.skip=true
export temppath=$1
cd $temppath/rpm
sed -i  "s/^Release:.*$/Release: "$4"/" $2.spec
sed -i  "s/^Version:.*$/Version: "$3"/" $2.spec
export TAGS=TAG:`svn info|grep "URL"|cut -d ":" -f 2-|sed "s/^ //g"|awk -F "trunk|tags|branche" '{print $1}'`tags/$2_A_`echo $3|tr "." "_"`_$4
sed -i  "s#%description#%description \n $TAGS#g"  $2.spec
/usr/local/bin/rpm_create -p /home/taobao -v $3 -r $4 $2.spec -k
svn revert $2.spec
mv `find . -name $2-$3-$4*rpm`  .
