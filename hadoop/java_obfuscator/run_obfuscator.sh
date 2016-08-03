#!/bin/sh

jar_dir=$HOME/linezing/effect_platform/hadoop/target

#rm $jar_dir/EffectPlatform_release.jar

java -jar proguard.jar @effect_platform.pro
