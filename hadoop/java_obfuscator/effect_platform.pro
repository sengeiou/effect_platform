-injars ../../../../target/EffectPlatform.jar 
-outjars ../../../../target/EffectPlatform_release.jar 
-libraryjars ../../../../target/lib:/usr/java/default//lib:/usr/java/default//jre/lib:/home/hadoop/hadoop-current/hadoop-0.19.1-dc-core.jar:/home/hadoop/hadoop-current/lib/commons-logging-api-1.0.4.jar:/home/hadoop/hadoop-current/lib/commons-logging-1.0.4.jar:/home/hive/jar/TaomiDecode.jar:/home/hive/jar/taobao_udf-0.1.jar:/home/hive/jar/toolkit-common-lang-1.0.jar:/home/hive/hive/lib

-keepparameternames
-renamesourcefileattribute SourceFile
-keepattributes Exceptions,InnerClasses,Signature,Deprecated,SourceFile,LineNumberTable,*Annotation*,EnclosingMethod
-keep public interface com.taobao.lz.dw.util.Processor
-keep public class * {
    public protected *;
}
-keepclassmembernames class * {
    java.lang.Class class$(java.lang.String);
    java.lang.Class class$(java.lang.String, boolean);
}

-keepclasseswithmembernames class * {
    native <methods>;
}
-keepclassmembers enum * {
    public static **[] values();
    public static ** valueOf(java.lang.String);
}

-keepclassmembers class * implements java.io.Serializable {
    static final long serialVersionUID;
    private static final java.io.ObjectStreamField[] serialPersistentFields;
    private void writeObject(java.io.ObjectOutputStream);
    private void readObject(java.io.ObjectInputStream);
    java.lang.Object writeReplace();
    java.lang.Object readResolve();
}
