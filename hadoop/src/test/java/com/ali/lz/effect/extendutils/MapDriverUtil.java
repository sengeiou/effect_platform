package com.ali.lz.effect.extendutils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

public class MapDriverUtil<K1, V1, K2, V2> extends MapDriverExt<Object, Text, K2, V2> {

    public void setMultiInputsfromFile(String input_file_name) {
        // input_file_name = ClassLoader.getSystemResource(input_file_name)
        // .getFile();
        BufferedReader r_in;
        try {
            r_in = new BufferedReader(new InputStreamReader(new FileInputStream(input_file_name)));

            String line;
            while ((line = r_in.readLine()) != null) {
                this.addInput(new Text(""), new Text(line));
            }
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
